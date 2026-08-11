"""Sequence step processing worker."""

import asyncio
import random
import re
import uuid
from datetime import datetime, timedelta

import structlog
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from src.config import get_settings
from src.models.base import async_session
from src.models.models import (
    SequenceEnrollmentStep,
    SequenceEnrollment,
    SentEmail,
    EnrollmentStepStatus,
    EnrollmentStatus,
)
from src.services.email_builder import build_tracked_email
from src.api.tracking import generate_unsubscribe_url
from src.services.gmail import GmailService, GmailError
from src.services.email_api import EmailAPITransport, EmailAPIError
from src.services.mailbox_rotation import (
    reserve_send,
    release_send,
    next_capacity_reset,
    seconds_until_capacity_reset,
)
from src.services.queue import queue_sequence_step
from src.services.template import render_email
from src.services.circuit_breaker import check_circuit_breaker
from src.services.send_window import check_send_window
from src.services.suppression import check_suppressed

settings = get_settings()
logger = structlog.get_logger()


def _gmail_call_locked(gmail, method, *args, **kwargs):
    """Run a blocking GmailService method under the per-mailbox lock.

    Runs in a worker thread via asyncio.to_thread. The lock serializes
    same-mailbox calls so two concurrent arq jobs cannot interleave httplib2
    HTTP state on the shared cached GmailService singleton. Different
    mailboxes have different instances -> different locks -> stay concurrent.
    """
    with gmail._lock:
        return method(*args, **kwargs)


def _blank_content(subject, body) -> bool:
    """True if an email would render blank (audit C2 / REVOPS-886).

    The Old/New-ICP step templates store '{{subject}}'/'{{body}}' placeholders that
    render to '' when Scout content is absent; we must never send such a blank
    email. Treats None / empty / whitespace-only subject OR body as blank.
    """
    return not (subject or "").strip() or not (body or "").strip()


def _is_html_body(body: str) -> bool:
    """True if ``body`` contains structural HTML tags we actually emit.

    REVOPS-1501: step bodies are overwhelmingly plain text (25,877 of 26,000
    ``sent_emails`` rows). Previously every body was passed to
    ``build_tracked_email`` with ``is_html=True``, so plain text went verbatim
    into the HTML MIME part and every mail client collapsed the newlines. A
    body counts as HTML only if it has an opening ``<p>``, ``<br>``, ``<div>``,
    ``<a>``, or ``<html>`` tag (followed by ``>``, space, or ``/``); a bare
    ``<`` in prose (e.g. ``"latency <10ms guaranteed"``) does NOT match because
    the char after ``<`` is a digit, not one of these tag names.
    """
    return bool(re.search(r"<(p|br|div|a|html)[ >/]", body, re.IGNORECASE))


async def _defer_step(
    db,
    enrollment_step: SequenceEnrollmentStep,
    fire_at: datetime,
    delay_seconds: int,
    tenant_id: str,
) -> None:
    """Stamp a step's `scheduled_at` to an ABSOLUTE naive-UTC fire time and
    re-enqueue it for that fire time, so the arq dedup key
    (`step:{id}:{int(UTC-aware scheduled_at.timestamp())}`) is STABLE across
    repeated defers for the same intended fire time — the root cause of the
    2026-07-20 83k-job arq flood (relative `utcnow() + delay` let the result
    land anywhere in the final second before the reset depending on
    `utcnow()`'s microsecond fraction, so the dedup key varied and arq did
    not collapse; ~27 dupes/step).

    `fire_at` MUST be naive UTC (tzinfo is None). The
    `sequence_enrollment_steps.scheduled_at` column is
    `timestamp WITHOUT time zone` holding naive UTC; assigning an aware
    datetime would error or silently shift the value (this repo has been
    bitten three times today by naive/aware UTC confusion, so be explicit).

    The two callers compute `fire_at` DIFFERENTLY by design:

    - **Capacity branch**: `next_capacity_reset().replace(tzinfo=None)`.
      `next_capacity_reset()` returns the next 00:05 UTC instant with
      second=0, microsecond=0 — identical for every call in the day, so every
      defer of a given step produces EXACTLY ONE dedup key (collapses ~27
      dupes/step to 1). It is tz-aware (`datetime.now(timezone.utc)`), so the
      `.replace(tzinfo=None)` is required before storing. An absolute target
      is available because the reset instant is independent of the caller.

    - **Send-window branch**: `check_send_window(tz)` returns a RELATIVE
      delay and the window opening is recipient-timezone dependent, so no
      absolute target exists at call time. `fire_at` is
      `(datetime.utcnow() + timedelta(seconds=window_delay))` floored to the
      minute (`.replace(second=0, microsecond=0)`) so repeated defers within
      the same minute share a key. This branch is secondary — it accounts for
      ~111 jobs vs 78,436 from the capacity path.

    `delay_seconds` is the arq `_defer_by` (relative delta) — kept as-is for
    both branches so arq fires the job at the intended instant even when
    `fire_at` was floored.
    """
    enrollment_step.scheduled_at = fire_at
    await db.commit()
    await queue_sequence_step(
        enrollment_step_id=enrollment_step.id,
        tenant_id=tenant_id,
        scheduled_at=fire_at,
        delay_seconds=delay_seconds,
    )


def compute_next_scheduled_at(
    created_at,
    delay_days,
    delay_hours,
    now,
    jitter_seconds=0,
    min_gap_seconds=0,
):
    """Absolute-offset scheduling: enrollment.created_at + delay, with a
    past-due guard and catch-up min-spacing.

    Scout authors ``delay_days`` as ABSOLUTE day-offsets from enrollment
    (Old-ICP 0,4,9,15,22), not incremental waits from the previous send.
    Anchoring on ``created_at`` keeps the cadence constant regardless of when
    prior steps ran. When the absolute target is already in the past (a prior
    step ran late), the step is scheduled ``min_gap_seconds`` from ``now``
    instead of firing immediately, so a behind-schedule enrollment cannot burn
    through remaining touches back-to-back (REVOPS-1376). Jitter is applied
    after the guard and a final ``max(result, now)`` clamp guarantees negative
    jitter never lands before ``now``.

    REVOPS-1375 / REVOPS-1376.
    """
    target = created_at + timedelta(days=delay_days, hours=delay_hours)
    if target < now:
        scheduled = now + timedelta(seconds=min_gap_seconds)
    else:
        scheduled = target
    result = scheduled + timedelta(seconds=jitter_seconds)
    return max(result, now)


async def process_sequence_step(
    ctx: dict,
    enrollment_step_id: str,
    tenant_id: str,
) -> dict:
    """
    Process a single sequence step.

    1. Load enrollment step with all related data
    2. Select or use assigned mailbox
    3. Render email content
    4. Send email (or stub)
    5. Update status
    """
    logger.info("Processing sequence step", enrollment_step_id=enrollment_step_id)

    async with async_session() as db:
        # Load enrollment step
        result = await db.execute(
            select(SequenceEnrollmentStep)
            .where(SequenceEnrollmentStep.id == enrollment_step_id)
            .options(
                selectinload(SequenceEnrollmentStep.enrollment).selectinload(
                    SequenceEnrollment.sequence
                ),
                selectinload(SequenceEnrollmentStep.step),
                selectinload(SequenceEnrollmentStep.mailbox),
            )
        )
        enrollment_step = result.scalar_one_or_none()

        if not enrollment_step:
            logger.error(
                "Enrollment step not found", enrollment_step_id=enrollment_step_id
            )
            raise ValueError(f"Enrollment step not found: {enrollment_step_id}")

        enrollment = enrollment_step.enrollment
        step = enrollment_step.step

        # Check enrollment is still active
        if enrollment.status != EnrollmentStatus.ACTIVE:
            logger.info(
                "Skipping - enrollment not active",
                enrollment_id=enrollment.id,
                status=enrollment.status,
            )
            return {"skipped": True, "reason": "enrollment_not_active"}

        # Suppression check — never send to suppressed contacts
        is_suppressed = await check_suppressed(db, enrollment.contact_email, tenant_id)
        if is_suppressed:
            logger.info(
                "Skipping - contact is suppressed",
                enrollment_id=enrollment.id,
                contact_email=enrollment.contact_email,
            )
            enrollment.status = EnrollmentStatus.UNSUBSCRIBED
            enrollment_step.status = EnrollmentStepStatus.SKIPPED
            await db.commit()
            return {"skipped": True, "reason": "suppressed"}

        # Circuit breaker check — skip if mailbox bounce rate is too high
        tripped = await check_circuit_breaker(db, enrollment.mailbox_id, tenant_id)
        if tripped:
            logger.warning(
                "Circuit breaker tripped — skipping send",
                enrollment_id=enrollment.id,
                mailbox_id=enrollment.mailbox_id,
            )
            return {"skipped": True, "reason": "circuit_breaker"}

        # Send window check — re-queue if outside recipient's business hours
        window_delay = check_send_window(enrollment.timezone)
        if window_delay is not None:
            logger.debug(
                "Outside send window — re-queuing",
                enrollment_step_id=enrollment_step_id,
                delay_seconds=window_delay,
            )
            # No absolute target is available — `check_send_window(tz)` returns
            # a RELATIVE delay and the window opening is recipient-timezone
            # dependent. Floor to the minute so repeated defers within the same
            # minute share a dedup key (secondary branch — accounts for ~111
            # jobs vs 78,436 from the capacity path; see `_defer_step`).
            fire_at = (datetime.utcnow() + timedelta(seconds=window_delay)).replace(
                second=0, microsecond=0
            )
            await _defer_step(
                db=db,
                enrollment_step=enrollment_step,
                fire_at=fire_at,
                delay_seconds=window_delay,
                tenant_id=tenant_id,
            )
            return {
                "skipped": True,
                "reason": "outside_send_window",
                "requeued_delay": window_delay,
            }

        # Check step is ready to process (PENDING or SCHEDULED)
        if enrollment_step.status not in (
            EnrollmentStepStatus.PENDING,
            EnrollmentStepStatus.SCHEDULED,
        ):
            logger.info(
                "Skipping - step not ready",
                enrollment_step_id=enrollment_step_id,
                status=enrollment_step.status,
            )
            return {"skipped": True, "reason": "step_not_ready"}

        # F3 idempotency (at-most-once): a SentEmail row is committed BEFORE the
        # Gmail call (below), so its presence means a prior attempt already
        # reached the send. On an arq retry (e.g. the SENT status was rolled back
        # by a crash after Gmail delivered) we must NOT send again — a duplicate
        # to a prospect is worse than a rare missed follow-up. A *known* GmailError
        # removes its marker (see below), so only a hard crash mid-send leaves one.
        existing_send = await db.execute(
            select(SentEmail.id)
            .where(SentEmail.enrollment_step_id == enrollment_step.id)
            .limit(1)
        )
        if existing_send.scalar_one_or_none() is not None:
            logger.warning(
                "Idempotency: send already attempted for step — skipping re-send",
                enrollment_step_id=enrollment_step_id,
            )
            if enrollment_step.status != EnrollmentStepStatus.SENT:
                enrollment_step.status = EnrollmentStepStatus.SENT
                await db.commit()
            return {"skipped": True, "reason": "already_sent"}

        # Use enrollment's sticky mailbox (assigned at enrollment time)
        from src.models.models import Mailbox
        from src.config import validate_mailbox_for_tenant

        result = await db.execute(
            select(Mailbox).where(Mailbox.id == enrollment.mailbox_id)
        )
        mailbox = result.scalar_one_or_none()

        if not mailbox:
            logger.error(
                "Enrollment mailbox not found", mailbox_id=enrollment.mailbox_id
            )
            raise RuntimeError(f"Enrollment mailbox not found: {enrollment.mailbox_id}")

        # HARDCODED ENFORCEMENT: Verify mailbox is allowed for this tenant
        try:
            validate_mailbox_for_tenant(tenant_id, mailbox.email)
        except ValueError as e:
            logger.error("Mailbox not allowed for tenant", error=str(e))
            raise RuntimeError(str(e))

        # Reserve send slot. reserve_send is the SOLE authoritative enforcer of the
        # hard 75/day cap (atomic conditional UPDATE) and returns False when the
        # sticky mailbox is at capacity. H3 (REVOPS-972): at-capacity must DEFER,
        # not crash. Previously this raised RuntimeError, so arq retried 3x/30s and
        # abandoned a hot follow-up pinned to a full mailbox. Instead we re-queue
        # the SAME step to just after the next 00:05 UTC capacity reset (mirroring
        # the send-window re-queue above) and return a deferred result. No
        # SentEmail row is written and no slot is consumed, so the cap is untouched.
        reserved = await reserve_send(db, mailbox.id)
        if not reserved:
            defer_delay = seconds_until_capacity_reset()
            logger.debug(
                "Mailbox at capacity — deferring to next daily reset",
                enrollment_step_id=enrollment_step_id,
                mailbox_id=mailbox.id,
                delay_seconds=defer_delay,
            )
            # ABSOLUTE target available: `next_capacity_reset()` returns the
            # next 00:05 UTC instant with second=0, microsecond=0 — identical
            # for every call in the day, so every defer of a given step
            # produces EXACTLY ONE dedup key (collapses ~27 dupes/step to 1).
            # It is tz-aware (`datetime.now(timezone.utc)`); the
            # `scheduled_at` column is naive UTC so `.replace(tzinfo=None)`
            # is required before storing (see `_defer_step`).
            fire_at = next_capacity_reset().replace(tzinfo=None)
            await _defer_step(
                db=db,
                enrollment_step=enrollment_step,
                fire_at=fire_at,
                delay_seconds=defer_delay,
                tenant_id=tenant_id,
            )
            return {"deferred": True, "reason": "mailbox_at_capacity"}

        # Use Scout-composed content if available, otherwise render step template
        if enrollment_step.custom_subject and enrollment_step.custom_body:
            # Scout composed this email - use it directly
            subject = enrollment_step.custom_subject
            body = enrollment_step.custom_body
            logger.info(
                "Using Scout-composed content", enrollment_step_id=enrollment_step_id
            )
        else:
            # Fall back to step template
            subject, body = render_email(
                step.subject,
                step.body,
                contact_name=enrollment.contact_name,
                contact_email=enrollment.contact_email,
            )
            logger.info("Using step template", enrollment_step_id=enrollment_step_id)

        # REVOPS-886 (audit C2): send-side safety net. Never emit a blank email.
        # The Old/New-ICP step templates store '{{subject}}'/'{{body}}' placeholders
        # that render to empty when Scout content is absent; combined with an
        # upstream miss this delivered blank emails to real prospects. Skip (do NOT
        # send) if the resolved subject OR body is empty after rendering, and log
        # loudly so it's alertable.
        if _blank_content(subject, body):
            logger.error(
                "Blocking BLANK email send (empty subject or body after render)",
                enrollment_step_id=enrollment_step_id,
                enrollment_id=enrollment.id,
                to_email=enrollment.contact_email,
                has_custom=bool(
                    enrollment_step.custom_subject and enrollment_step.custom_body
                ),
            )
            enrollment_step.status = EnrollmentStepStatus.SKIPPED
            await db.commit()
            return {"skipped": True, "reason": "empty_content_blocked"}

        # Create sent email record first (need ID for tracking)
        sent_email_id = str(uuid.uuid4())
        sent_email = SentEmail(
            id=sent_email_id,
            message_id=f"pending-{sent_email_id}",  # Placeholder until sent
            thread_id=None,
            mailbox_id=mailbox.id,
            enrollment_step_id=enrollment_step.id,
            subject=subject,
            body=body,
            to_email=enrollment.contact_email,
            to_name=enrollment.contact_name,
            from_email=mailbox.email,
            from_name=mailbox.display_name,
            sent_at=datetime.utcnow(),
        )
        db.add(sent_email)
        # F3: COMMIT the marker BEFORE the Gmail send (was a non-durable flush).
        # If the worker crashes after Gmail delivers but before the final commit,
        # this row survives → the idempotency pre-check above skips the retry
        # (at-most-once). A known GmailError below deletes it so the step stays
        # retryable.
        await db.commit()

        # Build tracked HTML email (with unsubscribe link + CAN-SPAM footer).
        # REVOPS-1501: step bodies are plain text (25,877 of 26,000 rows) —
        # detect HTML structurally instead of assuming it. Plain-text bodies go
        # through plain_text_to_html (escapes entities, newlines → <br>); genuine
        # HTML bodies pass through verbatim.
        is_html = _is_html_body(body)
        html_body, plain_body = build_tracked_email(
            body=body,
            sent_email_id=sent_email_id,
            is_html=is_html,
            enrollment_id=enrollment.id,
        )

        # Build RFC 8058 List-Unsubscribe header. Advertise the one-click HTTPS
        # endpoint ONLY when it's reachable (one_click_unsubscribe_enabled);
        # otherwise mailto-only, so we never advertise a dead one-click URL
        # (track.telnyx.com is NXDOMAIN — Wave 0 interim).
        mailto_unsub = "<mailto:unsubscribe@telnyx.com?subject=unsubscribe>"
        if settings.one_click_unsubscribe_enabled:
            unsub_url = generate_unsubscribe_url(
                settings.tracking_base_url, enrollment.id
            )
            list_unsubscribe = f"<{unsub_url}>, {mailto_unsub}"
        else:
            list_unsubscribe = mailto_unsub

        # REVOPS-1552: per-mailbox transport selection. The Mailbox.transport
        # column ('gmail' default | 'email_api') drives the send path. With
        # 'gmail', behavior is byte-identical to today (the full existing suite
        # is the contract — no code path changes). With 'email_api', the
        # Telnyx Email API path is taken. No mailbox can change transport
        # without an explicit per-mailbox DB flag change (the column is NOT
        # NULL with server_default 'gmail' + a CHECK constraint).
        if mailbox.transport == "email_api":
            try:
                transport = EmailAPITransport.get_instance()
                result = await transport.send_html_email(
                    from_email=mailbox.email,
                    to=enrollment.contact_email,
                    subject=subject,
                    html_body=html_body,
                    plain_text_fallback=plain_body,
                    sender_name=mailbox.display_name,
                    list_unsubscribe=list_unsubscribe,
                    one_click=settings.one_click_unsubscribe_enabled,
                    # Email-to-Salesforce: SFDC logs a completed Task on the
                    # matching contact/lead from this BCC copy.
                    bcc=settings.salesforce_bcc_address or None,
                )
                api_message_id = result["message_id"]

                # Update sent email with the Telnyx message UUID.
                sent_email.message_id = api_message_id
                sent_email.thread_id = result.get("thread_id")

                logger.info(
                    "Email sent via Telnyx Email API (HTML with tracking)",
                    from_email=mailbox.email,
                    to_email=enrollment.contact_email,
                    message_id=api_message_id,
                )
            except EmailAPIError as e:
                logger.error("Email API send failed", error=str(e))
                # F3 (at-most-once): a known EmailAPIError means it did NOT
                # deliver — remove the pre-send marker so the step stays
                # retryable (otherwise a transient API error would permanently
                # skip the prospect under the at-most-once pre-check). Hard
                # crashes (no except) keep the marker → at-most-once.
                try:
                    await db.delete(sent_email)
                    await db.commit()
                except Exception as del_err:
                    logger.warning("Failed to remove send marker", error=str(del_err))
                # F5: give the reserved capacity slot back so a failed/bounced
                # attempt doesn't permanently throttle the mailbox.
                try:
                    await release_send(db, mailbox.id)
                except Exception as rel_err:  # never mask the original failure
                    logger.warning("Failed to release send slot", error=str(rel_err))
                raise RuntimeError(f"Email API send failed: {e}")
        elif settings.gmail_enabled:
            # Gmail path (byte-identical to pre-1552 behavior). Offload the
            # blocking send to a worker thread (Fix 1) so the event loop stays
            # responsive for the other 9 arq jobs. Acquire the per-mailbox lock
            # (Fix 2) so two concurrent jobs on the SAME mailbox cannot
            # interleave httplib2 state on the shared cached GmailService
            # singleton.
            try:
                gmail = GmailService.get_inbox(mailbox.email)
                result = await asyncio.to_thread(
                    _gmail_call_locked,
                    gmail,
                    gmail.send_html_email,
                    to=enrollment.contact_email,
                    subject=subject,
                    html_body=html_body,
                    plain_text_fallback=plain_body,
                    sender_name=mailbox.display_name,
                    list_unsubscribe=list_unsubscribe,
                    one_click=settings.one_click_unsubscribe_enabled,
                    # Email-to-Salesforce: SFDC logs a completed Task on the
                    # matching contact/lead from this BCC copy.
                    bcc=settings.salesforce_bcc_address or None,
                )
                gmail_message_id = result["message_id"]
                gmail_thread_id = result["thread_id"]

                # Update sent email with actual IDs
                sent_email.message_id = gmail_message_id
                sent_email.thread_id = gmail_thread_id

                logger.info(
                    "Email sent via Gmail (HTML with tracking)",
                    from_email=mailbox.email,
                    to_email=enrollment.contact_email,
                    message_id=gmail_message_id,
                )
            except GmailError as e:
                logger.error("Gmail send failed", error=str(e))
                # F3: a known GmailError means it did NOT deliver — remove the
                # pre-send marker so the step stays retryable (otherwise a
                # transient SMTP error would permanently skip the prospect under
                # the at-most-once pre-check). Hard crashes (no except) keep the
                # marker → at-most-once.
                try:
                    await db.delete(sent_email)
                    await db.commit()
                except Exception as del_err:
                    logger.warning("Failed to remove send marker", error=str(del_err))
                # F5: the send failed — give the reserved capacity slot back so a
                # failed/bounced attempt doesn't permanently throttle the mailbox.
                try:
                    await release_send(db, mailbox.id)
                except Exception as rel_err:  # never mask the original failure
                    logger.warning("Failed to release send slot", error=str(rel_err))
                raise RuntimeError(f"Gmail send failed: {e}")
        else:
            # Stub mode - generate fake message ID
            sent_email.message_id = f"stub-{uuid.uuid4()}"
            logger.info(
                "[STUB] Gmail disabled - skipping actual send",
                from_email=mailbox.email,
                to_email=enrollment.contact_email,
                subject=subject,
            )

        # Update step status
        enrollment_step.status = EnrollmentStepStatus.SENT
        enrollment_step.sent_at = datetime.utcnow()

        # Update enrollment current_step
        enrollment.current_step = step.step_number

        await db.commit()

        logger.info(
            "Sequence step processed successfully",
            enrollment_step_id=enrollment_step_id,
            message_id=sent_email.message_id,
        )

        # Queue next step if exists
        next_step_info = await _queue_next_step(
            db=db,
            enrollment=enrollment,
            current_step_number=step.step_number,
            tenant_id=tenant_id,
        )

        return {
            "success": True,
            "message_id": sent_email.message_id,
            "to_email": enrollment.contact_email,
            "next_step_queued": next_step_info,
        }


async def _queue_next_step(
    db,
    enrollment: SequenceEnrollment,
    current_step_number: int,
    tenant_id: str,
) -> dict | None:
    """
    Find and queue the next step in the sequence.

    Returns info about queued step, or None if no next step.
    """
    from src.models.models import SequenceStep

    # Find the next step in sequence
    result = await db.execute(
        select(SequenceStep)
        .where(
            SequenceStep.sequence_id == enrollment.sequence_id,
            SequenceStep.step_number > current_step_number,
        )
        .order_by(SequenceStep.step_number)
        .limit(1)
    )
    next_step = result.scalar_one_or_none()

    if not next_step:
        logger.info(
            "No more steps in sequence",
            enrollment_id=enrollment.id,
            current_step=current_step_number,
        )
        # Mark enrollment as completed
        enrollment.status = EnrollmentStatus.COMPLETED
        await db.commit()
        return None

    # Find the enrollment step for the next sequence step
    result = await db.execute(
        select(SequenceEnrollmentStep).where(
            SequenceEnrollmentStep.enrollment_id == enrollment.id,
            SequenceEnrollmentStep.step_id == next_step.id,
        )
    )
    next_enrollment_step = result.scalar_one_or_none()

    if not next_enrollment_step:
        logger.error(
            "Enrollment step not found for next sequence step",
            enrollment_id=enrollment.id,
            step_id=next_step.id,
        )
        return None

    # REVOPS-1375: schedule ABSOLUTE-from-enrollment (created_at + delay), not
    # incremental-from-previous-send (utcnow + delay). Scout authors delay_days
    # as absolute day-offsets from enrollment (Old-ICP 0,4,9,15,22); the old
    # incremental anchor stretched a 22-day sequence to ~50 days because each
    # step re-anchored on the prior send time. The past-due guard inside
    # compute_next_scheduled_at (max(target, now)) prevents scheduling in the
    # past when a prior step ran late.
    # REVOPS-1376: a past-due step is scheduled min_gap from now (not now) so
    # a behind-schedule enrollment cannot fire back-to-back catch-up touches.
    # One captured `now` is reused for both the helper anchor and the arq delay
    # so a wall-clock tick between them cannot shift the delay (review #3).
    now = datetime.utcnow()
    jitter_seconds = 0
    if settings.send_jitter_enabled and settings.send_jitter_minutes > 0:
        jitter_seconds = random.randint(
            -settings.send_jitter_minutes * 60,
            settings.send_jitter_minutes * 60,
        )
        logger.info("Applied send jitter", jitter_seconds=jitter_seconds)

    # Apply the catch-up min-gap only to GENUINELY past-due steps (>1m past).
    # A step whose absolute target is within 1 minute of now (e.g. delay_days=0
    # on a just-created enrollment) is "due now", not catch-up — the min-gap
    # guard is for enrollments that fell genuinely behind (REVOPS-1376).
    target = enrollment.created_at + timedelta(
        days=next_step.delay_days, hours=next_step.delay_hours
    )
    min_gap_seconds = (
        settings.min_step_gap_hours * 3600 if target < now - timedelta(minutes=1) else 0
    )

    # Mark as scheduled. Record scheduled_at so a lost arq job can be detected
    # and reconciled (audit M4) — previously this column was never written.
    next_enrollment_step.status = EnrollmentStepStatus.SCHEDULED
    next_enrollment_step.scheduled_at = compute_next_scheduled_at(
        enrollment.created_at,
        next_step.delay_days,
        next_step.delay_hours,
        now,
        jitter_seconds,
        min_gap_seconds,
    )
    await db.commit()

    # Queue the next step. The arq delay is computed from the NEW scheduled_at
    # (absolute time), not the raw step delay, so the job fires at the right
    # moment even when a prior step ran late (past-due guard pushed
    # scheduled_at forward to now + min_gap).
    delay_seconds = int(
        max(0, (next_enrollment_step.scheduled_at - now).total_seconds())
    )
    try:
        job_id = await queue_sequence_step(
            enrollment_step_id=next_enrollment_step.id,
            tenant_id=tenant_id,
            scheduled_at=next_enrollment_step.scheduled_at,
            delay_seconds=delay_seconds if delay_seconds > 0 else None,
        )

        logger.info(
            "Queued next sequence step",
            enrollment_id=enrollment.id,
            enrollment_step_id=next_enrollment_step.id,
            step_number=next_step.step_number,
            delay_seconds=delay_seconds,
            job_id=job_id,
        )

        return {
            "enrollment_step_id": next_enrollment_step.id,
            "step_number": next_step.step_number,
            "delay_seconds": delay_seconds,
            "job_id": job_id,
        }
    except Exception as e:
        logger.error("Failed to queue next step", error=str(e))
        return None
