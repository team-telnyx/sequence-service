"""Sequence step processing worker."""

import asyncio
import random
import re
import uuid
from datetime import datetime, timedelta

import structlog
from arq.worker import Retry as ArqRetry
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


class _AmbiguousRetryableError(Exception):
    """A sentinel retryable error for the ambiguous-marker skip path. The
    marker exists from a prior ambiguous send (pending- message_id); we do
    NOT re-send (at-most-once). We raise ArqRetry with this sentinel so
    _compute_retry_defer computes a backoff and arq counts the attempt
    toward max_tries. The error message is informational only (it never
    reaches the user — it's caught by arq's retry machinery).
    """

    @property
    def retry_after(self) -> None:
        return None


def _compute_retry_defer(exc, ctx: dict) -> float:
    """Compute the arq Retry defer (seconds) for a retryable EmailAPIError.

    Honors the ``Retry-After`` header (parsed by the adapter into
    ``exc.retry_after``) for 429s. Otherwise exponential backoff keyed to the
    arq ``job_try`` counter (1-based): 30s, 60s, 120s, … capped at 600s. The
    base matches the WorkerSettings ``retry_defer_time = 30`` convention.

    r3 (REVOPS-1552): the defer is CAPPED at a safe fraction of the
    reconciler grace (``reconcile_grace_seconds * retry_defer_grace_fraction``)
    so a long Retry-After can never defer past the point where the reconciler
    would re-enqueue the step as "lost". If Retry-After exceeds the cap, defer
    at the cap and let the next attempt re-read Retry-After. The cap is
    derived from the same config the reconciler reads — no magic number. The
    scheduled_at advance in the error handler is the structural half.
    """
    retry_after = getattr(exc, "retry_after", None)
    if retry_after is not None:
        defer = float(retry_after)
    else:
        job_try = int(ctx.get("job_try", 1))
        defer = float(min(30 * 2 ** (job_try - 1), 600))
    grace_seconds = getattr(settings, "reconcile_grace_seconds", 900)
    grace_fraction = getattr(settings, "retry_defer_grace_fraction", 0.5)
    return min(defer, grace_seconds * grace_fraction)


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
            logger.error("Enrollment step not found", enrollment_step_id=enrollment_step_id)
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
            select(SentEmail).where(SentEmail.enrollment_step_id == enrollment_step.id).limit(1)
        )
        sent_email_existing = existing_send.scalar_one_or_none()
        if sent_email_existing is not None:
            msg_id = sent_email_existing.message_id or ""
            if not msg_id.startswith("pending-"):
                logger.info(
                    "Idempotency: send already completed for step — skipping re-send",
                    enrollment_step_id=enrollment_step_id,
                    message_id=msg_id,
                )
                if enrollment_step.status != EnrollmentStepStatus.SENT:
                    enrollment_step.status = EnrollmentStepStatus.SENT
                    await db.commit()
                return {"skipped": True, "reason": "already_sent"}
            job_try = int(ctx.get("job_try", 1))
            max_tries = settings.worker_max_tries
            if job_try >= max_tries:
                enrollment_step.status = EnrollmentStepStatus.FAILED
                try:
                    await release_send(db, sent_email_existing.mailbox_id)
                except Exception as rel_err:
                    logger.warning(
                        "Failed to release send slot on ambiguous-exhaustion",
                        error=str(rel_err),
                    )
                await db.commit()
                logger.error(
                    "Ambiguous send exhausted max_tries — terminal failure (row -> FAILED)",
                    job_try=job_try,
                    max_tries=max_tries,
                    enrollment_step_id=enrollment_step_id,
                    mailbox_id=sent_email_existing.mailbox_id,
                )
                return {
                    "failed": True,
                    "reason": "max_retries_exhausted",
                    "error": "ambiguous send (pending marker kept); exhausted retries without re-send",
                    "job_try": job_try,
                    "max_tries": max_tries,
                }
            logger.info(
                "Idempotency: ambiguous send marker present — skipping re-send, counting attempt",
                enrollment_step_id=enrollment_step_id,
                job_try=job_try,
                max_tries=max_tries,
            )
            raise ArqRetry(
                defer=_compute_retry_defer(
                    _AmbiguousRetryableError("ambiguous send marker present"), ctx
                )
            )

        # Use enrollment's sticky mailbox (assigned at enrollment time)
        from src.models.models import Mailbox
        from src.config import validate_mailbox_for_tenant

        result = await db.execute(select(Mailbox).where(Mailbox.id == enrollment.mailbox_id))
        mailbox = result.scalar_one_or_none()

        if not mailbox:
            logger.error("Enrollment mailbox not found", mailbox_id=enrollment.mailbox_id)
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
            logger.info("Using Scout-composed content", enrollment_step_id=enrollment_step_id)
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
                has_custom=bool(enrollment_step.custom_subject and enrollment_step.custom_body),
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

        # Build RFC 8058 List-Unsubscribe header. mailto: only — no first-party
        # HTTPS endpoint (the /track/unsubscribe base64 endpoint was deleted in
        # SV2-044). One-click unsubscribe is handled by the Email API webhook
        # (email.unsubscribed events) when the Email API is the transport.
        list_unsubscribe = "<mailto:unsubscribe@telnyx.com?subject=unsubscribe>"

        # REVOPS-1552: per-mailbox transport selection. The Mailbox.transport
        # column ('gmail' default | 'email_api') drives the send path. P1-B:
        # explicit dispatch — anything not exactly 'gmail'/'email_api' raises a
        # terminal configuration error (no silent fallthrough to Gmail). The
        # DB CHECK constrains SQL writes, but ORM writes, SQLite tests, and
        # future values can bypass it, so the dispatch fails safe.
        if mailbox.transport == "email_api":
            # SV2-044 r3 (FAIL 1c): STABLE Idempotency-Key derived from the
            # logical send identity. The r1 adapter generated a FRESH uuid4
            # per attempt → a timeout→retry sent a DIFFERENT key → duplicate
            # delivery. The key is deterministic from enrollment_step_id (the
            # durable send marker's identity) so a retry reuses the same key
            # → the Email API dedupes the second request server-side. The
            # SentEmail row (committed below BEFORE the send) is the durable
            # marker the pre-check at the top of process_sequence_step
            # reconciles by — a retry that reaches this point sees the marker
            # and skips the re-send. The Idempotency-Key header is
            # defense-in-depth at the API layer.
            send_idempotency_key = f"seq-send:{enrollment_step.id}"
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
                    bcc=settings.salesforce_bcc_address or None,
                    idempotency_key=send_idempotency_key,
                )
                api_message_id = result["message_id"]

                sent_email.message_id = api_message_id
                sent_email.thread_id = result.get("thread_id")

                logger.info(
                    "Email sent via Telnyx Email API (HTML with tracking)",
                    from_email=mailbox.email,
                    to_email=enrollment.contact_email,
                    message_id=api_message_id,
                    idempotency_key=send_idempotency_key,
                )
            except EmailAPIError as e:
                logger.error("Email API send failed", error=str(e))
                # SV2-044 r3 (FAIL 1c): on an AMBIGUOUS timeout (retryable
                # error), the email may have been delivered — we do NOT know.
                # The r1 path DELETED the durable send marker (SentEmail row)
                # then retried → the next arq attempt didn't see the marker,
                # reserved capacity AGAIN, and sent AGAIN → duplicate
                # delivery after an ambiguous timeout. The r3 fix:
                #   - retryable (timeout/5xx/429/malformed-202): KEEP the
                #     marker. The next arq attempt's pre-check (the
                #     existing_send block at the top of this function)
                #     sees the marker and SKIPS the re-send (returns
                #     already_sent). Capacity stays reserved from the
                #     original attempt — if the send actually delivered,
                #     capacity is correctly consumed; if not, we lose one
                #     slot until daily reset (the safer trade-off vs a
                #     duplicate send to a prospect). The Idempotency-Key
                #     header is defense-in-depth at the API layer.
                #   - permanent 4xx: DELETE the marker (definitive
                #     non-delivery — the API rejected the request). The
                #     step is terminalized to FAILED below; no re-send.
                is_retryable = e.retryable
                if not is_retryable:
                    # Permanent 4xx — definitive non-delivery. Delete the
                    # marker so the step is retryable by a manual operator
                    # action (not arq retry — arq retries are for
                    # retryable errors only). Capacity is released.
                    try:
                        await db.delete(sent_email)
                        await db.commit()
                    except Exception as del_err:
                        logger.warning("Failed to remove send marker", error=str(del_err))
                    try:
                        await release_send(db, mailbox.id)
                    except Exception as rel_err:
                        logger.warning("Failed to release send slot", error=str(rel_err))
                else:
                    # Retryable (ambiguous timeout/5xx/429/malformed-202):
                    # KEEP the marker. The next arq attempt reconciles by
                    # the durable marker (the existing_send pre-check
                    # skips the re-send). Capacity stays reserved — see
                    # the comment above for the trade-off rationale.
                    logger.warning(
                        "Email API retryable error — keeping send marker "
                        "(at-most-once; next arq attempt reconciles by marker)",
                        error=str(e),
                        enrollment_step_id=enrollment_step_id,
                        idempotency_key=send_idempotency_key,
                    )
                    # Do NOT release capacity on ambiguous timeout — the
                    # original attempt may have consumed the slot. Releasing
                    # would let a re-send consume a SECOND slot for the same
                    # logical send, defeating the daily cap.
                # P1-A: retryable adapter errors (429/5xx/timeout) must raise
                # arq.worker.Retry so ARQ actually re-enqueues with backoff.
                # The adapter sets e.retryable=True for those and False for
                # permanent 4xx. r5: permanent 4xx terminalizes to a durable
                # EnrollmentStepStatus.FAILED (same contract as the r4
                # max_tries-exhausted path below — marker removed + capacity
                # released above the branch, error preserved in the log +
                # result dict) so the reconciler's predicate
                # (status == SCHEDULED) excludes it — no infinite
                # re-enqueue loop. The Gmail path's RuntimeError contract is
                # pre-existing semantics, untouched here (out of PR scope).
                if e.retryable:
                    # r4: the SINGLE choke point for last-attempt terminal
                    # conversion. ARQ exposes the 1-based attempt in
                    # ctx['job_try']; max_tries is the shared config
                    # (settings.worker_max_tries == WorkerSettings.max_tries).
                    # On the final permitted attempt a retryable error is
                    # converted to a durable terminal FAILED — the row
                    # leaves SCHEDULED so the reconciler cannot resurrect it.
                    # Before r4, exhausted retries left the row SCHEDULED and
                    # the reconciler re-enqueued it as fresh, so a
                    # permanently-failing step looped through retry storms
                    # forever. The marker is already removed and capacity
                    # already released above; the underlying error is
                    # preserved in the structured log + the result dict.
                    # Every retryable path (5xx/429/timeout + r3's
                    # malformed-202) raises a retryable EmailAPIError caught
                    # here — one check, not per-site.
                    job_try = int(ctx.get("job_try", 1))
                    max_tries = settings.worker_max_tries
                    if job_try >= max_tries:
                        enrollment_step.status = EnrollmentStepStatus.FAILED
                        await db.commit()
                        # r3 (FAIL 1c): on the terminal-FAILED path, RELEASE
                        # the capacity slot (no more retries — releasing is
                        # safe under at-most-once; a re-send is impossible
                        # because the row is FAILED and the reconciler won't
                        # resurrect it). The marker is KEPT (not deleted) —
                        # if the row is ever resurrected by a future code
                        # path, the marker prevents a re-send (at-most-once
                        # defense). The marker staying in the DB is harmless
                        # (the pre-check only runs at the top of
                        # process_sequence_step, which won't be called again
                        # for a FAILED row).
                        try:
                            await release_send(db, mailbox.id)
                        except Exception as rel_err:
                            logger.warning(
                                "Failed to release send slot on terminal failure",
                                error=str(rel_err),
                            )
                        logger.error(
                            "Email API retryable error exhausted max_tries "
                            "— terminal failure (row -> FAILED)",
                            job_try=job_try,
                            max_tries=max_tries,
                            error=str(e),
                            enrollment_step_id=enrollment_step_id,
                            mailbox_id=mailbox.id,
                        )
                        return {
                            "failed": True,
                            "reason": "max_retries_exhausted",
                            "error": str(e),
                            "job_try": job_try,
                            "max_tries": max_tries,
                        }
                    defer_s = _compute_retry_defer(e, ctx)
                    # r3: advance scheduled_at so the reconciler's own
                    # predicate (scheduled_at < now - grace) excludes this
                    # deferred step. Without this, a long Retry-After would
                    # leave scheduled_at at the original past value while the
                    # arq job is deferred — the reconciler would see it as
                    # "lost" and re-enqueue a second job (double-enqueue). The
                    # cap in _compute_retry_defer bounds the advance so a
                    # genuinely lost job is still detected within
                    # cap + grace.
                    enrollment_step.scheduled_at = datetime.utcnow() + timedelta(seconds=defer_s)
                    await db.commit()
                    logger.info(
                        "Email API retryable error — deferring arq retry",
                        defer_seconds=defer_s,
                        error=str(e),
                    )
                    raise ArqRetry(defer=defer_s) from e
                # r5: permanent 4xx — terminalize to FAILED (same contract as
                # the r4 max_tries-exhausted path above: marker already removed
                # and capacity already released above the branch; underlying
                # error preserved in the structured log + the result dict).
                # The reconciler's predicate (status == SCHEDULED) excludes
                # FAILED, so a permanent Email API rejection can no longer be
                # re-enqueued post-grace (the reviewer's scratch-PG
                # reproduction showed POST_GRACE_RECONCILED=2 on a 400). The
                # Gmail path's RuntimeError contract is pre-existing
                # semantics, untouched here — out of PR scope (documented in
                # the PR body).
                enrollment_step.status = EnrollmentStepStatus.FAILED
                await db.commit()
                logger.error(
                    "Email API permanent error — terminal failure (row -> FAILED)",
                    error=str(e),
                    status_code=e.status_code,
                    enrollment_step_id=enrollment_step_id,
                    mailbox_id=mailbox.id,
                )
                return {
                    "failed": True,
                    "reason": "permanent_error",
                    "error": str(e),
                    "status_code": e.status_code,
                }
        elif mailbox.transport == "gmail":
            if settings.gmail_enabled:
                # Gmail path (byte-identical to pre-1552 behavior). Offload
                # the blocking send to a worker thread (Fix 1) so the event
                # loop stays responsive for the other 9 arq jobs. Acquire the
                # per-mailbox lock (Fix 2) so two concurrent jobs on the SAME
                # mailbox cannot interleave httplib2 state on the shared
                # cached GmailService singleton.
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
                        bcc=settings.salesforce_bcc_address or None,
                    )
                    gmail_message_id = result["message_id"]
                    gmail_thread_id = result["thread_id"]

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
                    try:
                        await db.delete(sent_email)
                        await db.commit()
                    except Exception as del_err:
                        logger.warning("Failed to remove send marker", error=str(del_err))
                    try:
                        await release_send(db, mailbox.id)
                    except Exception as rel_err:
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
        else:
            # P1-B: unknown transport value — fail safe. No send on EITHER
            # transport. The DB CHECK constrains SQL writes, but ORM writes,
            # SQLite tests, and future values bypass it, so the dispatch must
            # not silently fall through to Gmail.
            logger.error(
                "Unknown mailbox transport — refusing to send",
                mailbox_id=mailbox.id,
                transport=mailbox.transport,
            )
            try:
                await db.delete(sent_email)
                await db.commit()
            except Exception as del_err:
                logger.warning("Failed to remove send marker", error=str(del_err))
            try:
                await release_send(db, mailbox.id)
            except Exception as rel_err:
                logger.warning("Failed to release send slot", error=str(rel_err))
            raise RuntimeError(
                f"Unknown mailbox transport {mailbox.transport!r} for "
                f"mailbox {mailbox.email} — refusing to send. "
                f"Expected 'gmail' or 'email_api'."
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
    delay_seconds = int(max(0, (next_enrollment_step.scheduled_at - now).total_seconds()))
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
