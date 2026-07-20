"""Reconciler for enrollment steps stranded in SCHEDULED (audit M4 / REVOPS-892).

`_queue_next_step` marks a step SCHEDULED and commits it *before* enqueuing the
arq job. If the enqueue throws, Redis is flushed, or the worker is down when the
deferred job should fire, the step is stranded SCHEDULED forever — the enrollment
never advances and never completes.

This cron sweep re-enqueues SCHEDULED steps that are overdue
(scheduled_at < now - grace) and pushes scheduled_at forward so a step that is
now back in flight is not re-selected on the next sweep.

NOTE: steps with a NULL scheduled_at are deliberately NOT reconciled here. Before
this fix scheduled_at was never written, so a NULL is ambiguous — it could be a
genuinely stuck step OR a step legitimately waiting on a valid future arq job
(re-firing the latter would send a follow-up early). Going forward every step
gets a scheduled_at, so NULL only exists on pre-fix rows; those are recovered by
a one-time backfill (scripts/backfill_scheduled_at_M4.py) that computes each
step's intended fire time, after which this sweep handles the overdue ones.

Per-mailbox capacity pacing (REVOPS-1378 / 2026-07-20 incident): the reconciler
previously re-enqueued up to reconcile_batch_limit(200) past-due steps every
10 min with delay_seconds=None — unpaced. Live 2026-07-20 this burned AMER's
entire 300/day budget in one hour (251 sends in the 13:00 UTC hour; all 8
mailboxes at sent_today=daily_send_limit=75 by 09:30 ET). The pacer now mirrors
the existing circuit_resume.py:124-138 precedent: cap each mailbox's
reconciliation per sweep, and reserve a fraction of daily_send_limit that the
reconciler may never touch, so catch-up trickles in behind in-flight work instead
of crowding it out of the shared send cap. Grouping is by enrollment.mailbox_id
(step.mailbox_id is NULL on all SCHEDULED rows — assigned at send time).
"""

import math
from collections import defaultdict
from datetime import datetime, timedelta

import structlog
from sqlalchemy import func, select

from src.config import get_settings
from src.models.base import async_session
from src.models.models import (
    EnrollmentStepStatus,
    Mailbox,
    Sequence,
    SequenceEnrollment,
    SequenceEnrollmentStep,
)
from src.services.queue import queue_sequence_step

settings = get_settings()
logger = structlog.get_logger()


async def reconcile_scheduled_steps(ctx: dict) -> dict:
    """Re-enqueue stuck SCHEDULED steps with per-mailbox capacity pacing.

    Returns {"reconciled": n, "scanned": m, "skipped_at_capacity": k,
    "skipped_reserve_floor": j, "past_due_backlog_depth": d, "per_mailbox": {...}}.
    """
    grace = timedelta(seconds=getattr(settings, "reconcile_grace_seconds", 600))
    batch_limit = getattr(settings, "reconcile_batch_limit", 100)
    per_mailbox_per_run = getattr(settings, "reconcile_per_mailbox_per_run", 10)
    reserve_fraction = getattr(settings, "reconcile_new_send_reserve_fraction", 0.30)
    now = datetime.utcnow()
    cutoff = now - grace
    reconciled = 0
    skipped_at_capacity = 0
    skipped_reserve_floor = 0
    per_mailbox_out: dict[str, dict] = {}

    async with async_session() as db:
        result = await db.execute(
            select(
                SequenceEnrollmentStep,
                Sequence.tenant_id,
                SequenceEnrollment.mailbox_id.label("enrollment_mailbox_id"),
            )
            .join(
                SequenceEnrollment,
                SequenceEnrollment.id == SequenceEnrollmentStep.enrollment_id,
            )
            .join(Sequence, Sequence.id == SequenceEnrollment.sequence_id)
            .where(
                SequenceEnrollmentStep.status == EnrollmentStepStatus.SCHEDULED,
                SequenceEnrollmentStep.scheduled_at < cutoff,
            )
            .order_by(SequenceEnrollmentStep.scheduled_at.asc())
            .limit(batch_limit)
        )
        rows = result.all()

        # Past-due backlog depth: total past-due SCHEDULED, uncapped (not just
        # this batch). Must be visibly trending down day over day; flat-or-rising
        # means arrivals exceed drain and needs escalation.
        backlog_depth = (
            await db.execute(
                select(func.count(SequenceEnrollmentStep.id)).where(
                    SequenceEnrollmentStep.status == EnrollmentStepStatus.SCHEDULED,
                    SequenceEnrollmentStep.scheduled_at < cutoff,
                )
            )
        ).scalar_one()

        # Bucket by enrollment.mailbox_id (NOT step.mailbox_id — NULL on all
        # SCHEDULED rows, assigned at send time). Ordered ASC by scheduled_at
        # above, so each bucket is already FIFO.
        buckets: dict[str, list] = defaultdict(list)
        for step, tenant_id, enrollment_mailbox_id in rows:
            buckets[enrollment_mailbox_id].append((step, tenant_id))

        # Per-mailbox capacity limiting — mirrors circuit_resume.py:124-138 with
        # an added reserve floor the reconciler may never consume.
        for mailbox_id, items in buckets.items():
            if mailbox_id is None:
                # Defensive: enrollment.mailbox_id is NOT NULL in the live schema
                # (sticky sender assigned at enrollment), but guard against a
                # legacy row so the sweep never silently drops steps.
                per_mailbox_out[str(mailbox_id)] = {
                    "reconciled": 0,
                    "spare": None,
                    "reason": "null_enrollment_mailbox",
                }
                continue

            mbx = (
                await db.execute(select(Mailbox).where(Mailbox.id == mailbox_id))
            ).scalar_one_or_none()

            daily_limit = mbx.daily_send_limit if mbx else 0
            sent_today = mbx.sent_today if mbx else 0
            spare = max(0, daily_limit - sent_today)
            floor = math.ceil(daily_limit * reserve_fraction)
            usable = max(0, spare - floor)
            limit = min(per_mailbox_per_run, usable)

            if limit <= 0:
                if spare == 0:
                    skipped_at_capacity += 1
                    reason = "exhausted"
                else:
                    skipped_reserve_floor += 1
                    reason = "reserve_floor"
                per_mailbox_out[mailbox_id] = {
                    "reconciled": 0,
                    "spare": spare,
                    "daily_limit": daily_limit,
                    "sent_today": sent_today,
                    "floor": floor,
                    "reason": reason,
                }
                logger.info(
                    "reconcile: mailbox deferred (capacity)",
                    mailbox_id=mailbox_id,
                    spare=spare,
                    daily_limit=daily_limit,
                    sent_today=sent_today,
                    floor=floor,
                    reason=reason,
                )
                continue

            mailbox_reconciled = 0
            for step, tenant_id in items[:limit]:
                try:
                    await queue_sequence_step(
                        enrollment_step_id=step.id,
                        tenant_id=tenant_id,
                        delay_seconds=None,
                    )
                except Exception as exc:  # don't let one bad row block the sweep
                    logger.error(
                        "reconcile: re-enqueue failed",
                        enrollment_step_id=step.id,
                        error=str(exc),
                    )
                    continue
                # Reset the grace window so an in-flight step isn't re-selected
                # next sweep.
                step.scheduled_at = now
                mailbox_reconciled += 1

            per_mailbox_out[mailbox_id] = {
                "reconciled": mailbox_reconciled,
                "spare": spare,
                "daily_limit": daily_limit,
                "sent_today": sent_today,
                "floor": floor,
                "limit": limit,
            }
            reconciled += mailbox_reconciled

        if reconciled:
            await db.commit()

    logger.info(
        "reconcile_scheduled_steps complete",
        reconciled=reconciled,
        scanned=len(rows),
        past_due_backlog_depth=backlog_depth,
        skipped_at_capacity=skipped_at_capacity,
        skipped_reserve_floor=skipped_reserve_floor,
        per_mailbox=per_mailbox_out,
    )
    return {
        "reconciled": reconciled,
        "scanned": len(rows),
        "past_due_backlog_depth": backlog_depth,
        "skipped_at_capacity": skipped_at_capacity,
        "skipped_reserve_floor": skipped_reserve_floor,
        "per_mailbox": per_mailbox_out,
    }
