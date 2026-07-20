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

Two blocker fixes from the PR #23 planner review (2026-07-20):
- Selection uses ROW_NUMBER() OVER (PARTITION BY enrollment.mailbox_id ORDER BY
  scheduled_at ASC) so a correlated clump on one mailbox cannot fill the entire
  batch and starve every other mailbox (a global ORDER BY ... LIMIT taken before
  bucketing does exactly that — the original implementation).
- The per-mailbox limit spreads the daily `usable` allowance across the send
  window's sweeps (`allowance = ceil(usable / sweeps_left)`), bounding the *hourly
  rate*, not just the daily total. Without it `per_run(10) × 6 sweeps/hr × 4
  mailboxes ≈ 208 sends in hour one` — a 17% reduction on the 251 incident, not a
  fix. The feedback lag (sent_today increments at send time while pacing decides
  at enqueue time) makes the spread essential: several sweeps fire before the
  counter reflects any of them.
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

    Returns {"reconciled": n, "scanned": m, "past_due_backlog_depth": d,
    "skipped_at_capacity": k, "skipped_reserve_floor": j,
    "skipped_mailbox_missing": p, "skipped_no_mailbox": q,
    "per_mailbox": {mailbox_id: {reconciled, pending, spare, ...}}}.
    """
    grace = timedelta(seconds=getattr(settings, "reconcile_grace_seconds", 600))
    batch_limit = getattr(settings, "reconcile_batch_limit", 100)
    per_mailbox_per_run = getattr(settings, "reconcile_per_mailbox_per_run", 10)
    reserve_fraction = getattr(settings, "reconcile_new_send_reserve_fraction", 0.30)
    pacing_window_hours = getattr(settings, "reconcile_pacing_window_hours", 9)
    sweep_minutes = getattr(settings, "reconcile_sweep_minutes", 10)
    now = datetime.utcnow()
    cutoff = now - grace

    sweeps_left = max(1, math.ceil(pacing_window_hours * 60 / sweep_minutes))

    reconciled = 0
    skipped_at_capacity = 0
    skipped_reserve_floor = 0
    skipped_mailbox_missing = 0
    skipped_no_mailbox = 0
    per_mailbox_out: dict = {}

    async with async_session() as db:
        # Per-mailbox pending depth over the past-due predicate. One GROUP BY
        # query supplies both the per-mailbox `pending` observability field AND
        # the global `past_due_backlog_depth` (sum of the per-mailbox counts),
        # replacing the separate global count.
        pending_rows = (
            await db.execute(
                select(
                    SequenceEnrollment.mailbox_id.label("mid"),
                    func.count(SequenceEnrollmentStep.id).label("pending"),
                )
                .join(
                    SequenceEnrollment,
                    SequenceEnrollment.id == SequenceEnrollmentStep.enrollment_id,
                )
                .where(
                    SequenceEnrollmentStep.status == EnrollmentStepStatus.SCHEDULED,
                    # NULL < cutoff is NULL/false — a NULL scheduled_at is NOT
                    # selected. A future `OR scheduled_at IS NULL` here would
                    # re-fire follow-ups whose arq job is legitimately pending.
                    SequenceEnrollmentStep.scheduled_at < cutoff,
                )
                .group_by(SequenceEnrollment.mailbox_id)
            )
        ).all()
        pending_by_mailbox: dict = {row.mid: row.pending for row in pending_rows}
        backlog_depth = sum(pending_by_mailbox.values())

        # Selection: ROW_NUMBER() OVER (PARTITION BY enrollment.mailbox_id ORDER BY
        # scheduled_at ASC) bounds the fetch to per_mailbox_per_run × n_mailboxes
        # and guarantees every mailbox is represented. A global
        # ORDER BY scheduled_at LIMIT batch_limit taken *before* bucketing lets
        # one mailbox with the oldest past-due clump fill the entire batch and
        # starve every other mailbox indefinitely (BLOCKER 1 from the PR #23
        # review). batch_limit is an outer safety bound applied AFTER partitioning.
        rn = (
            func.row_number()
            .over(
                partition_by=SequenceEnrollment.mailbox_id,
                order_by=SequenceEnrollmentStep.scheduled_at.asc(),
            )
            .label("rn")
        )
        subq = (
            select(
                SequenceEnrollmentStep.id.label("step_id"),
                SequenceEnrollmentStep.scheduled_at.label("scheduled_at"),
                Sequence.tenant_id.label("tenant_id"),
                SequenceEnrollment.mailbox_id.label("enrollment_mailbox_id"),
                rn,
            )
            .join(
                SequenceEnrollment,
                SequenceEnrollment.id == SequenceEnrollmentStep.enrollment_id,
            )
            .join(Sequence, Sequence.id == SequenceEnrollment.sequence_id)
            .where(
                SequenceEnrollmentStep.status == EnrollmentStepStatus.SCHEDULED,
                # NULL < cutoff is NULL/false — see comment on pending query.
                SequenceEnrollmentStep.scheduled_at < cutoff,
            )
        ).subquery()
        stmt = (
            select(
                subq.c.step_id,
                subq.c.scheduled_at,
                subq.c.tenant_id,
                subq.c.enrollment_mailbox_id,
            )
            .where(subq.c.rn <= per_mailbox_per_run)
            .order_by(subq.c.enrollment_mailbox_id, subq.c.scheduled_at)
            .limit(batch_limit)
        )
        rows = (await db.execute(stmt)).all()

        # Bucket by enrollment.mailbox_id (NOT step.mailbox_id — NULL on all
        # SCHEDULED rows, assigned at send time). The subquery already ordered
        # by (enrollment_mailbox_id, scheduled_at), so each bucket is FIFO.
        buckets: dict = defaultdict(list)
        for row in rows:
            buckets[row.enrollment_mailbox_id].append(row)

        # Fetch all referenced mailboxes in one query (avoids per-bucket N+1).
        mailbox_ids = [mid for mid in buckets if mid is not None]
        mailboxes: dict = {}
        if mailbox_ids:
            mbx_rows = (
                (await db.execute(select(Mailbox).where(Mailbox.id.in_(mailbox_ids))))
                .scalars()
                .all()
            )
            mailboxes = {m.id: m for m in mbx_rows}

        for mailbox_id, items in buckets.items():
            pending = pending_by_mailbox.get(mailbox_id, 0)

            if mailbox_id is None:
                # Defensive: enrollment.mailbox_id is NOT NULL in the live schema
                # (sticky sender assigned at enrollment), but a legacy row could
                # slip through. Increment a counter so the drop is observable,
                # never silent.
                skipped_no_mailbox += len(items)
                per_mailbox_out[mailbox_id] = {
                    "reconciled": 0,
                    "pending": pending,
                    "reason": "null_enrollment_mailbox",
                }
                logger.warning(
                    "reconcile: steps with NULL enrollment.mailbox_id dropped",
                    count=len(items),
                )
                continue

            mbx = mailboxes.get(mailbox_id)
            if mbx is None:
                # A missing Mailbox row (deleted mailbox, stale FK) is an error,
                # NOT "exhausted". Falling through to daily_limit=0 would stall
                # these steps forever behind a metric that looks like normal
                # capping.
                skipped_mailbox_missing += len(items)
                per_mailbox_out[mailbox_id] = {
                    "reconciled": 0,
                    "pending": pending,
                    "reason": "mailbox_missing",
                }
                logger.error(
                    "reconcile: enrollment references missing mailbox row",
                    mailbox_id=mailbox_id,
                    past_due_steps=len(items),
                )
                continue

            daily_limit = mbx.daily_send_limit
            sent_today = mbx.sent_today
            spare = max(0, daily_limit - sent_today)
            floor = math.ceil(daily_limit * reserve_fraction)
            usable = max(0, spare - floor)
            allowance = max(1, math.ceil(usable / sweeps_left))
            limit = min(per_mailbox_per_run, allowance, usable)

            if limit <= 0:
                if spare == 0:
                    skipped_at_capacity += 1
                    reason = "exhausted"
                else:
                    skipped_reserve_floor += 1
                    reason = "reserve_floor"
                per_mailbox_out[mailbox_id] = {
                    "reconciled": 0,
                    "pending": pending,
                    "spare": spare,
                    "daily_limit": daily_limit,
                    "sent_today": sent_today,
                    "floor": floor,
                    "allowance": allowance,
                    "reason": reason,
                }
                logger.info(
                    "reconcile: mailbox deferred (capacity)",
                    mailbox_id=mailbox_id,
                    spare=spare,
                    daily_limit=daily_limit,
                    sent_today=sent_today,
                    floor=floor,
                    allowance=allowance,
                    reason=reason,
                )
                continue

            mailbox_reconciled = 0
            for row in items[:limit]:
                step = await db.get(SequenceEnrollmentStep, row.step_id)
                if step is None:
                    continue
                try:
                    await queue_sequence_step(
                        enrollment_step_id=step.id,
                        tenant_id=row.tenant_id,
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
                "pending": pending,
                "spare": spare,
                "daily_limit": daily_limit,
                "sent_today": sent_today,
                "floor": floor,
                "allowance": allowance,
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
        skipped_mailbox_missing=skipped_mailbox_missing,
        skipped_no_mailbox=skipped_no_mailbox,
        per_mailbox=per_mailbox_out,
    )
    return {
        "reconciled": reconciled,
        "scanned": len(rows),
        "past_due_backlog_depth": backlog_depth,
        "skipped_at_capacity": skipped_at_capacity,
        "skipped_reserve_floor": skipped_reserve_floor,
        "skipped_mailbox_missing": skipped_mailbox_missing,
        "skipped_no_mailbox": skipped_no_mailbox,
        "per_mailbox": per_mailbox_out,
    }
