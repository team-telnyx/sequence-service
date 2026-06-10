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
"""
from datetime import datetime, timedelta

import structlog
from sqlalchemy import select

from src.config import get_settings
from src.models.base import async_session
from src.models.models import (
    EnrollmentStepStatus,
    Sequence,
    SequenceEnrollment,
    SequenceEnrollmentStep,
)
from src.services.queue import queue_sequence_step

settings = get_settings()
logger = structlog.get_logger()


async def reconcile_scheduled_steps(ctx: dict) -> dict:
    """Re-enqueue stuck SCHEDULED steps. Returns {"reconciled": n, "scanned": m}."""
    grace = timedelta(seconds=getattr(settings, "reconcile_grace_seconds", 600))
    limit = getattr(settings, "reconcile_batch_limit", 100)
    now = datetime.utcnow()
    cutoff = now - grace
    reconciled = 0

    async with async_session() as db:
        result = await db.execute(
            select(SequenceEnrollmentStep, Sequence.tenant_id)
            .join(
                SequenceEnrollment,
                SequenceEnrollment.id == SequenceEnrollmentStep.enrollment_id,
            )
            .join(Sequence, Sequence.id == SequenceEnrollment.sequence_id)
            .where(
                SequenceEnrollmentStep.status == EnrollmentStepStatus.SCHEDULED,
                # NULL scheduled_at is excluded by this comparison (NULL < x is
                # NULL/false) — see module docstring; those are backfilled, not
                # auto-reconciled.
                SequenceEnrollmentStep.scheduled_at < cutoff,
            )
            .order_by(SequenceEnrollmentStep.scheduled_at.asc())
            .limit(limit)
        )
        rows = result.all()

        for step, tenant_id in rows:
            try:
                await queue_sequence_step(
                    enrollment_step_id=step.id,
                    tenant_id=tenant_id,
                    delay_seconds=None,
                )
            except Exception as exc:  # don't let one bad row block the sweep
                logger.error(
                    "reconcile: re-enqueue failed",
                    enrollment_step_id=step.id, error=str(exc),
                )
                continue
            # Reset the grace window so an in-flight step isn't re-selected next sweep.
            step.scheduled_at = now
            reconciled += 1

        if reconciled:
            await db.commit()

    logger.info(
        "reconcile_scheduled_steps complete",
        reconciled=reconciled, scanned=len(rows),
    )
    return {"reconciled": reconciled, "scanned": len(rows)}
