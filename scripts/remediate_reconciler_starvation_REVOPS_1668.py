"""One-time remediation for reconciler starvation (REVOPS-1668).

Two phases, each idempotent and never touching PAUSED enrollments:

  1. Terminal-enrollment step release — enrollments with status in
     (COMPLETED, BOUNCED, UNSUBSCRIBED): set every PENDING/SCHEDULED step to
     SKIPPED. After D1 the reconciler no longer re-enqueues these, but pre-fix
     rows linger as dead backlog. Idempotent: a second run finds none (they're
     all SKIPPED).

  2. Stranded pre-M4 enrollments — ACTIVE enrollments with >=1 PENDING step,
     zero SCHEDULED steps, and most recent sent_at (or enrollment.created_at
     when no step was ever sent) older than --stranded-days (default 14): set
     their PENDING steps to SKIPPED and the enrollment to COMPLETED. The
     reconciler ignores NULL scheduled_at by design, so these never advance;
     the Scout capacity projection counts them against today forever.
     Idempotent: a second run finds 0 (they're COMPLETED, not ACTIVE).

DRY-RUN by default — prints affected ids / counts and writes nothing:
  python -m scripts.remediate_reconciler_starvation_REVOPS_1668
  python -m scripts.remediate_reconciler_starvation_REVOPS_1668 --apply
  python -m scripts.remediate_reconciler_starvation_REVOPS_1668 --apply --stranded-days 21

NOTE: pause_reason is left NULL on phase-2 enrollments. The CHECK on
sequence_enrollments.pause_reason only allows a fixed set of values
('circuit_breaker', 'reply', 'unsubscribe', 'bounce', 'manual'), none of which
fit a "stranded pre-M4 cleanup" marker. The enrollment is moved to COMPLETED
(its terminal state), which is itself the durable marker — a future query for
remediated rows is `status = 'COMPLETED' AND pause_reason IS NULL AND
updated_at >= <run time>`, or via the run's printed id list.
"""

import argparse
import asyncio
import sys
from datetime import datetime, timedelta

from sqlalchemy import func, select, update
from sqlalchemy.orm import selectinload

sys.path.insert(0, ".")
from src.config import get_settings  # noqa: E402
from src.models.base import async_session  # noqa: E402
from src.models.models import (  # noqa: E402
    EnrollmentStatus,
    EnrollmentStepStatus,
    SequenceEnrollment,
    SequenceEnrollmentStep,
)


TERMINAL_STATUSES = (
    EnrollmentStatus.COMPLETED,
    EnrollmentStatus.BOUNCED,
    EnrollmentStatus.UNSUBSCRIBED,
)


async def _phase1_preview(db) -> list[str]:
    """Return enrollment ids with terminal status and >=1 PENDING/SCHEDULED step."""
    subq = (
        select(SequenceEnrollmentStep.enrollment_id)
        .where(SequenceEnrollmentStep.status.in_(
            (EnrollmentStepStatus.PENDING, EnrollmentStepStatus.SCHEDULED)
        ))
    ).subquery()
    rows = (
        await db.execute(
            select(SequenceEnrollment.id)
            .where(
                SequenceEnrollment.status.in_(TERMINAL_STATUSES),
                SequenceEnrollment.id.in_(select(subq.c.enrollment_id)),
            )
            .order_by(SequenceEnrollment.id)
        )
    ).all()
    return [r[0] for r in rows]


async def _phase1_apply(db) -> tuple[int, int]:
    """Release steps of terminal enrollments. Returns (enrollments, steps)."""
    enr_ids = await _phase1_preview(db)
    if not enr_ids:
        return 0, 0
    result = await db.execute(
        update(SequenceEnrollmentStep)
        .where(
            SequenceEnrollmentStep.enrollment_id.in_(enr_ids),
            SequenceEnrollmentStep.status.in_(
                (EnrollmentStepStatus.PENDING, EnrollmentStepStatus.SCHEDULED)
            ),
        )
        .values(status=EnrollmentStepStatus.SKIPPED)
    )
    return len(enr_ids), result.rowcount


async def _phase2_preview(db, stranded_days: int) -> list[str]:
    """Return ACTIVE enrollment ids that are stranded pre-M4 rows."""
    cutoff = datetime.utcnow() - timedelta(days=stranded_days)
    last_sent_subq = (
        select(
            SequenceEnrollmentStep.enrollment_id.label("enr_id"),
            func.max(SequenceEnrollmentStep.sent_at).label("last_sent"),
        )
        .group_by(SequenceEnrollmentStep.enrollment_id)
    ).subquery()
    has_pending_subq = (
        select(SequenceEnrollmentStep.enrollment_id)
        .where(SequenceEnrollmentStep.status == EnrollmentStepStatus.PENDING)
    ).subquery()
    has_scheduled_subq = (
        select(SequenceEnrollmentStep.enrollment_id)
        .where(SequenceEnrollmentStep.status == EnrollmentStepStatus.SCHEDULED)
    ).subquery()
    rows = (
        await db.execute(
            select(SequenceEnrollment.id)
            .outerjoin(last_sent_subq, last_sent_subq.c.enr_id == SequenceEnrollment.id)
            .where(
                SequenceEnrollment.status == EnrollmentStatus.ACTIVE,
                SequenceEnrollment.id.in_(select(has_pending_subq.c.enrollment_id)),
                ~SequenceEnrollment.id.in_(
                    select(has_scheduled_subq.c.enrollment_id)
                ),
                func.coalesce(last_sent_subq.c.last_sent, SequenceEnrollment.created_at)
                < cutoff,
            )
            .order_by(SequenceEnrollment.id)
        )
    ).all()
    return [r[0] for r in rows]


async def _phase2_apply(db, stranded_days: int) -> tuple[int, int]:
    """Complete stranded ACTIVE enrollments + skip their PENDING steps."""
    enr_ids = await _phase2_preview(db, stranded_days)
    if not enr_ids:
        return 0, 0
    step_result = await db.execute(
        update(SequenceEnrollmentStep)
        .where(
            SequenceEnrollmentStep.enrollment_id.in_(enr_ids),
            SequenceEnrollmentStep.status == EnrollmentStepStatus.PENDING,
        )
        .values(status=EnrollmentStepStatus.SKIPPED)
    )
    await db.execute(
        update(SequenceEnrollment)
        .where(SequenceEnrollment.id.in_(enr_ids))
        .values(status=EnrollmentStatus.COMPLETED, pause_reason=None)
    )
    return len(enr_ids), step_result.rowcount


async def run(
    *,
    apply: bool,
    stranded_days: int,
    session_factory=None,
) -> dict:
    """Execute the two-phase remediation. Returns a summary dict.

    Pass `session_factory` to run against a test DB; omit it to use the app's
    `async_session` (the live sequence_service DB).
    """
    sf = session_factory or async_session
    phase1_enr = 0
    phase1_steps = 0
    phase2_enr = 0
    phase2_steps = 0
    phase1_ids: list[str] = []
    phase2_ids: list[str] = []

    async with sf() as db:
        phase1_ids = await _phase1_preview(db)
        phase2_ids = await _phase2_preview(db, stranded_days)
        print("── REVOPS-1668 reconciler starvation remediation "
              f"— {'APPLY' if apply else 'DRY RUN'} ──────────────────")
        print(f"Phase 1 — terminal-enrollment step release:")
        print(f"  enrollments (COMPLETED/BOUNCED/UNSUBSCRIBED): {len(phase1_ids)}")
        print(f"  ids: {phase1_ids}")
        print(f"Phase 2 — stranded pre-M4 enrollments (>={stranded_days}d):")
        print(f"  enrollments (ACTIVE, stranded): {len(phase2_ids)}")
        print(f"  ids: {phase2_ids}")
        if apply:
            phase1_enr, phase1_steps = await _phase1_apply(db)
            phase2_enr, phase2_steps = await _phase2_apply(db, stranded_days)
            await db.commit()
            print()
            print("── APPLIED ───────────────────────────────────────")
            print(f"  phase 1 enrollments released : {phase1_enr}")
            print(f"  phase 1 steps skipped        : {phase1_steps}")
            print(f"  phase 2 enrollments completed: {phase2_enr}")
            print(f"  phase 2 steps skipped        : {phase2_steps}")
        else:
            print("Nothing written. Re-run with --apply to remediate.")
    return {
        "phase1_enrollments": len(phase1_ids) if not apply else phase1_enr,
        "phase1_steps": phase1_steps,
        "phase2_enrollments": len(phase2_ids) if not apply else phase2_enr,
        "phase2_steps": phase2_steps,
        "phase1_ids": phase1_ids,
        "phase2_ids": phase2_ids,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--apply", action="store_true", help="write changes (default: dry-run)"
    )
    ap.add_argument(
        "--stranded-days", type=int, default=14,
        help="phase-2 stranded threshold in days (default: 14)",
    )
    args = ap.parse_args()
    asyncio.run(run(apply=args.apply, stranded_days=args.stranded_days))


if __name__ == "__main__":
    main()
