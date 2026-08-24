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

DRY-RUN by default — prints the audit table / counts and writes only the
audit JSON (no DB mutations):
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

sys.path.insert(0, ".")
from src.models.base import async_session  # noqa: E402
from src.models.models import (  # noqa: E402
    EnrollmentStatus,
    EnrollmentStepStatus,
    Mailbox,
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


TERMINAL_STATUSES = (
    EnrollmentStatus.COMPLETED,
    EnrollmentStatus.BOUNCED,
    EnrollmentStatus.UNSUBSCRIBED,
)


async def _phase1_rows(db) -> list[dict]:
    """Return rich rows for terminal enrollments with PENDING/SCHEDULED steps."""
    rows = (
        await db.execute(
            select(
                SequenceEnrollmentStep.id.label("step_id"),
                SequenceEnrollmentStep.status.label("step_status"),
                SequenceEnrollment.id.label("enr_id"),
                SequenceEnrollment.contact_email.label("contact_email"),
                SequenceEnrollment.status.label("enr_status"),
                Mailbox.email.label("mailbox_email"),
            )
            .join(
                SequenceEnrollment,
                SequenceEnrollment.id == SequenceEnrollmentStep.enrollment_id,
            )
            .outerjoin(Mailbox, Mailbox.id == SequenceEnrollment.mailbox_id)
            .where(
                SequenceEnrollment.status.in_(TERMINAL_STATUSES),
                SequenceEnrollmentStep.status.in_(
                    (EnrollmentStepStatus.PENDING, EnrollmentStepStatus.SCHEDULED)
                ),
            )
            .order_by(SequenceEnrollment.id, SequenceEnrollmentStep.id)
        )
    ).all()
    by_enr: dict[str, dict] = {}
    for r in rows:
        e = by_enr.setdefault(
            r.enr_id,
            {
                "enrollment_id": r.enr_id,
                "mailbox_email": r.mailbox_email or "",
                "contact_email": r.contact_email,
                "status_before": r.enr_status.value,
                "status_after": r.enr_status.value,
                "step_ids": [],
                "step_pre_statuses": [],
            },
        )
        e["step_ids"].append(r.step_id)
        e["step_pre_statuses"].append(r.step_status.value)
    return list(by_enr.values())


async def _phase2_rows(db, stranded_days: int) -> list[dict]:
    """Return rich rows for stranded ACTIVE enrollments."""
    cutoff = datetime.utcnow() - timedelta(days=stranded_days)
    last_sent_subq = (
        select(
            SequenceEnrollmentStep.enrollment_id.label("enr_id"),
            func.max(SequenceEnrollmentStep.sent_at).label("last_sent"),
        )
        .where(SequenceEnrollmentStep.status == EnrollmentStepStatus.SENT)
        .group_by(SequenceEnrollmentStep.enrollment_id)
    ).subquery()
    has_pending_subq = (
        select(SequenceEnrollmentStep.enrollment_id).where(
            SequenceEnrollmentStep.status == EnrollmentStepStatus.PENDING
        )
    ).subquery()
    has_scheduled_subq = (
        select(SequenceEnrollmentStep.enrollment_id).where(
            SequenceEnrollmentStep.status == EnrollmentStepStatus.SCHEDULED
        )
    ).subquery()
    stranded_ids = (
        await db.execute(
            select(SequenceEnrollment.id)
            .outerjoin(last_sent_subq, last_sent_subq.c.enr_id == SequenceEnrollment.id)
            .where(
                SequenceEnrollment.status == EnrollmentStatus.ACTIVE,
                SequenceEnrollment.id.in_(select(has_pending_subq.c.enrollment_id)),
                ~SequenceEnrollment.id.in_(select(has_scheduled_subq.c.enrollment_id)),
                func.coalesce(last_sent_subq.c.last_sent, SequenceEnrollment.created_at)
                < cutoff,
            )
            .order_by(SequenceEnrollment.id)
        )
    ).all()
    if not stranded_ids:
        return []
    stranded_id_list = [r[0] for r in stranded_ids]
    step_rows = (
        await db.execute(
            select(
                SequenceEnrollmentStep.id.label("step_id"),
                SequenceEnrollmentStep.enrollment_id.label("enr_id"),
                SequenceEnrollment.contact_email.label("contact_email"),
                Mailbox.email.label("mailbox_email"),
            )
            .join(
                SequenceEnrollment,
                SequenceEnrollment.id == SequenceEnrollmentStep.enrollment_id,
            )
            .outerjoin(Mailbox, Mailbox.id == SequenceEnrollment.mailbox_id)
            .where(
                SequenceEnrollmentStep.enrollment_id.in_(stranded_id_list),
                SequenceEnrollmentStep.status == EnrollmentStepStatus.PENDING,
            )
            .order_by(SequenceEnrollment.id, SequenceEnrollmentStep.id)
        )
    ).all()
    by_enr: dict[str, dict] = {}
    for r in step_rows:
        e = by_enr.setdefault(
            r.enr_id,
            {
                "enrollment_id": r.enr_id,
                "mailbox_email": r.mailbox_email or "",
                "contact_email": r.contact_email,
                "status_before": EnrollmentStatus.ACTIVE.value,
                "status_after": EnrollmentStatus.COMPLETED.value,
                "step_ids": [],
                "step_pre_statuses": [],
            },
        )
        e["step_ids"].append(r.step_id)
        e["step_pre_statuses"].append(EnrollmentStepStatus.PENDING.value)
    return list(by_enr.values())


async def _phase1_apply(db, rows: list[dict]) -> tuple[int, int]:
    """Release steps of terminal enrollments. Returns (enrollments, steps)."""
    if not rows:
        return 0, 0
    enr_ids = [r["enrollment_id"] for r in rows]
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


async def _phase2_apply(db, rows: list[dict]) -> tuple[int, int]:
    """Complete stranded ACTIVE enrollments + skip their PENDING steps."""
    if not rows:
        return 0, 0
    enr_ids = [r["enrollment_id"] for r in rows]
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


def _phase1_count_block(rows: list[dict]) -> dict:
    """Per-status step counts for phase 1: {status: {PENDING: n, SCHEDULED: m}}."""
    counts: dict[str, dict[str, int]] = {
        s.value: {"PENDING": 0, "SCHEDULED": 0} for s in TERMINAL_STATUSES
    }
    for r in rows:
        sc = counts.setdefault(r["status_before"], {"PENDING": 0, "SCHEDULED": 0})
        for ps in r["step_pre_statuses"]:
            sc[ps] = sc.get(ps, 0) + 1
    return counts


def _print_table(title: str, rows: list[dict]) -> None:
    print(title)
    if not rows:
        print("  (none)")
        return
    print(
        f"  {'enrollment_id':<36} {'mailbox_email':<28} "
        f"{'contact_email':<28} {'status':<24} step_ids"
    )
    for r in rows:
        steps = ",".join(r["step_ids"])
        st = f"{r['status_before']} -> {r['status_after']}"
        print(
            f"  {r['enrollment_id']:<36} {r['mailbox_email']:<28} "
            f"{r['contact_email']:<28} {st:<24} {steps}"
        )


def _write_audit_json(
    path: str,
    phase1_rows: list[dict],
    phase1_counts: dict,
    phase2_rows: list[dict],
) -> None:
    import json

    data = {
        "phase1": phase1_rows,
        "phase1_counts": phase1_counts,
        "phase2": phase2_rows,
    }
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)


async def run(
    *,
    apply: bool,
    stranded_days: int,
    session_factory=None,
    audit_out: str = "./remediation_1668_audit.json",
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

    async with sf() as db:
        p1_rows = await _phase1_rows(db)
        p2_rows = await _phase2_rows(db, stranded_days)
        p1_counts = _phase1_count_block(p1_rows)

        print(
            "── REVOPS-1668 reconciler starvation remediation "
            f"— {'APPLY' if apply else 'DRY RUN'} ──────────────────"
        )
        print()
        _print_table("Phase 1 — terminal-enrollment step release:", p1_rows)
        print()
        print("Phase 1 — per-status step counts:")
        for status in (s.value for s in TERMINAL_STATUSES):
            c = p1_counts.get(status, {})
            print(
                f"  {status:<14} PENDING={c.get('PENDING', 0)} SCHEDULED={c.get('SCHEDULED', 0)}"
            )
        print()
        _print_table(
            f"Phase 2 — stranded pre-M4 enrollments (>={stranded_days}d):", p2_rows
        )
        print()

        if apply:
            phase1_enr, phase1_steps = await _phase1_apply(db, p1_rows)
            phase2_enr, phase2_steps = await _phase2_apply(db, p2_rows)
            await db.commit()
            print("── APPLIED ───────────────────────────────────────")
            print(f"  phase 1 enrollments released : {phase1_enr}")
            print(f"  phase 1 steps skipped        : {phase1_steps}")
            print(f"  phase 2 enrollments completed: {phase2_enr}")
            print(f"  phase 2 steps skipped        : {phase2_steps}")
        else:
            print("Nothing written. Re-run with --apply to remediate.")

        _write_audit_json(audit_out, p1_rows, p1_counts, p2_rows)
        print(f"Audit JSON written to: {audit_out}")

    return {
        "phase1_enrollments": len(p1_rows) if not apply else phase1_enr,
        "phase1_steps": phase1_steps,
        "phase2_enrollments": len(p2_rows) if not apply else phase2_enr,
        "phase2_steps": phase2_steps,
        "phase1_ids": [r["enrollment_id"] for r in p1_rows],
        "phase2_ids": [r["enrollment_id"] for r in p2_rows],
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--apply", action="store_true", help="write changes (default: dry-run)"
    )
    ap.add_argument(
        "--stranded-days",
        type=int,
        default=14,
        help="phase-2 stranded threshold in days (default: 14)",
    )
    ap.add_argument(
        "--audit-out",
        default="./remediation_1668_audit.json",
        help="path to write audit JSON (default: ./remediation_1668_audit.json)",
    )
    args = ap.parse_args()
    asyncio.run(
        run(
            apply=args.apply,
            stranded_days=args.stranded_days,
            audit_out=args.audit_out,
        )
    )


if __name__ == "__main__":
    main()
