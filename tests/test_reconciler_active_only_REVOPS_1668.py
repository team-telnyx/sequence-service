"""REVOPS-1668 — Reconciler only touches ACTIVE enrollments + terminal step
release + stranded pre-M4 cleanup.

D1: reconcile_scheduled_steps filters SequenceEnrollment.status == ACTIVE on
both the pending-depth query and the selection subquery. Non-ACTIVE past-due
steps are NOT enqueued, NOT counted in past_due_backlog_depth, and counted in
a new skipped_inactive_enrollment summary field. A stranded_active_enrollments
counter observes (never mutates) ACTIVE enrollments whose last SENT (or
created_at) is older than settings.stranded_enrollment_days with PENDING steps
and no SCHEDULED step.

D2: process_sequence_step on a step of a terminal enrollment (COMPLETED /
BOUNCED / UNSUBSCRIBED) sets the step SKIPPED; PAUSED leaves the step
untouched (circuit_resume depends on it).

D3: scripts/remediate_reconciler_starvation_REVOPS_1668.py — idempotent two-
phase cleanup (terminal release + stranded pre-M4 → COMPLETED).
"""

from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest

import src.workers.reconcile as rec
import src.workers.sequence_step as ss
from src.models.models import (
    SequenceEnrollment,
    SequenceEnrollmentStep,
    EnrollmentStatus,
    EnrollmentStepStatus,
)


async def _make_enrollment(
    session_factory,
    seeded,
    *,
    enr_id,
    status,
    mailbox_id=None,
    created_at=None,
):
    async with session_factory() as s:
        s.add(
            SequenceEnrollment(
                id=enr_id,
                sequence_id=seeded["sequence_id"],
                mailbox_id=mailbox_id or seeded["active_mailbox_id"],
                contact_email=f"vp+{enr_id}@acme.com",
                contact_name="VP",
                timezone="America/New_York",
                status=status,
                current_step=0,
                created_at=created_at or datetime.utcnow(),
            )
        )
        await s.commit()
    return enr_id


async def _make_step(
    session_factory,
    *,
    enrollment_id,
    status,
    scheduled_at,
    sent_at=None,
    est_id="est-1",
    step_id="step-1",
    mailbox_id=None,
):
    async with session_factory() as s:
        s.add(
            SequenceEnrollmentStep(
                id=est_id,
                enrollment_id=enrollment_id,
                step_id=step_id,
                mailbox_id=mailbox_id,
                status=status,
                scheduled_at=scheduled_at,
                sent_at=sent_at,
                custom_subject="Hi",
                custom_body="<p>B</p>",
            )
        )
        await s.commit()
    return est_id


async def _status_and_sched(session_factory, est_id):
    async with session_factory() as s:
        st = await s.get(SequenceEnrollmentStep, est_id)
        return st.status, st.scheduled_at


def _patch(session_factory, q):
    return [
        patch.object(rec, "async_session", session_factory),
        patch.object(rec, "queue_sequence_step", q),
    ]


def _enter(cms):
    for cm in cms:
        cm.start()


def _exit(cms):
    for cm in cms:
        cm.stop()


def PAST():
    return datetime.utcnow() - timedelta(hours=2)


# ── D1 tests 1-3: non-ACTIVE enrollments are not reconciled ──────────────


@pytest.mark.asyncio
async def test_paused_enrollment_step_not_reconciled(
    seeded, session_factory, monkeypatch
):
    monkeypatch.setattr(rec.settings, "reconcile_grace_seconds", 600, raising=False)
    enr = await _make_enrollment(
        session_factory, seeded, enr_id="enr-paused", status=EnrollmentStatus.PAUSED
    )
    est = await _make_step(
        session_factory,
        enrollment_id=enr,
        status=EnrollmentStepStatus.SCHEDULED,
        scheduled_at=PAST(),
        est_id="est-paused",
    )
    q = AsyncMock(return_value="job-1")
    cms = _patch(session_factory, q)
    _enter(cms)
    try:
        out = await rec.reconcile_scheduled_steps({})
    finally:
        _exit(cms)
    assert out["reconciled"] == 0
    q.assert_not_awaited()
    assert out["past_due_backlog_depth"] == 0
    assert out["skipped_inactive_enrollment"] == 1
    status, _ = await _status_and_sched(session_factory, est)
    assert status == EnrollmentStepStatus.SCHEDULED


@pytest.mark.asyncio
async def test_unsubscribed_enrollment_step_not_reconciled(
    seeded, session_factory, monkeypatch
):
    monkeypatch.setattr(rec.settings, "reconcile_grace_seconds", 600, raising=False)
    enr = await _make_enrollment(
        session_factory,
        seeded,
        enr_id="enr-unsub",
        status=EnrollmentStatus.UNSUBSCRIBED,
    )
    est = await _make_step(
        session_factory,
        enrollment_id=enr,
        status=EnrollmentStepStatus.SCHEDULED,
        scheduled_at=PAST(),
        est_id="est-unsub",
    )
    q = AsyncMock(return_value="job-1")
    cms = _patch(session_factory, q)
    _enter(cms)
    try:
        out = await rec.reconcile_scheduled_steps({})
    finally:
        _exit(cms)
    assert out["reconciled"] == 0
    q.assert_not_awaited()
    assert out["past_due_backlog_depth"] == 0
    assert out["skipped_inactive_enrollment"] == 1
    status, _ = await _status_and_sched(session_factory, est)
    assert status == EnrollmentStepStatus.SCHEDULED


@pytest.mark.asyncio
async def test_active_enrollment_step_still_reconciled(
    seeded, session_factory, monkeypatch
):
    monkeypatch.setattr(rec.settings, "reconcile_grace_seconds", 600, raising=False)
    monkeypatch.setattr(rec.settings, "reconcile_pacing_window_hours", 1, raising=False)
    enr = await _make_enrollment(
        session_factory, seeded, enr_id="enr-active", status=EnrollmentStatus.ACTIVE
    )
    est = await _make_step(
        session_factory,
        enrollment_id=enr,
        status=EnrollmentStepStatus.SCHEDULED,
        scheduled_at=PAST(),
        est_id="est-active",
    )
    q = AsyncMock(return_value="job-1")
    cms = _patch(session_factory, q)
    _enter(cms)
    try:
        out = await rec.reconcile_scheduled_steps({})
    finally:
        _exit(cms)
    assert out["reconciled"] == 1
    q.assert_awaited_once()
    assert out["past_due_backlog_depth"] == 1
    assert out["skipped_inactive_enrollment"] == 0
    _, sched = await _status_and_sched(session_factory, est)
    assert sched > datetime.utcnow() - timedelta(seconds=60)


# ── D1 tests 4-5: allowance + stranded counter ───────────────────────────


@pytest.mark.asyncio
async def test_dead_paused_step_does_not_consume_mailbox_allowance(
    seeded, session_factory, monkeypatch
):
    monkeypatch.setattr(rec.settings, "reconcile_grace_seconds", 600, raising=False)
    monkeypatch.setattr(rec.settings, "reconcile_pacing_window_hours", 1, raising=False)
    # Two enrollments on the SAME mailbox: a PAUSED one with an OLDER scheduled_at
    # (would have been dequeued first under the old global ORDER BY) and an ACTIVE
    # one with a newer scheduled_at. Under the ACTIVE filter the PAUSED step is
    # excluded entirely, so the ACTIVE step is enqueued in the same sweep.
    mb = seeded["active_mailbox_id"]
    enr_dead = await _make_enrollment(
        session_factory,
        seeded,
        enr_id="enr-dead",
        status=EnrollmentStatus.PAUSED,
        mailbox_id=mb,
    )
    older = PAST() - timedelta(hours=1)
    await _make_step(
        session_factory,
        enrollment_id=enr_dead,
        status=EnrollmentStepStatus.SCHEDULED,
        scheduled_at=older,
        est_id="est-dead",
        mailbox_id=mb,
    )
    enr_live = await _make_enrollment(
        session_factory,
        seeded,
        enr_id="enr-live",
        status=EnrollmentStatus.ACTIVE,
        mailbox_id=mb,
    )
    await _make_step(
        session_factory,
        enrollment_id=enr_live,
        status=EnrollmentStepStatus.SCHEDULED,
        scheduled_at=PAST(),
        est_id="est-live",
        mailbox_id=mb,
    )
    q = AsyncMock(return_value="job-live")
    cms = _patch(session_factory, q)
    _enter(cms)
    try:
        out = await rec.reconcile_scheduled_steps({})
    finally:
        _exit(cms)
    assert out["reconciled"] == 1
    q.assert_awaited_once()
    assert q.await_args.kwargs["enrollment_step_id"] == "est-live"
    assert out["skipped_inactive_enrollment"] == 1


@pytest.mark.asyncio
async def test_stranded_active_enrollments_counter(
    seeded, session_factory, monkeypatch
):
    monkeypatch.setattr(rec.settings, "reconcile_grace_seconds", 600, raising=False)
    monkeypatch.setattr(rec.settings, "stranded_enrollment_days", 14, raising=False)

    old = datetime.utcnow() - timedelta(days=30)
    recent = datetime.utcnow() - timedelta(days=2)

    # (a) stranded: ACTIVE, PENDING step, no SCHEDULED, last SENT older than 14d.
    enr_a = await _make_enrollment(
        session_factory,
        seeded,
        enr_id="enr-a",
        status=EnrollmentStatus.ACTIVE,
        created_at=old,
    )
    await _make_step(
        session_factory,
        enrollment_id=enr_a,
        status=EnrollmentStepStatus.PENDING,
        scheduled_at=None,
        sent_at=None,
        est_id="est-a-pending",
    )
    await _make_step(
        session_factory,
        enrollment_id=enr_a,
        status=EnrollmentStepStatus.SENT,
        scheduled_at=None,
        sent_at=old,
        est_id="est-a-sent",
    )

    # (b) NOT stranded: last SENT inside the threshold (2 days ago).
    enr_b = await _make_enrollment(
        session_factory,
        seeded,
        enr_id="enr-b",
        status=EnrollmentStatus.ACTIVE,
        created_at=old,
    )
    await _make_step(
        session_factory,
        enrollment_id=enr_b,
        status=EnrollmentStepStatus.PENDING,
        scheduled_at=None,
        sent_at=None,
        est_id="est-b-pending",
    )
    await _make_step(
        session_factory,
        enrollment_id=enr_b,
        status=EnrollmentStepStatus.SENT,
        scheduled_at=None,
        sent_at=recent,
        est_id="est-b-sent",
    )

    # (c) NOT stranded: has a SCHEDULED step (so the reconciler is still pushing it).
    enr_c = await _make_enrollment(
        session_factory,
        seeded,
        enr_id="enr-c",
        status=EnrollmentStatus.ACTIVE,
        created_at=old,
    )
    await _make_step(
        session_factory,
        enrollment_id=enr_c,
        status=EnrollmentStepStatus.PENDING,
        scheduled_at=None,
        sent_at=None,
        est_id="est-c1",
    )
    await _make_step(
        session_factory,
        enrollment_id=enr_c,
        status=EnrollmentStepStatus.SCHEDULED,
        scheduled_at=PAST(),
        sent_at=None,
        est_id="est-c2",
    )

    q = AsyncMock(return_value="job-1")
    cms = _patch(session_factory, q)
    _enter(cms)
    try:
        out = await rec.reconcile_scheduled_steps({})
    finally:
        _exit(cms)
    # Only enr-c has a SCHEDULED past-due step, so reconciled == 1; the stranded
    # count is exactly enr-a (enr-b inside threshold, enr-c has a SCHEDULED step).
    assert out["stranded_active_enrollments"] == 1
    assert out["reconciled"] == 1


# ── D2 test 6: terminal enrollments release their steps; PAUSED is untouched ─


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,expected_step_status",
    [
        (EnrollmentStatus.UNSUBSCRIBED, EnrollmentStepStatus.SKIPPED),
        (EnrollmentStatus.COMPLETED, EnrollmentStepStatus.SKIPPED),
        (EnrollmentStatus.BOUNCED, EnrollmentStepStatus.SKIPPED),
        (EnrollmentStatus.PAUSED, EnrollmentStepStatus.SCHEDULED),
    ],
)
async def test_process_step_inactive_enrollment(
    seeded, session_factory, status, expected_step_status
):
    enr_id = f"enr-{status.value}"
    enr = await _make_enrollment(session_factory, seeded, enr_id=enr_id, status=status)
    est = await _make_step(
        session_factory,
        enrollment_id=enr,
        status=EnrollmentStepStatus.SCHEDULED,
        scheduled_at=PAST(),
        est_id=f"est-{status.value}",
    )
    with patch.object(ss, "async_session", session_factory):
        out = await ss.process_sequence_step({}, est, seeded["tenant_id"])
    assert out.get("skipped") is True
    assert out["reason"] == "enrollment_not_active"
    assert out["enrollment_status"] == status.value
    final_status, _ = await _status_and_sched(session_factory, est)
    assert final_status == expected_step_status


# ── D3 test 7: remediation script dry-run / apply / idempotency ──────────


async def _seed_script_scenario(session_factory, seeded):
    """Seed: one terminal (UNSUBSCRIBED) enrollment with a SCHEDULED step,
    one stranded ACTIVE enrollment with a PENDING step (old created_at, no
    SENT, no SCHEDULED), one PAUSED enrollment with a PENDING step (must be
    untouched), and one recent ACTIVE enrollment (inside the stranded
    threshold — must NOT be completed by phase 2)."""
    old = datetime.utcnow() - timedelta(days=30)
    mb = seeded["active_mailbox_id"]

    async with session_factory() as s:
        s.add(
            SequenceEnrollment(
                id="enr-term",
                sequence_id=seeded["sequence_id"],
                mailbox_id=mb,
                contact_email="term@acme.com",
                contact_name="T",
                timezone="America/New_York",
                status=EnrollmentStatus.UNSUBSCRIBED,
                current_step=0,
            )
        )
        s.add(
            SequenceEnrollmentStep(
                id="est-term",
                enrollment_id="enr-term",
                step_id="step-1",
                mailbox_id=mb,
                status=EnrollmentStepStatus.SCHEDULED,
                scheduled_at=PAST(),
                custom_subject="x",
                custom_body="<p>y</p>",
            )
        )
        s.add(
            SequenceEnrollment(
                id="enr-stranded",
                sequence_id=seeded["sequence_id"],
                mailbox_id=mb,
                contact_email="stranded@acme.com",
                contact_name="S",
                timezone="America/New_York",
                status=EnrollmentStatus.ACTIVE,
                current_step=0,
                created_at=old,
            )
        )
        s.add(
            SequenceEnrollmentStep(
                id="est-stranded",
                enrollment_id="enr-stranded",
                step_id="step-1",
                mailbox_id=mb,
                status=EnrollmentStepStatus.PENDING,
                scheduled_at=None,
                sent_at=None,
                custom_subject="x",
                custom_body="<p>y</p>",
            )
        )
        s.add(
            SequenceEnrollment(
                id="enr-paused",
                sequence_id=seeded["sequence_id"],
                mailbox_id=mb,
                contact_email="paused@acme.com",
                contact_name="P",
                timezone="America/New_York",
                status=EnrollmentStatus.PAUSED,
                current_step=0,
                created_at=old,
                pause_reason="manual",
            )
        )
        s.add(
            SequenceEnrollmentStep(
                id="est-paused",
                enrollment_id="enr-paused",
                step_id="step-1",
                mailbox_id=mb,
                status=EnrollmentStepStatus.PENDING,
                scheduled_at=None,
                custom_subject="x",
                custom_body="<p>y</p>",
            )
        )
        s.add(
            SequenceEnrollment(
                id="enr-recent",
                sequence_id=seeded["sequence_id"],
                mailbox_id=mb,
                contact_email="recent@acme.com",
                contact_name="R",
                timezone="America/New_York",
                status=EnrollmentStatus.ACTIVE,
                current_step=0,
                created_at=datetime.utcnow() - timedelta(days=2),
            )
        )
        s.add(
            SequenceEnrollmentStep(
                id="est-recent",
                enrollment_id="enr-recent",
                step_id="step-1",
                mailbox_id=mb,
                status=EnrollmentStepStatus.PENDING,
                scheduled_at=None,
                sent_at=None,
                custom_subject="x",
                custom_body="<p>y</p>",
            )
        )
        await s.commit()


async def _step_status(session_factory, est_id):
    async with session_factory() as s:
        st = await s.get(SequenceEnrollmentStep, est_id)
        return st.status


async def _enr_status(session_factory, enr_id):
    async with session_factory() as s:
        en = await s.get(SequenceEnrollment, enr_id)
        return en.status


@pytest.mark.asyncio
async def test_remediation_script_dry_run_mutates_nothing(
    seeded, session_factory, tmp_path
):
    from scripts.remediate_reconciler_starvation_REVOPS_1668 import run

    await _seed_script_scenario(session_factory, seeded)
    audit = tmp_path / "audit_dry.json"
    out = await run(
        apply=False,
        stranded_days=14,
        session_factory=session_factory,
        audit_out=str(audit),
    )
    assert out["phase1_enrollments"] == 1
    assert out["phase2_enrollments"] == 1
    assert out["phase1_steps"] == 0
    assert out["phase2_steps"] == 0
    assert (
        await _step_status(session_factory, "est-term")
        == EnrollmentStepStatus.SCHEDULED
    )
    assert (
        await _step_status(session_factory, "est-stranded")
        == EnrollmentStepStatus.PENDING
    )
    assert (
        await _step_status(session_factory, "est-paused")
        == EnrollmentStepStatus.PENDING
    )
    assert await _enr_status(session_factory, "enr-stranded") == EnrollmentStatus.ACTIVE
    assert (
        await _enr_status(session_factory, "enr-term") == EnrollmentStatus.UNSUBSCRIBED
    )
    assert await _enr_status(session_factory, "enr-paused") == EnrollmentStatus.PAUSED


@pytest.mark.asyncio
async def test_remediation_script_apply_both_phases(seeded, session_factory, tmp_path):
    from scripts.remediate_reconciler_starvation_REVOPS_1668 import run

    await _seed_script_scenario(session_factory, seeded)
    audit = tmp_path / "audit_apply.json"
    out = await run(
        apply=True,
        stranded_days=14,
        session_factory=session_factory,
        audit_out=str(audit),
    )
    assert out["phase1_enrollments"] == 1
    assert out["phase1_steps"] == 1
    assert out["phase2_enrollments"] == 1
    assert out["phase2_steps"] == 1
    assert (
        await _step_status(session_factory, "est-term") == EnrollmentStepStatus.SKIPPED
    )
    assert (
        await _step_status(session_factory, "est-stranded")
        == EnrollmentStepStatus.SKIPPED
    )
    assert (
        await _enr_status(session_factory, "enr-stranded") == EnrollmentStatus.COMPLETED
    )
    # REVOPS-1668 D3: pause_reason must stay NULL (CHECK-constrained to NULL or
    # the existing reason set — no new marker value, no migration).
    async with session_factory() as s:
        en = await s.get(SequenceEnrollment, "enr-stranded")
        assert en.pause_reason is None
    # PAUSED enrollment and its step are untouched.
    assert (
        await _step_status(session_factory, "est-paused")
        == EnrollmentStepStatus.PENDING
    )
    assert await _enr_status(session_factory, "enr-paused") == EnrollmentStatus.PAUSED
    # Recent ACTIVE enrollment (inside threshold) is untouched.
    assert await _enr_status(session_factory, "enr-recent") == EnrollmentStatus.ACTIVE
    assert (
        await _step_status(session_factory, "est-recent")
        == EnrollmentStepStatus.PENDING
    )


@pytest.mark.asyncio
async def test_remediation_script_idempotent(seeded, session_factory, tmp_path):
    from scripts.remediate_reconciler_starvation_REVOPS_1668 import run

    audit = tmp_path / "audit_idem.json"
    await _seed_script_scenario(session_factory, seeded)
    await run(
        apply=True,
        stranded_days=14,
        session_factory=session_factory,
        audit_out=str(audit),
    )
    out = await run(
        apply=True,
        stranded_days=14,
        session_factory=session_factory,
        audit_out=str(audit),
    )
    assert out["phase1_enrollments"] == 0
    assert out["phase1_steps"] == 0
    assert out["phase2_enrollments"] == 0
    assert out["phase2_steps"] == 0


@pytest.mark.asyncio
async def test_remediation_script_audit_json_and_table_stdout(
    seeded, session_factory, tmp_path, capsys
):
    """D3: dry-run writes audit JSON with expected enrollment ids and the
    plain-text table lines appear in captured stdout."""
    import json
    from scripts.remediate_reconciler_starvation_REVOPS_1668 import run

    await _seed_script_scenario(session_factory, seeded)
    audit = tmp_path / "audit_test.json"
    await run(
        apply=False,
        stranded_days=14,
        session_factory=session_factory,
        audit_out=str(audit),
    )
    captured = capsys.readouterr()
    data = json.loads(audit.read_text())
    assert set(data.keys()) == {"phase1", "phase1_counts", "phase2"}
    p1_ids = {r["enrollment_id"] for r in data["phase1"]}
    p2_ids = {r["enrollment_id"] for r in data["phase2"]}
    assert p1_ids == {"enr-term"}
    assert p2_ids == {"enr-stranded"}
    assert "enr-term" in captured.out
    assert "enr-stranded" in captured.out
    assert "Phase 1" in captured.out
    assert "Phase 2" in captured.out
    assert "per-status step counts" in captured.out


# ── D1/D3 adversarial: last_sent must aggregate SENT steps only ──────────
#
# Reviewer FAIL D1/D3: last_sent_subq aggregated max(sent_at) across every
# step status. An old actual SENT step plus a recent PENDING row carrying a
# sent_at value incorrectly suppressed the stranded counter (reconcile.py)
# and phase-2 remediation (script). The contract: "most recent SENT step
# sent_at" means filter on EnrollmentStepStatus.SENT — a PENDING row's
# sent_at column is not a SENT event.


@pytest.mark.asyncio
async def test_stranded_counter_ignores_pending_sent_at(
    seeded, session_factory, monkeypatch
):
    """An old SENT step + a recent PENDING row with sent_at set must still
    count the enrollment as stranded — the recent PENDING sent_at must NOT
    mask the old real SENT."""
    monkeypatch.setattr(rec.settings, "reconcile_grace_seconds", 600, raising=False)
    monkeypatch.setattr(rec.settings, "stranded_enrollment_days", 14, raising=False)

    old = datetime.utcnow() - timedelta(days=30)
    recent = datetime.utcnow() - timedelta(days=2)

    # ACTIVE enrollment with an old real SENT step and a recent PENDING step
    # that carries a sent_at value (stale column on a non-SENT row).
    enr = await _make_enrollment(
        session_factory,
        seeded,
        enr_id="enr-sent-contract",
        status=EnrollmentStatus.ACTIVE,
        created_at=old,
    )
    # Old real SENT step — this is the actual last-sent event.
    await _make_step(
        session_factory,
        enrollment_id=enr,
        status=EnrollmentStepStatus.SENT,
        scheduled_at=None,
        sent_at=old,
        est_id="est-sent-old",
    )
    # Recent PENDING step with a stale sent_at — must NOT be treated as a SENT.
    await _make_step(
        session_factory,
        enrollment_id=enr,
        status=EnrollmentStepStatus.PENDING,
        scheduled_at=None,
        sent_at=recent,
        est_id="est-pending-recent",
    )

    q = AsyncMock(return_value="job-1")
    cms = _patch(session_factory, q)
    _enter(cms)
    try:
        out = await rec.reconcile_scheduled_steps({})
    finally:
        _exit(cms)
    # No SCHEDULED steps → reconciled == 0. The enrollment IS stranded: the
    # only real SENT is 30d old, exceeding the 14d threshold. The recent
    # PENDING row's sent_at must not suppress the counter.
    assert out["reconciled"] == 0
    assert out["stranded_active_enrollments"] == 1, (
        "stranded counter must aggregate SENT steps only; a recent PENDING "
        "row carrying sent_at must not mask an old real SENT step"
    )


@pytest.mark.asyncio
async def test_remediation_phase2_ignores_pending_sent_at(
    seeded, session_factory, tmp_path
):
    """Phase-2 remediation must select the same stranded enrollment: old SENT
    + recent PENDING-with-sent_at → stranded (selected for phase 2)."""
    from scripts.remediate_reconciler_starvation_REVOPS_1668 import run

    old = datetime.utcnow() - timedelta(days=30)
    recent = datetime.utcnow() - timedelta(days=2)
    mb = seeded["active_mailbox_id"]

    async with session_factory() as s:
        s.add(
            SequenceEnrollment(
                id="enr-sent-contract",
                sequence_id=seeded["sequence_id"],
                mailbox_id=mb,
                contact_email="sc@acme.com",
                contact_name="SC",
                timezone="America/New_York",
                status=EnrollmentStatus.ACTIVE,
                current_step=0,
                created_at=old,
            )
        )
        s.add(
            SequenceEnrollmentStep(
                id="est-sent-old",
                enrollment_id="enr-sent-contract",
                step_id="step-1",
                mailbox_id=mb,
                status=EnrollmentStepStatus.SENT,
                scheduled_at=None,
                sent_at=old,
                custom_subject="x",
                custom_body="<p>y</p>",
            )
        )
        s.add(
            SequenceEnrollmentStep(
                id="est-pending-recent",
                enrollment_id="enr-sent-contract",
                step_id="step-2",
                mailbox_id=mb,
                status=EnrollmentStepStatus.PENDING,
                scheduled_at=None,
                sent_at=recent,
                custom_subject="x",
                custom_body="<p>y</p>",
            )
        )
        await s.commit()

    audit = tmp_path / "audit_sent_contract.json"
    out = await run(
        apply=False,
        stranded_days=14,
        session_factory=session_factory,
        audit_out=str(audit),
    )
    # The enrollment must be selected by phase 2 despite the recent PENDING
    # row's sent_at — the only real SENT is 30d old.
    assert out["phase2_enrollments"] == 1, (
        "phase-2 remediation must aggregate SENT steps only; a recent "
        "PENDING row carrying sent_at must not mask an old real SENT step"
    )
