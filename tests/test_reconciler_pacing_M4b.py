"""Per-mailbox capacity pacing for the stuck-step reconciler (REVOPS-1378 / 2026-07-20 incident).

The M4 reconciler re-enqueued up to reconcile_batch_limit(200) past-due SCHEDULED
steps every 10 min with delay_seconds=None — unpaced. Live 2026-07-20 this burned
AMER's entire 300/day budget in one hour (251 sends in the 13:00 UTC hour; all 8
mailboxes at sent_today=daily_send_limit=75 by 09:30 ET). These tests pin the
fix: per-mailbox capacity-aware limiting that mirrors the existing
circuit_resume.py:124-138 precedent, with an added reserve floor that the
reconciler may never consume so catch-up trickles in behind in-flight work.

Critical grouping invariant verified live: sequence_enrollment_steps.mailbox_id
is NULL on all SCHEDULED rows (assigned at send time); sequence_enrollments.
mailbox_id is 100% populated. The pacer MUST group by enrollment.mailbox_id,
never step.mailbox_id (grouping by the step column yields one NULL bucket = no
pacing at all).
"""

from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest

import src.workers.reconcile as rec
from src.models.models import (
    Mailbox,
    MailboxStatus,
    SequenceEnrollment,
    SequenceEnrollmentStep,
    EnrollmentStatus,
    EnrollmentStepStatus,
)

# Live AMER mailbox shape: daily_send_limit=75 across all mailboxes. The spec
# math (ceil(75 * 0.30) = 23) is calibrated to this number, so tests that assert
# the reserve floor use 75 and not the seeded fixture's 50.
AMER_DAILY_LIMIT = 75
RESERVE_FRACTION = 0.30
PER_RUN = 10


async def _add_mailbox(
    session_factory,
    *,
    mailbox_id,
    sent_today,
    daily_send_limit=AMER_DAILY_LIMIT,
    status=MailboxStatus.ACTIVE,
):
    async with session_factory() as s:
        s.add(
            Mailbox(
                id=mailbox_id,
                tenant_id="tenant-scout",
                email=f"{mailbox_id}@telnyx.com",
                status=status,
                weight=1,
                daily_send_limit=daily_send_limit,
                sent_today=sent_today,
            )
        )
        await s.commit()


async def _make_overdue_step(
    session_factory,
    seeded,
    *,
    est_id,
    enr_id,
    mailbox_id,
    step_mailbox_id=None,
    scheduled_at=None,
    step_id="step-1",
):
    """Create an enrollment + overdue SCHEDULED step.

    By default step.mailbox_id is None to mirror the live invariant (NULL on all
    SCHEDULED rows, assigned at send time). The pacer must group by
    enrollment.mailbox_id, which is always populated.
    """
    if scheduled_at is None:
        scheduled_at = datetime.utcnow() - timedelta(hours=2)
    async with session_factory() as s:
        s.add(
            SequenceEnrollment(
                id=enr_id,
                sequence_id=seeded["sequence_id"],
                mailbox_id=mailbox_id,
                contact_email=f"vp+{enr_id}@acme.com",
                contact_name="VP",
                timezone="America/New_York",
                status=EnrollmentStatus.ACTIVE,
                current_step=1,
            )
        )
        s.add(
            SequenceEnrollmentStep(
                id=est_id,
                enrollment_id=enr_id,
                step_id=step_id,
                mailbox_id=step_mailbox_id,
                status=EnrollmentStepStatus.SCHEDULED,
                scheduled_at=scheduled_at,
                custom_subject="Hi",
                custom_body="<p>B</p>",
            )
        )
        await s.commit()
    return est_id


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


def _config(
    monkeypatch,
    *,
    per_run=PER_RUN,
    reserve=RESERVE_FRACTION,
    batch_limit=200,
    grace=600,
):
    monkeypatch.setattr(
        rec.settings, "reconcile_per_mailbox_per_run", per_run, raising=False
    )
    monkeypatch.setattr(
        rec.settings, "reconcile_new_send_reserve_fraction", reserve, raising=False
    )
    monkeypatch.setattr(
        rec.settings, "reconcile_batch_limit", batch_limit, raising=False
    )
    monkeypatch.setattr(rec.settings, "reconcile_grace_seconds", grace, raising=False)


# ── Per-run cap binds ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_per_run_cap_binds(seeded, session_factory, monkeypatch):
    """sent_today=0, daily_send_limit=75, reserve=0.30, per_run=10 → exactly 10."""
    _config(monkeypatch)
    await _add_mailbox(session_factory, mailbox_id="mb-fresh", sent_today=0)
    for i in range(30):
        await _make_overdue_step(
            session_factory,
            seeded,
            est_id=f"e{i}",
            enr_id=f"enr{i}",
            mailbox_id="mb-fresh",
        )
    q = AsyncMock(return_value="j")
    cms = _patch(session_factory, q)
    _enter(cms)
    try:
        out = await rec.reconcile_scheduled_steps({})
    finally:
        _exit(cms)
    assert out["reconciled"] == 10, (
        f"per-run cap should bind at 10 (sent_today=0, spare 75, floor 23, "
        f"usable 52, min(10, 52)=10); got {out['reconciled']}"
    )
    assert q.await_count == 10


# ── Reserve floor ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_reserve_floor_blocks_reconcile(seeded, session_factory, monkeypatch):
    """sent_today=60 → spare 15, floor 23, usable 0 → 0 reconciled, skipped_reserve_floor."""
    _config(monkeypatch)
    # spare = 75 - 60 = 15; floor = ceil(75 * 0.30) = 23; usable = max(0, 15-23) = 0
    await _add_mailbox(session_factory, mailbox_id="mb-floor", sent_today=60)
    for i in range(5):
        await _make_overdue_step(
            session_factory,
            seeded,
            est_id=f"e{i}",
            enr_id=f"enr{i}",
            mailbox_id="mb-floor",
        )
    q = AsyncMock(return_value="j")
    cms = _patch(session_factory, q)
    _enter(cms)
    try:
        out = await rec.reconcile_scheduled_steps({})
    finally:
        _exit(cms)
    assert out["reconciled"] == 0, (
        f"reserve floor should block: spare 15 < floor 23 → usable 0; "
        f"got {out['reconciled']}"
    )
    assert out.get("skipped_reserve_floor", 0) >= 1, (
        "skipped_reserve_floor must be incremented when spare>0 but usable=0"
    )
    q.assert_not_awaited()


# ── Exhausted ───────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_exhausted_mailbox_skipped_at_capacity(
    seeded, session_factory, monkeypatch
):
    """sent_today=75 → 0 reconciled, skipped_at_capacity incremented (not reserve floor)."""
    _config(monkeypatch)
    await _add_mailbox(session_factory, mailbox_id="mb-exh", sent_today=75)
    for i in range(5):
        await _make_overdue_step(
            session_factory,
            seeded,
            est_id=f"e{i}",
            enr_id=f"enr{i}",
            mailbox_id="mb-exh",
        )
    q = AsyncMock(return_value="j")
    cms = _patch(session_factory, q)
    _enter(cms)
    try:
        out = await rec.reconcile_scheduled_steps({})
    finally:
        _exit(cms)
    assert out["reconciled"] == 0
    assert out.get("skipped_at_capacity", 0) >= 1, (
        "skipped_at_capacity must be incremented when spare=0 (mailbox exhausted), "
        "distinct from skipped_reserve_floor"
    )
    q.assert_not_awaited()


# ── Per-mailbox isolation ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_per_mailbox_isolation(seeded, session_factory, monkeypatch):
    """400 past-due steps across 4 mailboxes → ≤10 per mailbox per sweep (≤40 total),
    NOT 200 from whichever mailbox sorts first."""
    _config(monkeypatch)
    # 4 fresh mailboxes, each with sent_today=0 (spare 75, floor 23, usable 52, cap 10)
    mailbox_ids = ["mb-iso-a", "mb-iso-b", "mb-iso-c", "mb-iso-d"]
    for mid in mailbox_ids:
        await _add_mailbox(session_factory, mailbox_id=mid, sent_today=0)
    # 100 steps per mailbox = 400 total, well above batch_limit(200).
    # Interleave scheduled_at across mailboxes so the batch_limit(200) fetch
    # (ordered scheduled_at ASC) sees a representative sample of all 4 —
    # mirrors the live shape where steps become overdue at different times.
    base = datetime.utcnow() - timedelta(hours=10)
    global_idx = 0
    for i in range(100):
        for mi, mid in enumerate(mailbox_ids):
            await _make_overdue_step(
                session_factory,
                seeded,
                est_id=f"e{mi}_{i}",
                enr_id=f"enr{mi}_{i}",
                mailbox_id=mid,
                scheduled_at=base + timedelta(seconds=global_idx),
            )
            global_idx += 1
    q = AsyncMock(return_value="j")
    cms = _patch(session_factory, q)
    _enter(cms)
    try:
        out = await rec.reconcile_scheduled_steps({})
    finally:
        _exit(cms)
    assert out["reconciled"] <= 40, (
        f"4 mailboxes × per_run(10) = 40 max; got {out['reconciled']}. "
        f"If this is 200, per-mailbox isolation is broken (single bucket)."
    )
    assert out["reconciled"] <= 10 * len(mailbox_ids)


# ── NULL step.mailbox_id regression guard ────────────────────────────────────


@pytest.mark.asyncio
async def test_null_step_mailbox_id_groups_by_enrollment(
    seeded, session_factory, monkeypatch
):
    """All steps have mailbox_id=NULL but enrollment.mailbox_id is set → still paced
    correctly. This is the regression guard for the grouping bug: grouping by
    step.mailbox_id yields one NULL bucket = no pacing."""
    _config(monkeypatch)
    await _add_mailbox(session_factory, mailbox_id="mb-grp", sent_today=0)
    # 30 steps, ALL with step.mailbox_id=None, enrollment.mailbox_id="mb-grp"
    for i in range(30):
        await _make_overdue_step(
            session_factory,
            seeded,
            est_id=f"e{i}",
            enr_id=f"enr{i}",
            mailbox_id="mb-grp",
            step_mailbox_id=None,
        )
    q = AsyncMock(return_value="j")
    cms = _patch(session_factory, q)
    _enter(cms)
    try:
        out = await rec.reconcile_scheduled_steps({})
    finally:
        _exit(cms)
    assert out["reconciled"] == 10, (
        f"Must pace by enrollment.mailbox_id (not NULL step.mailbox_id); "
        f"expected 10, got {out['reconciled']}. If 30, grouping bug regressed."
    )
    assert q.await_count == 10


# ── FIFO ordering ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_fifo_oldest_reconciled_first(seeded, session_factory, monkeypatch):
    """Oldest scheduled_at reconciled first when the per-mailbox cap binds."""
    _config(monkeypatch)
    await _add_mailbox(session_factory, mailbox_id="mb-fifo", sent_today=0)
    # Create steps with distinct, ascending scheduled_at; per-run cap=10 < 15.
    base = datetime.utcnow() - timedelta(hours=10)
    for i in range(15):
        await _make_overdue_step(
            session_factory,
            seeded,
            est_id=f"e{i:02d}",
            enr_id=f"enr{i:02d}",
            mailbox_id="mb-fifo",
            scheduled_at=base + timedelta(minutes=i),
        )  # e00 oldest, e14 newest
    q = AsyncMock(return_value="j")
    cms = _patch(session_factory, q)
    _enter(cms)
    try:
        out = await rec.reconcile_scheduled_steps({})
    finally:
        _exit(cms)
    assert out["reconciled"] == 10
    # The 10 enqueued must be the oldest 10 (e00..e09), NOT e05..e14.
    enqueued_ids = {call.kwargs["enrollment_step_id"] for call in q.await_args_list}
    expected_oldest = {f"e{i:02d}" for i in range(10)}
    assert enqueued_ids == expected_oldest, (
        f"FIFO violated: expected oldest 10 {expected_oldest}, got {enqueued_ids}"
    )


# ── Burst regression (the incident) ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_burst_regression_399_across_4_mailboxes(
    seeded, session_factory, monkeypatch
):
    """The 2026-07-20 incident: 399 past-due steps across 4 AMER mailboxes in one
    sweep → total re-enqueued ≤40, asserting the 251-in-an-hour scenario cannot
    recur. Live evidence: 399 distinct steps across two sweeps → 251 sends/hr."""
    _config(monkeypatch)
    # 4 AMER mailboxes, all fresh (sent_today=0) to maximize what could be sent.
    # Pacing: 4 × min(10, 75-0-23) = 4 × 10 = 40 max per sweep.
    amer_mailboxes = ["mb-amer-c", "mb-amer-d", "mb-amer-h", "mb-amer-j"]
    for mid in amer_mailboxes:
        await _add_mailbox(session_factory, mailbox_id=mid, sent_today=0)
    # 399 steps distributed across the 4 mailboxes (~100 each, 3 short).
    # Interleave scheduled_at across mailboxes so the batch_limit(200) fetch
    # (ordered scheduled_at ASC) sees a representative sample of all 4 — mirrors
    # the live shape where steps become overdue at different times as their arq
    # jobs fail independently, not all at the same instant.
    base = datetime.utcnow() - timedelta(hours=10)
    global_idx = 0
    per_mailbox_counts = [100, 100, 100, 99]  # 399 total
    for i in range(max(per_mailbox_counts)):
        for mi, mid in enumerate(amer_mailboxes):
            if i < per_mailbox_counts[mi]:
                await _make_overdue_step(
                    session_factory,
                    seeded,
                    est_id=f"e{mi}_{i:03d}",
                    enr_id=f"enr{mi}_{i:03d}",
                    mailbox_id=mid,
                    scheduled_at=base + timedelta(seconds=global_idx),
                )
                global_idx += 1
    assert global_idx == 399
    q = AsyncMock(return_value="j")
    cms = _patch(session_factory, q)
    _enter(cms)
    try:
        out = await rec.reconcile_scheduled_steps({})
    finally:
        _exit(cms)
    # 4 mailboxes × per_run(10) = 40. The batch_limit(200) does NOT bind.
    assert out["reconciled"] <= 40, (
        f"Burst regression: expected ≤40 (4 × 10), got {out['reconciled']}. "
        f"The 251-in-an-hour incident could recur if this exceeds 40."
    )
    assert out["reconciled"] == 40, (
        f"All 4 mailboxes have usable 52 (> per_run 10) → should hit 4×10=40; "
        f"got {out['reconciled']}"
    )
    # Per-mailbox distribution: exactly 10 each (not 13/13/13/1 from naive batching)
    per_mailbox = {mid: 0 for mid in amer_mailboxes}
    for call in q.await_args_list:
        # We can't see which mailbox from the queue call directly; verify via
        # the step's enrollment. Instead, check the observability field.
        pass
    # The out["per_mailbox"] observability must show 10 for each mailbox.
    pm = out.get("per_mailbox", {})
    for mid in amer_mailboxes:
        assert pm.get(mid, {}).get("reconciled", 0) == 10, (
            f"per_mailbox[{mid}].reconciled must be 10, got {pm.get(mid)}"
        )


# ── Observability: past_due_backlog_depth ────────────────────────────────────


@pytest.mark.asyncio
async def test_past_due_backlog_depth_reported(seeded, session_factory, monkeypatch):
    """The completion log must include past_due_backlog_depth (total past-due
    SCHEDULED, not just this batch) so drain health is visible day over day."""
    _config(monkeypatch)
    await _add_mailbox(session_factory, mailbox_id="mb-obs", sent_today=0)
    # 30 steps but batch_limit will return ≤10 — backlog must be counted separately
    for i in range(30):
        await _make_overdue_step(
            session_factory,
            seeded,
            est_id=f"e{i}",
            enr_id=f"enr{i}",
            mailbox_id="mb-obs",
        )
    q = AsyncMock(return_value="j")
    cms = _patch(session_factory, q)
    _enter(cms)
    try:
        out = await rec.reconcile_scheduled_steps({})
    finally:
        _exit(cms)
    assert out["reconciled"] == 10
    # past_due_backlog_depth must reflect ALL 30 past-due, not just the 10 reconciled
    assert out.get("past_due_backlog_depth") == 30, (
        f"backlog depth must count ALL past-due SCHEDULED (30), not just this "
        f"batch (10); got {out.get('past_due_backlog_depth')}"
    )


# ── Existing behavior preserved ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_null_scheduled_at_still_skipped(seeded, session_factory, monkeypatch):
    """NULL scheduled_at must NOT be reconciled (existing behavior preserved)."""
    _config(monkeypatch)
    await _add_mailbox(session_factory, mailbox_id="mb-null", sent_today=0)
    async with session_factory() as s:
        s.add(
            SequenceEnrollment(
                id="enr-null",
                sequence_id=seeded["sequence_id"],
                mailbox_id="mb-null",
                contact_email="vp+null@acme.com",
                contact_name="VP",
                timezone="America/New_York",
                status=EnrollmentStatus.ACTIVE,
                current_step=1,
            )
        )
        s.add(
            SequenceEnrollmentStep(
                id="e-null",
                enrollment_id="enr-null",
                step_id="step-1",
                mailbox_id=None,
                status=EnrollmentStepStatus.SCHEDULED,
                scheduled_at=None,
                custom_subject="Hi",
                custom_body="<p>B</p>",
            )
        )
        await s.commit()
    q = AsyncMock(return_value="j")
    cms = _patch(session_factory, q)
    _enter(cms)
    try:
        out = await rec.reconcile_scheduled_steps({})
    finally:
        _exit(cms)
    assert out["reconciled"] == 0
    q.assert_not_awaited()
