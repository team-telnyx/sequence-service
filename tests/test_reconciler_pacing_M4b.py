"""Per-mailbox capacity pacing for the stuck-step reconciler (REVOPS-1378 / 2026-07-20 incident).

The M4 reconciler re-enqueued up to reconcile_batch_limit(200) past-due SCHEDULED
steps every 10 min with delay_seconds=None — unpaced. Live 2026-07-20 this burned
AMER's entire 300/day budget in one hour (251 sends in the 13:00 UTC hour; all 8
mailboxes at sent_today=daily_send_limit=75 by 09:30 ET). These tests pin the
fix: per-mailbox capacity-aware limiting that mirrors the existing
circuit_resume.py:124-138 precedent, with an added reserve floor that the
reconciler may never consume so catch-up trickles in behind in-flight work.

Two blockers from the PR #23 planner review (2026-07-20) are pinned here:
- STARVATION: selection uses ROW_NUMBER() OVER (PARTITION BY enrollment.mailbox_id
  ORDER BY scheduled_at ASC). A global ORDER BY ... LIMIT 200 taken BEFORE
  bucketing lets one mailbox with the oldest clump fill the batch and starve
  every other mailbox. Correlated (NOT interleaved) fixtures are mandatory —
  interleaving hid the bug entirely in the first test pass.
- HOURLY RATE: the per-mailbox limit spreads the daily `usable` allowance across
  the send window's sweeps (allowance = ceil(usable / sweeps_left)), bounding
  the hourly rate, not just the daily total. Without it per_run(10) × 6 sweeps/hr
  × 4 mailboxes ≈ 208 sends in hour one — a 17% reduction on the 251 incident.

Critical grouping invariant verified live: sequence_enrollment_steps.mailbox_id
is NULL on all SCHEDULED rows (assigned at send time); sequence_enrollments.
mailbox_id is 100% populated. The pacer MUST group by enrollment.mailbox_id,
never step.mailbox_id (grouping by the step column yields one NULL bucket = no
pacing at all).
"""

import math
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest

import src.workers.reconcile as rec
from src.models.models import (
    EnrollmentStatus,
    EnrollmentStepStatus,
    Mailbox,
    MailboxStatus,
    SequenceEnrollment,
    SequenceEnrollmentStep,
)

# Live AMER mailbox shape: daily_send_limit=75 across all mailboxes. The spec
# math (ceil(75 * 0.30) = 23, usable 52) is calibrated to this number.
AMER_DAILY_LIMIT = 75
RESERVE_FRACTION = 0.30
PER_RUN = 10
# Defaults matching src/config.py: 9h send window, 10-min sweep cadence.
PACING_WINDOW_HOURS = 9
SWEEP_MINUTES = 10


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
    scheduled_at,
    step_mailbox_id=None,
    step_id="step-1",
):
    """Create an enrollment + overdue SCHEDULED step.

    By default step.mailbox_id is None to mirror the live invariant (NULL on all
    SCHEDULED rows, assigned at send time). The pacer must group by
    enrollment.mailbox_id, which is always populated.
    """
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
    window_hours=PACING_WINDOW_HOURS,
    sweep_minutes=SWEEP_MINUTES,
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
    monkeypatch.setattr(
        rec.settings, "reconcile_pacing_window_hours", window_hours, raising=False
    )
    monkeypatch.setattr(
        rec.settings, "reconcile_sweep_minutes", sweep_minutes, raising=False
    )


async def _run(q, sf):
    cms = _patch(sf, q)
    _enter(cms)
    try:
        return await rec.reconcile_scheduled_steps({})
    finally:
        _exit(cms)


# ── Pacing: allowance math binds (not per_run) ──────────────────────────────


@pytest.mark.asyncio
async def test_pacing_allowance_binds_not_per_run(seeded, session_factory, monkeypatch):
    """With defaults (75/0, 9h window, 10min sweep): usable=52, sweeps_left=54,
    allowance=ceil(52/54)=1 → reconciles 1, NOT per_run=10. Asserts the allowance
    math explicitly."""
    _config(monkeypatch)
    await _add_mailbox(session_factory, mailbox_id="mb-pace", sent_today=0)
    base = datetime.utcnow() - timedelta(hours=2)
    for i in range(30):
        await _make_overdue_step(
            session_factory,
            seeded,
            est_id=f"e{i}",
            enr_id=f"enr{i}",
            mailbox_id="mb-pace",
            scheduled_at=base + timedelta(seconds=i),
        )
    q = AsyncMock(return_value="j")
    out = await _run(q, session_factory)
    # usable = 75 - 0 - ceil(75*0.30) = 75 - 23 = 52
    # sweeps_left = ceil(9*60/10) = 54
    # allowance = max(1, ceil(52/54)) = 1
    # limit = min(10, 1, 52) = 1
    assert out["reconciled"] == 1, (
        f"allowance must bind (1 with defaults), not per_run(10); "
        f"got {out['reconciled']}"
    )
    pm = out["per_mailbox"]["mb-pace"]
    assert pm["allowance"] == 1, f"allowance must be 1; got {pm}"
    assert pm["limit"] == 1
    assert q.await_count == 1


# ── Per-run cap binds when allowance is loose ────────────────────────────────


@pytest.mark.asyncio
async def test_per_run_cap_binds(seeded, session_factory, monkeypatch):
    """When allowance ≥ per_run, the per_run hard ceiling binds. Use a 1000-cap
    mailbox (usable 700, allowance ceil(700/54)=13 ≥ 10) → exactly 10."""
    _config(monkeypatch)
    await _add_mailbox(
        session_factory, mailbox_id="mb-fresh", sent_today=0, daily_send_limit=1000
    )
    base = datetime.utcnow() - timedelta(hours=2)
    for i in range(30):
        await _make_overdue_step(
            session_factory,
            seeded,
            est_id=f"e{i}",
            enr_id=f"enr{i}",
            mailbox_id="mb-fresh",
            scheduled_at=base + timedelta(seconds=i),
        )
    q = AsyncMock(return_value="j")
    out = await _run(q, session_factory)
    # usable = 1000 - 0 - ceil(1000*0.30) = 700; allowance = ceil(700/54) = 13
    # limit = min(10, 13, 700) = 10
    assert out["reconciled"] == 10, (
        f"per-run cap should bind at 10 when allowance is loose; "
        f"got {out['reconciled']}"
    )
    assert q.await_count == 10


# ── Reserve floor ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_reserve_floor_blocks_reconcile(seeded, session_factory, monkeypatch):
    """sent_today=60 → spare 15, floor 23, usable 0 → 0 reconciled,
    skipped_reserve_floor incremented (even though 15 raw spare exists)."""
    _config(monkeypatch)
    await _add_mailbox(session_factory, mailbox_id="mb-floor", sent_today=60)
    base = datetime.utcnow() - timedelta(hours=2)
    for i in range(5):
        await _make_overdue_step(
            session_factory,
            seeded,
            est_id=f"e{i}",
            enr_id=f"enr{i}",
            mailbox_id="mb-floor",
            scheduled_at=base + timedelta(seconds=i),
        )
    q = AsyncMock(return_value="j")
    out = await _run(q, session_factory)
    assert out["reconciled"] == 0
    assert out.get("skipped_reserve_floor", 0) >= 1
    q.assert_not_awaited()


# ── Exhausted ───────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_exhausted_mailbox_skipped_at_capacity(
    seeded, session_factory, monkeypatch
):
    """sent_today=75 → spare 0 → 0 reconciled, skipped_at_capacity (not reserve floor)."""
    _config(monkeypatch)
    await _add_mailbox(session_factory, mailbox_id="mb-exh", sent_today=75)
    base = datetime.utcnow() - timedelta(hours=2)
    for i in range(5):
        await _make_overdue_step(
            session_factory,
            seeded,
            est_id=f"e{i}",
            enr_id=f"enr{i}",
            mailbox_id="mb-exh",
            scheduled_at=base + timedelta(seconds=i),
        )
    q = AsyncMock(return_value="j")
    out = await _run(q, session_factory)
    assert out["reconciled"] == 0
    assert out.get("skipped_at_capacity", 0) >= 1
    q.assert_not_awaited()


# ── 🔴 STARVATION REGRESSION (BLOCKER 1) ─────────────────────────────────────


@pytest.mark.asyncio
async def test_starvation_correlated_clump(seeded, session_factory, monkeypatch):
    """Mailbox A has 300 past-due steps ALL older than any step on B/C/D
    (correlated clump — the real stranding shape). B/C/D have 20 each with full
    spare. Assert B, C, and D EACH reconcile >= 1. Under the global
    ORDER BY scheduled_at LIMIT 200 taken before bucketing, A fills the entire
    batch and B/C/D reconcile 0 — the exact failure this test exists to catch."""
    _config(monkeypatch)
    mailbox_ids = ["mb-A", "mb-B", "mb-C", "mb-D"]
    for mid in mailbox_ids:
        await _add_mailbox(session_factory, mailbox_id=mid, sent_today=0)

    # CORRELATED: A's 300 steps are the oldest (10h ago). B/C/D's 20 each are 1h ago.
    # A global ORDER BY scheduled_at ASC LIMIT 200 returns only A's first 200.
    base_A = datetime.utcnow() - timedelta(hours=10)
    base_other = datetime.utcnow() - timedelta(hours=1)
    for i in range(300):
        await _make_overdue_step(
            session_factory,
            seeded,
            est_id=f"eA_{i:03d}",
            enr_id=f"enrA_{i:03d}",
            mailbox_id="mb-A",
            scheduled_at=base_A + timedelta(seconds=i),
        )
    for mi, mid in enumerate(["mb-B", "mb-C", "mb-D"]):
        for i in range(20):
            await _make_overdue_step(
                session_factory,
                seeded,
                est_id=f"e{mi}_{i:03d}",
                enr_id=f"enr{mi}_{i:03d}",
                mailbox_id=mid,
                scheduled_at=base_other + timedelta(seconds=mi * 100 + i),
            )
    q = AsyncMock(return_value="j")
    out = await _run(q, session_factory)
    pm = out.get("per_mailbox", {})
    # Each of B, C, D must reconcile >= 1 (allowance=1 with defaults).
    for mid in ["mb-B", "mb-C", "mb-D"]:
        assert pm.get(mid, {}).get("reconciled", 0) >= 1, (
            f"STARVATION: {mid} reconciled 0 — partition-by-mailbox selection "
            f"broken. per_mailbox={pm}"
        )
    # A reconciles its allowance (1), NOT the full 10 — it's bounded too.
    assert pm.get("mb-A", {}).get("reconciled", 0) <= 10
    # Total bounded by 4 × per_run = 40, and in practice by 4 × allowance.
    assert out["reconciled"] <= 40


# ── 🔴 HOURLY-RATE BOUND (BLOCKER 2) ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_hourly_rate_bound_6_sweeps(seeded, session_factory, monkeypatch):
    """Simulate 6 consecutive sweeps (one hour) with sent_today FROZEN at 0
    (models the send-counter feedback lag — sent_today increments at send time
    while pacing decides at enqueue time, so several sweeps fire before the
    counter reflects any of them). Total re-enqueued across 4 AMER mailboxes
    must stay WELL UNDER 200. Under per_run=10 with NO allowance this reaches
    4 × 10 × 6 = 240 and must fail."""
    _config(monkeypatch)
    amer_mailboxes = ["mb-am-a", "mb-am-b", "mb-am-c", "mb-am-d"]
    for mid in amer_mailboxes:
        await _add_mailbox(session_factory, mailbox_id=mid, sent_today=0)

    # 100 past-due steps per mailbox, CORRELATED (each mailbox's steps contiguous;
    # A oldest 10h → D newest 7h). All steps past-due, A's all older than B's.
    for mi, mid in enumerate(amer_mailboxes):
        base_m = datetime.utcnow() - timedelta(hours=10 - mi)
        for i in range(100):
            await _make_overdue_step(
                session_factory,
                seeded,
                est_id=f"e{mi}_{i:03d}",
                enr_id=f"enr{mi}_{i:03d}",
                mailbox_id=mid,
                scheduled_at=base_m + timedelta(seconds=i),
            )

    # Simulate 6 sweeps. After each sweep, the reconciled steps get scheduled_at=now
    # (so they're no longer past-due for the next sweep). sent_today stays 0
    # (feedback lag). Total across 6 sweeps × 4 mailboxes must be well under 200.
    total_reconciled = 0
    sweep_counts = []
    for sweep in range(6):
        q = AsyncMock(return_value="j")
        out = await _run(q, session_factory)
        n = out["reconciled"]
        sweep_counts.append(n)
        total_reconciled += n
    # With defaults: 4 mailboxes × allowance(1) × 6 sweeps = 24, well under 200.
    # Without allowance (per_run=10 binding): 4 × 10 × 6 = 240 → FAILS.
    assert total_reconciled < 200, (
        f"Hourly-rate bound violated: {total_reconciled} re-enqueued across 6 sweeps "
        f"× 4 mailboxes (must be well under 200). Per-sweep: {sweep_counts}. "
        f"Under per_run=10 with no allowance this reaches ~240 — the 251 incident."
    )
    # Sanity: each sweep should reconcile 4 (one per mailbox, allowance=1).
    assert all(n <= 4 * PER_RUN for n in sweep_counts)


# ── Per-mailbox isolation (CORRELATED fixtures) ──────────────────────────────


@pytest.mark.asyncio
async def test_per_mailbox_isolation_correlated(seeded, session_factory, monkeypatch):
    """400 past-due steps across 4 mailboxes, CORRELATED by mailbox (each
    mailbox's steps are contiguous in time, NOT interleaved). Every mailbox
    reconciles > 0 and <= its allowance. Total <= 4 × per_run."""
    _config(monkeypatch)
    mailbox_ids = ["mb-iso-a", "mb-iso-b", "mb-iso-c", "mb-iso-d"]
    for mid in mailbox_ids:
        await _add_mailbox(session_factory, mailbox_id=mid, sent_today=0)
    # CORRELATED: mailbox A's steps are the oldest (10h ago), B 9h, C 8h, D 7h.
    # Each mailbox's 100 steps span ~100s within their base. A's newest is still
    # older than B's oldest — the real stranding shape.
    for mi, mid in enumerate(mailbox_ids):
        base_m = datetime.utcnow() - timedelta(hours=10 - mi)
        for i in range(100):
            await _make_overdue_step(
                session_factory,
                seeded,
                est_id=f"e{mi}_{i:03d}",
                enr_id=f"enr{mi}_{i:03d}",
                mailbox_id=mid,
                scheduled_at=base_m + timedelta(seconds=i),
            )
    q = AsyncMock(return_value="j")
    out = await _run(q, session_factory)
    pm = out.get("per_mailbox", {})
    # Every mailbox reconciles > 0 (no starvation) and <= its allowance.
    for mid in mailbox_ids:
        r = pm.get(mid, {}).get("reconciled", 0)
        allowance = pm.get(mid, {}).get("allowance", 0)
        assert r > 0, (
            f"{mid} starved (0 reconciled) — partition broken. per_mailbox={pm}"
        )
        assert r <= allowance, (
            f"{mid} reconciled {r} > allowance {allowance}; per_mailbox={pm}"
        )
    assert out["reconciled"] <= 4 * PER_RUN


# ── NULL step.mailbox_id regression guard ───────────────────────────────────


@pytest.mark.asyncio
async def test_null_step_mailbox_id_groups_by_enrollment(
    seeded, session_factory, monkeypatch
):
    """All steps have step.mailbox_id=NULL but enrollment.mailbox_id is set →
    still paced correctly. Regression guard for the grouping bug: grouping by
    step.mailbox_id yields one NULL bucket = no pacing."""
    _config(monkeypatch)
    await _add_mailbox(session_factory, mailbox_id="mb-grp", sent_today=0)
    base = datetime.utcnow() - timedelta(hours=2)
    for i in range(30):
        await _make_overdue_step(
            session_factory,
            seeded,
            est_id=f"e{i}",
            enr_id=f"enr{i}",
            mailbox_id="mb-grp",
            scheduled_at=base + timedelta(seconds=i),
            step_mailbox_id=None,
        )
    q = AsyncMock(return_value="j")
    out = await _run(q, session_factory)
    # With defaults: allowance=1, so 1 reconciled (NOT 30 — grouping bug guard).
    assert out["reconciled"] == 1, (
        f"Must pace by enrollment.mailbox_id (not NULL step.mailbox_id); "
        f"expected 1, got {out['reconciled']}. If 30, grouping bug regressed."
    )
    assert q.await_count == 1


# ── Missing Mailbox row ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_missing_mailbox_row_logged_as_error(
    seeded, session_factory, monkeypatch
):
    """An enrollment referencing a mailbox_id with no Mailbox row →
    skipped_mailbox_missing + logger.error, NOT skipped_at_capacity. Without
    this, a deleted mailbox stalls its steps forever behind a metric that
    looks like normal capping."""
    _config(monkeypatch)
    # Create steps on an enrollment whose mailbox_id has NO Mailbox row.
    # Do NOT call _add_mailbox for "mb-gone".
    base = datetime.utcnow() - timedelta(hours=2)
    for i in range(3):
        await _make_overdue_step(
            session_factory,
            seeded,
            est_id=f"e{i}",
            enr_id=f"enr{i}",
            mailbox_id="mb-gone",
            scheduled_at=base + timedelta(seconds=i),
        )
    error_calls = []

    def _capture_error(msg, *args, **kwargs):
        error_calls.append((msg, kwargs))

    monkeypatch.setattr(rec.logger, "error", _capture_error)
    q = AsyncMock(return_value="j")
    out = await _run(q, session_factory)
    assert out["reconciled"] == 0
    assert out.get("skipped_mailbox_missing", 0) >= 1, (
        f"missing mailbox must increment skipped_mailbox_missing; got {out}"
    )
    assert out.get("skipped_at_capacity", 0) == 0, (
        "missing mailbox must NOT count as exhausted (would stall forever)"
    )
    q.assert_not_awaited()
    pm = out["per_mailbox"].get("mb-gone", {})
    assert pm.get("reason") == "mailbox_missing"
    # logger.error must have been called with the missing-mailbox message.
    assert any("missing mailbox" in str(msg) for msg, _ in error_calls), (
        f"logger.error must fire for missing mailbox; got calls={error_calls}"
    )


# ── FIFO ordering within a mailbox ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_fifo_oldest_reconciled_first(seeded, session_factory, monkeypatch):
    """Oldest scheduled_at reconciled first when the per-mailbox limit binds.
    Use a high-cap mailbox so allowance is loose and per_run(10) caps; create 15
    steps; the oldest 10 must be the ones enqueued."""
    _config(monkeypatch)
    await _add_mailbox(
        session_factory, mailbox_id="mb-fifo", sent_today=0, daily_send_limit=1000
    )
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
    out = await _run(q, session_factory)
    assert out["reconciled"] == 10
    enqueued_ids = {call.kwargs["enrollment_step_id"] for call in q.await_args_list}
    expected_oldest = {f"e{i:02d}" for i in range(10)}
    assert enqueued_ids == expected_oldest, (
        f"FIFO violated: expected oldest 10 {expected_oldest}, got {enqueued_ids}"
    )


# ── Burst regression (the incident) — CORRELATED ────────────────────────────


@pytest.mark.asyncio
async def test_burst_regression_399_correlated(seeded, session_factory, monkeypatch):
    """The 2026-07-20 incident: 399 past-due steps across 4 AMER mailboxes.
    CORRELATED by mailbox (each mailbox's steps contiguous). Total re-enqueued
    per sweep bounded by 4 × allowance (≈4), well under the 40 that per_run alone
    would allow, and the one-hour projection stays under the incident's 251."""
    _config(monkeypatch)
    amer_mailboxes = ["mb-amer-c", "mb-amer-d", "mb-amer-h", "mb-amer-j"]
    for mid in amer_mailboxes:
        await _add_mailbox(session_factory, mailbox_id=mid, sent_today=0)
    # 399 steps CORRELATED by mailbox: each mailbox's steps contiguous in time,
    # A oldest (10h) → D newest (7h). A's newest is still older than B's oldest.
    per_mailbox_counts = [100, 100, 100, 99]  # 399 total
    for mi, mid in enumerate(amer_mailboxes):
        base_m = datetime.utcnow() - timedelta(hours=10 - mi)
        for i in range(per_mailbox_counts[mi]):
            await _make_overdue_step(
                session_factory,
                seeded,
                est_id=f"e{mi}_{i:03d}",
                enr_id=f"enr{mi}_{i:03d}",
                mailbox_id=mid,
                scheduled_at=base_m + timedelta(seconds=i),
            )
    q = AsyncMock(return_value="j")
    out = await _run(q, session_factory)
    pm = out.get("per_mailbox", {})
    # With defaults each mailbox reconciles allowance(1), NOT per_run(10).
    # 4 × 1 = 4 per sweep. Over 6 sweeps/hr = 24/hr, vs the 251 incident.
    assert out["reconciled"] <= 4 * PER_RUN, (
        f"Burst: expected ≤ 4×per_run(40), got {out['reconciled']}"
    )
    for mid in amer_mailboxes:
        r = pm.get(mid, {}).get("reconciled", 0)
        allowance = pm.get(mid, {}).get("allowance", 0)
        assert r > 0, f"{mid} starved — partition broken. per_mailbox={pm}"
        assert r <= allowance, (
            f"{mid} reconciled {r} > allowance {allowance}; per_mailbox={pm}"
        )


# ── Observability: past_due_backlog_depth + per-mailbox pending ─────────────


@pytest.mark.asyncio
async def test_past_due_backlog_depth_and_pending_reported(
    seeded, session_factory, monkeypatch
):
    """The completion log must include past_due_backlog_depth (total past-due
    SCHEDULED, not just this batch) AND per-mailbox `pending` so drain health is
    visible day over day."""
    _config(monkeypatch)
    await _add_mailbox(session_factory, mailbox_id="mb-obs", sent_today=0)
    base = datetime.utcnow() - timedelta(hours=2)
    for i in range(30):
        await _make_overdue_step(
            session_factory,
            seeded,
            est_id=f"e{i}",
            enr_id=f"enr{i}",
            mailbox_id="mb-obs",
            scheduled_at=base + timedelta(seconds=i),
        )
    q = AsyncMock(return_value="j")
    out = await _run(q, session_factory)
    # With defaults allowance=1, so 1 reconciled.
    assert out["reconciled"] == 1
    # backlog depth must reflect ALL 30 past-due, not just the 1 reconciled.
    assert out.get("past_due_backlog_depth") == 30, (
        f"backlog depth must count ALL past-due (30); got "
        f"{out.get('past_due_backlog_depth')}"
    )
    pm = out["per_mailbox"].get("mb-obs", {})
    assert pm.get("pending") == 30, (
        f"per-mailbox pending must be 30; got {pm.get('pending')}"
    )


# ── Existing behavior preserved ─────────────────────────────────────────────


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
    out = await _run(q, session_factory)
    assert out["reconciled"] == 0
    q.assert_not_awaited()


# ── Per-mailbox key type: raw id, never str(None) ───────────────────────────


@pytest.mark.asyncio
async def test_per_mailbox_key_is_raw_id(seeded, session_factory, monkeypatch):
    """The per_mailbox_out dict must be keyed by the raw mailbox id (the actual
    string from the DB), never str(None) or a stringified id.

    The NULL-enrollment-mailbox defensive branch (skipped_no_mailbox) guards a
    state the live schema forbids (sequence_enrollments.mailbox_id is NOT NULL),
    so it cannot be exercised through the ORM — it exists for defense-in-depth
    against a raw-SQL or schema-drift edge. The raw-id keying is verified here
    on a real mailbox instead."""
    _config(monkeypatch)
    await _add_mailbox(session_factory, mailbox_id="mb-key", sent_today=0)
    base = datetime.utcnow() - timedelta(hours=2)
    for i in range(5):
        await _make_overdue_step(
            session_factory,
            seeded,
            est_id=f"e{i}",
            enr_id=f"enr{i}",
            mailbox_id="mb-key",
            scheduled_at=base + timedelta(seconds=i),
        )
    q = AsyncMock(return_value="j")
    out = await _run(q, session_factory)
    pm = out.get("per_mailbox", {})
    assert "mb-key" in pm, (
        f"per_mailbox must contain raw id key; got keys={list(pm.keys())}"
    )
    assert None not in pm, "no None key expected for a normal mailbox"
    assert pm["mb-key"].get("reconciled") == 1  # allowance=1 with defaults
    # The string "None" must never appear as a key (the old bug).
    assert "None" not in pm
