"""Reconciler for enrollment steps stranded in SCHEDULED (audit M4 / REVOPS-892).

A step is marked SCHEDULED and committed *before* its arq job is enqueued
(sequence_step._queue_next_step). If the enqueue throws, Redis is flushed, or
the worker is down when the deferred job should fire, the step stays SCHEDULED
forever with no job behind it — the enrollment never advances and never
completes. Before this fix `scheduled_at` was also never written, so there was
no way to even detect a stuck step.

Fix: (1) write scheduled_at whenever a step is set SCHEDULED; (2) a cron sweep
(reconcile_scheduled_steps) re-enqueues SCHEDULED steps that are overdue
(scheduled_at < now - grace) or have a NULL scheduled_at (legacy stuck rows),
and pushes scheduled_at forward so an in-flight step isn't re-selected next sweep.
"""
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest

import src.workers.reconcile as rec
import src.workers.sequence_step as ss
from src.models.models import (
    SequenceEnrollment, SequenceEnrollmentStep,
    EnrollmentStatus, EnrollmentStepStatus,
)


async def _make_step(session_factory, seeded, *, status, scheduled_at,
                     step_id="step-1", est_id="estep-1", enr_id="enr-1"):
    async with session_factory() as s:
        s.add(SequenceEnrollment(
            id=enr_id, sequence_id=seeded["sequence_id"],
            mailbox_id=seeded["active_mailbox_id"],
            contact_email=f"vp+{enr_id}@acme.com",  # unique per enrollment
            contact_name="VP", timezone="America/New_York",
            status=EnrollmentStatus.ACTIVE, current_step=0,
        ))
        s.add(SequenceEnrollmentStep(
            id=est_id, enrollment_id=enr_id, step_id=step_id,
            mailbox_id=seeded["active_mailbox_id"], status=status,
            scheduled_at=scheduled_at, custom_subject="Hi", custom_body="<p>B</p>",
        ))
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


# ── reconciler selection ─────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_overdue_scheduled_step_reenqueued(seeded, session_factory, monkeypatch):
    monkeypatch.setattr(rec.settings, "reconcile_grace_seconds", 600, raising=False)
    past = datetime.utcnow() - timedelta(hours=2)
    est = await _make_step(session_factory, seeded,
                           status=EnrollmentStepStatus.SCHEDULED, scheduled_at=past)
    q = AsyncMock(return_value="job-1")
    cms = _patch(session_factory, q)
    _enter(cms)
    try:
        out = await rec.reconcile_scheduled_steps({})
    finally:
        _exit(cms)
    assert out["reconciled"] == 1
    q.assert_awaited_once()
    assert q.await_args.kwargs["enrollment_step_id"] == est
    assert q.await_args.kwargs["tenant_id"] == seeded["tenant_id"]
    # scheduled_at pushed forward (within grace now) so next sweep won't re-pick
    _, sched = await _status_and_sched(session_factory, est)
    assert sched > datetime.utcnow() - timedelta(seconds=60)


@pytest.mark.asyncio
async def test_null_scheduled_at_skipped(seeded, session_factory):
    # A NULL scheduled_at is ambiguous under pre-fix data (could be a valid
    # future job). The live reconciler must NOT re-fire it; the one-time backfill
    # computes its intended fire time instead.
    await _make_step(session_factory, seeded,
                     status=EnrollmentStepStatus.SCHEDULED, scheduled_at=None)
    q = AsyncMock(return_value="job-1")
    cms = _patch(session_factory, q)
    _enter(cms)
    try:
        out = await rec.reconcile_scheduled_steps({})
    finally:
        _exit(cms)
    assert out["reconciled"] == 0
    q.assert_not_awaited()


@pytest.mark.asyncio
async def test_future_scheduled_step_skipped(seeded, session_factory):
    future = datetime.utcnow() + timedelta(days=2)
    await _make_step(session_factory, seeded,
                     status=EnrollmentStepStatus.SCHEDULED, scheduled_at=future)
    q = AsyncMock()
    cms = _patch(session_factory, q)
    _enter(cms)
    try:
        out = await rec.reconcile_scheduled_steps({})
    finally:
        _exit(cms)
    assert out["reconciled"] == 0
    q.assert_not_awaited()


@pytest.mark.asyncio
async def test_recent_within_grace_skipped(seeded, session_factory, monkeypatch):
    monkeypatch.setattr(rec.settings, "reconcile_grace_seconds", 600, raising=False)
    recent = datetime.utcnow() - timedelta(seconds=120)  # past, but inside 600s grace
    await _make_step(session_factory, seeded,
                     status=EnrollmentStepStatus.SCHEDULED, scheduled_at=recent)
    q = AsyncMock()
    cms = _patch(session_factory, q)
    _enter(cms)
    try:
        out = await rec.reconcile_scheduled_steps({})
    finally:
        _exit(cms)
    assert out["reconciled"] == 0
    q.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [
    EnrollmentStepStatus.PENDING,
    EnrollmentStepStatus.SENT,
    EnrollmentStepStatus.SKIPPED,
])
async def test_non_scheduled_status_ignored(seeded, session_factory, status):
    await _make_step(session_factory, seeded,
                     status=status, scheduled_at=datetime.utcnow() - timedelta(hours=2))
    q = AsyncMock()
    cms = _patch(session_factory, q)
    _enter(cms)
    try:
        out = await rec.reconcile_scheduled_steps({})
    finally:
        _exit(cms)
    assert out["reconciled"] == 0
    q.assert_not_awaited()


@pytest.mark.asyncio
async def test_batch_limit_respected(seeded, session_factory, monkeypatch):
    monkeypatch.setattr(rec.settings, "reconcile_batch_limit", 2, raising=False)
    past = datetime.utcnow() - timedelta(hours=2)
    for i in range(5):
        await _make_step(session_factory, seeded, status=EnrollmentStepStatus.SCHEDULED,
                         scheduled_at=past, est_id=f"e{i}", enr_id=f"enr{i}")
    q = AsyncMock(return_value="j")
    cms = _patch(session_factory, q)
    _enter(cms)
    try:
        out = await rec.reconcile_scheduled_steps({})
    finally:
        _exit(cms)
    assert out["reconciled"] == 2
    assert q.await_count == 2


@pytest.mark.asyncio
async def test_one_enqueue_failure_does_not_block_others(seeded, session_factory):
    past = datetime.utcnow() - timedelta(hours=2)
    for i in range(2):
        await _make_step(session_factory, seeded, status=EnrollmentStepStatus.SCHEDULED,
                         scheduled_at=past, est_id=f"e{i}", enr_id=f"enr{i}")
    q = AsyncMock(side_effect=[RuntimeError("redis down"), "job-ok"])
    cms = _patch(session_factory, q)
    _enter(cms)
    try:
        out = await rec.reconcile_scheduled_steps({})
    finally:
        _exit(cms)
    # the failed one is not counted/reset; the other succeeds
    assert out["reconciled"] == 1
    assert q.await_count == 2


# ── scheduled_at is written when a step is set SCHEDULED ──────────────────────

@pytest.mark.asyncio
async def test_queue_next_step_sets_scheduled_at(seeded, session_factory, monkeypatch):
    # enrollment on step 1 (SENT), step 2 PENDING -> _queue_next_step should
    # mark step 2 SCHEDULED *and* set scheduled_at ~ now + delay.
    # Disable jitter so scheduled_at is deterministically ~now (step-2 delay = 0).
    monkeypatch.setattr(ss.settings, "send_jitter_enabled", False, raising=False)
    async with session_factory() as s:
        s.add(SequenceEnrollment(
            id="enr-1", sequence_id=seeded["sequence_id"],
            mailbox_id=seeded["active_mailbox_id"], contact_email="vp@acme.com",
            contact_name="VP", timezone="America/New_York",
            status=EnrollmentStatus.ACTIVE, current_step=1,
        ))
        s.add(SequenceEnrollmentStep(
            id="es2", enrollment_id="enr-1", step_id="step-2",
            mailbox_id=seeded["active_mailbox_id"],
            status=EnrollmentStepStatus.PENDING, scheduled_at=None,
        ))
        await s.commit()

    q = AsyncMock(return_value="job-x")
    with patch.object(ss, "queue_sequence_step", q):
        async with session_factory() as db:
            enr = await db.get(SequenceEnrollment, "enr-1")
            await ss._queue_next_step(db, enr, current_step_number=1,
                                      tenant_id=seeded["tenant_id"])
    status, sched = await _status_and_sched(session_factory, "es2")
    assert status == EnrollmentStepStatus.SCHEDULED
    assert sched is not None
    # step-2 delay in the seeded sequence is 0 days -> scheduled ~ now
    assert abs((sched - datetime.utcnow()).total_seconds()) < 120
