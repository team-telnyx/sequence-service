"""Auto-resume circuit_breaker-paused enrollments (REVOPS — circuit breaker had no resume).

The breaker pauses a mailbox's enrollments when bounce rate is high but never
un-pauses them. This cron resumes them once the mailbox cools below the resume
threshold (hysteresis), and re-queues the next step. A still-elevated mailbox
stays paused.
"""
from datetime import datetime
from unittest.mock import AsyncMock, patch

import pytest

import src.workers.circuit_resume as cr
from src.models.models import (
    EnrollmentStatus, EnrollmentStepStatus,
    SequenceEnrollment, SequenceEnrollmentStep,
)


async def _make_paused(session_factory, seeded, *, enr_id, mailbox_id,
                       pause_reason="circuit_breaker", status=EnrollmentStatus.PAUSED):
    async with session_factory() as s:
        s.add(SequenceEnrollment(
            id=enr_id, sequence_id=seeded["sequence_id"], mailbox_id=mailbox_id,
            contact_email=f"vp+{enr_id}@acme.com", contact_name="VP",
            timezone="America/New_York", status=status, current_step=1,
            pause_reason=pause_reason,
        ))
        # step 1 already SENT, step 2 PENDING (the next touch to resume)
        s.add(SequenceEnrollmentStep(id=f"{enr_id}-s1", enrollment_id=enr_id, step_id="step-1",
                                     mailbox_id=mailbox_id, status=EnrollmentStepStatus.SENT))
        s.add(SequenceEnrollmentStep(id=f"{enr_id}-s2", enrollment_id=enr_id, step_id="step-2",
                                     mailbox_id=mailbox_id, status=EnrollmentStepStatus.PENDING))
        await s.commit()


async def _enr(session_factory, enr_id):
    async with session_factory() as s:
        return await s.get(SequenceEnrollment, enr_id)


def _patches(session_factory, rate, q):
    return [
        patch.object(cr, "async_session", session_factory),
        patch.object(cr, "mailbox_bounce_rate", AsyncMock(return_value=rate)),
        patch.object(cr, "queue_sequence_step", q),
    ]


def _run(cms):
    for c in cms:
        c.start()


def _stop(cms):
    for c in cms:
        c.stop()


@pytest.mark.asyncio
async def test_resumes_when_mailbox_cooled(seeded, session_factory, monkeypatch):
    monkeypatch.setattr(cr.settings, "circuit_breaker_resume_threshold", 0.06, raising=False)
    await _make_paused(session_factory, seeded, enr_id="e1", mailbox_id=seeded["active_mailbox_id"])
    q = AsyncMock(return_value="job-1")
    cms = _patches(session_factory, 0.02, q)  # 2% < 6% → resume
    _run(cms)
    try:
        out = await cr.resume_circuit_breaker_paused({})
    finally:
        _stop(cms)
    assert out["resumed"] == 1
    e = await _enr(session_factory, "e1")
    assert e.status == EnrollmentStatus.ACTIVE and e.pause_reason is None
    q.assert_awaited_once()
    assert q.await_args.kwargs["enrollment_step_id"] == "e1-s2"  # the next (PENDING) touch


@pytest.mark.asyncio
async def test_stays_paused_when_still_elevated(seeded, session_factory, monkeypatch):
    monkeypatch.setattr(cr.settings, "circuit_breaker_resume_threshold", 0.06, raising=False)
    await _make_paused(session_factory, seeded, enr_id="e2", mailbox_id=seeded["active_mailbox_id"])
    q = AsyncMock()
    cms = _patches(session_factory, 0.085, q)  # 8.5% >= 6% → stay paused (quinn.g case)
    _run(cms)
    try:
        out = await cr.resume_circuit_breaker_paused({})
    finally:
        _stop(cms)
    assert out["resumed"] == 0 and out["skipped_hot"] == 1
    e = await _enr(session_factory, "e2")
    assert e.status == EnrollmentStatus.PAUSED
    q.assert_not_awaited()


@pytest.mark.asyncio
async def test_no_sends_window_is_safe_to_resume(seeded, session_factory):
    await _make_paused(session_factory, seeded, enr_id="e3", mailbox_id=seeded["active_mailbox_id"])
    q = AsyncMock(return_value="j")
    cms = _patches(session_factory, None, q)  # None = no recent sends → resume
    _run(cms)
    try:
        out = await cr.resume_circuit_breaker_paused({})
    finally:
        _stop(cms)
    assert out["resumed"] == 1


@pytest.mark.asyncio
async def test_only_circuit_breaker_pauses_resumed(seeded, session_factory):
    # a reply-paused enrollment must NOT be touched by this job.
    await _make_paused(session_factory, seeded, enr_id="e4", mailbox_id=seeded["active_mailbox_id"],
                       pause_reason="reply")
    q = AsyncMock()
    cms = _patches(session_factory, 0.0, q)
    _run(cms)
    try:
        out = await cr.resume_circuit_breaker_paused({})
    finally:
        _stop(cms)
    assert out["resumed"] == 0
    e = await _enr(session_factory, "e4")
    assert e.status == EnrollmentStatus.PAUSED and e.pause_reason == "reply"


@pytest.mark.asyncio
async def test_defers_when_mailbox_at_capacity(seeded, session_factory):
    # mb-full is sent_today=50 / cap=50 → spare 0 → resume nothing (yield to
    # in-flight enrollments), even though the mailbox is cool.
    await _make_paused(session_factory, seeded, enr_id="ef", mailbox_id=seeded["full_mailbox_id"])
    q = AsyncMock()
    cms = _patches(session_factory, 0.0, q)
    _run(cms)
    try:
        out = await cr.resume_circuit_breaker_paused({})
    finally:
        _stop(cms)
    assert out["resumed"] == 0 and out["skipped_full"] == 1
    e = await _enr(session_factory, "ef")
    assert e.status == EnrollmentStatus.PAUSED
    q.assert_not_awaited()


@pytest.mark.asyncio
async def test_per_run_cap_limits_resume(seeded, session_factory, monkeypatch):
    # mb-active has spare 40 (sent 10 / cap 50); with per-run cap 3 and 8 paused,
    # only 3 resume this run (the rest trickle on later runs).
    monkeypatch.setattr(cr.settings, "circuit_breaker_resume_per_run", 3, raising=False)
    for i in range(8):
        await _make_paused(session_factory, seeded, enr_id=f"pc{i}",
                           mailbox_id=seeded["active_mailbox_id"])
    q = AsyncMock(return_value="j")
    cms = _patches(session_factory, 0.0, q)
    _run(cms)
    try:
        out = await cr.resume_circuit_breaker_paused({})
    finally:
        _stop(cms)
    assert out["resumed"] == 3
