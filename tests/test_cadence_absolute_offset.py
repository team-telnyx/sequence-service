"""REVOPS-1375 — cadence scheduling must be ABSOLUTE-from-enrollment, not incremental.

Before this fix, _queue_next_step scheduled each next step as
`utcnow() + delay_days` (incremental wait from the previous send), but Scout
authors `delay_days` as ABSOLUTE day-offsets from enrollment (Old-ICP
0,4,9,15,22). Result: a 22-day sequence ran over ~50 days because each step
re-anchored on the send time of the prior step, accumulating delays.

Fix: anchor scheduling on `enrollment.created_at` via a pure helper
`compute_next_scheduled_at`, with a past-due guard (never schedule in the past:
returns `max(target, now)`).

Tests (written FIRST, must fail before implementation):
  1. Unit, jitter OFF — exact absolute offsets + past-due guard.
  2. Unit, jitter ON — result within +/-send_jitter_minutes of the absolute target.
  3. Integration — process_sequence_step for step 1 (SENT via stub) schedules
     step 2 at `enrollment.created_at + delay_days`, NOT `utcnow() + delay_days`.
"""

from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest

import src.workers.sequence_step as ss
from src.models.models import (
    SequenceEnrollment,
    SequenceEnrollmentStep,
    SequenceStep,
    EnrollmentStatus,
    EnrollmentStepStatus,
)


# -- 1. Unit: pure helper, jitter OFF -----------------------------------------


def test_compute_next_scheduled_at_jitter_off_exact_offset():
    """created_at + delay_days, jitter disabled -> exact absolute target."""
    T0 = datetime(2026, 1, 1, 12, 0, 0)
    now = T0 + timedelta(hours=1)
    result = ss.compute_next_scheduled_at(
        created_at=T0,
        delay_days=4,
        delay_hours=0,
        now=now,
        jitter_seconds=0,
    )
    assert result == T0 + timedelta(days=4)


def test_compute_next_scheduled_at_full_sequence_offsets_jitter_off():
    """Old-ICP offsets {0,4,9,15,22}d from a fixed T0 -> exact T0 + offset.

    ``now`` is set 1 min BEFORE T0 so every offset's target is strictly in the
    future and the past-due guard (max(target, now)) never triggers -- this
    isolates the exact-offset assertion from the past-due guard, which has its
    own dedicated test.
    """
    T0 = datetime(2026, 1, 1, 12, 0, 0)
    now = T0 - timedelta(minutes=1)
    offsets = [0, 4, 9, 15, 22]
    results = [
        ss.compute_next_scheduled_at(
            created_at=T0,
            delay_days=d,
            delay_hours=0,
            now=now,
            jitter_seconds=0,
        )
        for d in offsets
    ]
    assert results == [T0 + timedelta(days=d) for d in offsets]


def test_compute_next_scheduled_at_past_due_guard_returns_now():
    """Past-due guard: when target < now, return now (never schedule in past)."""
    T0 = datetime(2026, 1, 1, 12, 0, 0)
    now = T0 + timedelta(days=15)  # target = T0 + 9d = now - 6d (past)
    result = ss.compute_next_scheduled_at(
        created_at=T0,
        delay_days=9,
        delay_hours=0,
        now=now,
        jitter_seconds=0,
    )
    assert result == now


# -- 2. Unit: pure helper, jitter ON ------------------------------------------


def test_compute_next_scheduled_at_jitter_on_within_bounds():
    """jitter_seconds in +/-15min -> result within +/-15min of the absolute target.

    The helper adds jitter_seconds AFTER the past-due guard. With the target in
    the future, scheduled = target, and result = target + jitter. Assert the
    result stays within +/-900s of the un-jittered target for several fixed
    jitter values spanning the full +/-send_jitter_minutes window.
    """
    T0 = datetime(2026, 1, 1, 12, 0, 0)
    now = T0 + timedelta(minutes=1)
    target = T0 + timedelta(days=4)
    for jitter in (-900, -300, 0, 300, 900):
        result = ss.compute_next_scheduled_at(
            created_at=T0,
            delay_days=4,
            delay_hours=0,
            now=now,
            jitter_seconds=jitter,
        )
        assert abs((result - target).total_seconds()) <= 900


# -- 3. Integration: process_sequence_step schedules step 2 from enrollment.created_at


async def _async_false(*a, **k):
    return False


async def _make_enrollment_with_step2_delay(session_factory, seeded, *, delay_days=4):
    """ACTIVE enrollment (created_at = now-1d) with step-1 PENDING and step-2
    PENDING. Sets the seeded step-2's delay_days so _queue_next_step uses it.

    Returns (estep-1-id, estep-2-id, T0=created_at).

    T0 = now - 1d is chosen so that T0 + 4d = now + 3d (future), while the old
    bug (utcnow() + 4d = now + 4d) differs by 1 day -- far beyond the +/-15min
    jitter window, so the test unambiguously distinguishes absolute vs
    incremental semantics.
    """
    T0 = datetime.utcnow() - timedelta(days=1)
    async with session_factory() as s:
        step2 = await s.get(SequenceStep, "step-2")
        step2.delay_days = delay_days
        step2.delay_hours = 0
        s.add(
            SequenceEnrollment(
                id="enr-cad",
                sequence_id=seeded["sequence_id"],
                mailbox_id=seeded["active_mailbox_id"],
                contact_email="cad@acme.com",
                contact_name="CAD",
                timezone="America/New_York",
                status=EnrollmentStatus.ACTIVE,
                current_step=0,
                created_at=T0,
            )
        )
        s.add(
            SequenceEnrollmentStep(
                id="estep-cad-1",
                enrollment_id="enr-cad",
                step_id="step-1",
                mailbox_id=seeded["active_mailbox_id"],
                status=EnrollmentStepStatus.PENDING,
                custom_subject="Hi",
                custom_body="<p>Body</p>",
            )
        )
        s.add(
            SequenceEnrollmentStep(
                id="estep-cad-2",
                enrollment_id="enr-cad",
                step_id="step-2",
                mailbox_id=seeded["active_mailbox_id"],
                status=EnrollmentStepStatus.PENDING,
            )
        )
        await s.commit()
    return "estep-cad-1", "estep-cad-2", T0


@pytest.mark.asyncio
async def test_process_sequence_step_schedules_step2_from_enrollment_created_at(
    seeded,
    session_factory,
    monkeypatch,
):
    """Step 1 SENT (stub) -> step 2 SCHEDULED at enrollment.created_at + delay_days,
    NOT utcnow() + delay_days. Jitter disabled for an exact assertion."""
    monkeypatch.setattr(ss.settings, "send_jitter_enabled", False, raising=False)
    monkeypatch.setattr(ss.settings, "gmail_enabled", False, raising=False)
    estep1, estep2, T0 = await _make_enrollment_with_step2_delay(
        session_factory,
        seeded,
        delay_days=4,
    )
    q = AsyncMock(return_value="job-cad")
    cms = [
        patch.object(ss, "async_session", session_factory),
        patch.object(ss, "check_suppressed", new=_async_false),
        patch.object(ss, "check_circuit_breaker", new=_async_false),
        patch.object(ss, "check_send_window", new=lambda tz: None),
        patch.object(ss, "queue_sequence_step", q),
    ]
    for c in cms:
        c.start()
    try:
        await ss.process_sequence_step({}, estep1, seeded["tenant_id"])
    finally:
        for c in cms:
            c.stop()

    async with session_factory() as s:
        step2 = await s.get(SequenceEnrollmentStep, estep2)

    assert step2.status == EnrollmentStepStatus.SCHEDULED
    expected_absolute = T0 + timedelta(days=4)
    assert step2.scheduled_at == expected_absolute

    # Definitively NOT the old incremental behavior (utcnow() + 4d). The ~1-day
    # gap between T0+4d and utcnow()+4d is far beyond any jitter/rounding, so a
    # >23h distance from the old-bug result proves absolute semantics.
    old_bug_expected = datetime.utcnow() + timedelta(days=4)
    assert (
        abs((step2.scheduled_at - old_bug_expected).total_seconds())
        > timedelta(hours=23).total_seconds()
    )


# -- 4. REVOPS-1376 min-gap catch-up spacing (Part 2A) -----------------------


def test_compute_next_scheduled_at_min_gap_off_zero_returns_now():
    """min_gap_seconds=0 reproduces Part D's past-due behavior (== now)."""
    T0 = datetime(2026, 1, 1, 12, 0, 0)
    now = T0 + timedelta(days=15)  # target = T0 + 9d, past-due
    result = ss.compute_next_scheduled_at(
        created_at=T0,
        delay_days=9,
        delay_hours=0,
        now=now,
        jitter_seconds=0,
        min_gap_seconds=0,
    )
    assert result == now


def test_compute_next_scheduled_at_min_gap_on_past_due_returns_now_plus_gap():
    """min_gap on: past-due step schedules at now + min_gap (jitter=0)."""
    T0 = datetime(2026, 1, 1, 12, 0, 0)
    now = T0 + timedelta(days=15)  # target = T0 + 9d, past-due
    min_gap = 3600  # 1h
    result = ss.compute_next_scheduled_at(
        created_at=T0,
        delay_days=9,
        delay_hours=0,
        now=now,
        jitter_seconds=0,
        min_gap_seconds=min_gap,
    )
    assert result == now + timedelta(seconds=min_gap)


def test_compute_next_scheduled_at_min_gap_on_jitter_within_bounds():
    """min_gap + jitter: result within +/-jitter of now + min_gap (past-due)."""
    T0 = datetime(2026, 1, 1, 12, 0, 0)
    now = T0 + timedelta(days=15)
    min_gap = 3600
    target = now + timedelta(seconds=min_gap)
    for jitter in (-900, -300, 0, 300, 900):
        result = ss.compute_next_scheduled_at(
            created_at=T0,
            delay_days=9,
            delay_hours=0,
            now=now,
            jitter_seconds=jitter,
            min_gap_seconds=min_gap,
        )
        assert abs((result - target).total_seconds()) <= 900


def test_compute_next_scheduled_at_consecutive_catch_up_min_gap_apart():
    """Two consecutive catch-up advances are exactly min_gap apart (jitter=0).

    Step A past-due (delay_days=9, now=T0+15d) -> now1 + min_gap.
    Step B past-due (delay_days=15, now=now1+min_gap) -> (now1+min_gap) + min_gap.
    The two scheduled_at values differ by exactly min_gap_seconds.
    """
    T0 = datetime(2026, 1, 1, 12, 0, 0)
    min_gap = 86400  # 1d
    now1 = T0 + timedelta(days=15)  # step A: target T0+9d, past-due
    a = ss.compute_next_scheduled_at(
        created_at=T0,
        delay_days=9,
        delay_hours=0,
        now=now1,
        jitter_seconds=0,
        min_gap_seconds=min_gap,
    )
    now2 = a  # step B's "now" is step A's scheduled_at (consecutive advance)
    b = ss.compute_next_scheduled_at(
        created_at=T0,
        delay_days=15,
        delay_hours=0,
        now=now2,
        jitter_seconds=0,
        min_gap_seconds=min_gap,
    )
    assert (b - a).total_seconds() == min_gap


def test_compute_next_scheduled_at_negative_jitter_past_due_ge_now():
    """Negative jitter + past-due + min_gap: result >= now (never before)."""
    T0 = datetime(2026, 1, 1, 12, 0, 0)
    now = T0 + timedelta(days=15)
    result = ss.compute_next_scheduled_at(
        created_at=T0,
        delay_days=9,
        delay_hours=0,
        now=now,
        jitter_seconds=-900,
        min_gap_seconds=3600,
    )
    assert result >= now


def test_compute_next_scheduled_at_on_cadence_unaffected_by_min_gap():
    """On-cadence step (target > now): min_gap not applied, still == target."""
    T0 = datetime(2026, 1, 1, 12, 0, 0)
    now = T0 + timedelta(hours=1)  # target T0+4d is in the future
    result = ss.compute_next_scheduled_at(
        created_at=T0,
        delay_days=4,
        delay_hours=0,
        now=now,
        jitter_seconds=0,
        min_gap_seconds=86400,  # 1d, must be ignored since target is future
    )
    assert result == T0 + timedelta(days=4)
