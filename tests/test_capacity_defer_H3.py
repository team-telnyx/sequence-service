"""H3 (REVOPS-972) — at-capacity worker send must DEFER, not raise.

Before this fix, sequence_step.process_sequence_step raised
`RuntimeError("Failed to reserve send slot - mailbox at capacity")` when
reserve_send returned False (mailbox at the hard 75/day cap). arq then retried
3x/30s and abandoned — wasted retries + error-log noise + ~10-20 min latency on
a hot follow-up pinned to a full sticky mailbox.

Fix (mirrors the clean send-window re-queue at sequence_step.py ~115-126):
on reserve_send==False, re-queue the SAME step with a delay to the next 00:05
UTC capacity reset and return {"deferred": True, "reason": "mailbox_at_capacity"}
— no RuntimeError, no SentEmail row, no Gmail send.

The 75/day atomic cap (reserve_send conditional UPDATE) is UNCHANGED — this only
swaps crash->defer behavior.
"""
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest

import src.workers.sequence_step as ss
from src.services.mailbox_rotation import next_capacity_reset
from src.models.models import (
    SequenceEnrollment, SequenceEnrollmentStep, SentEmail,
    EnrollmentStatus, EnrollmentStepStatus,
)


async def _make_enrollment_step(session_factory, seeded, *, mailbox_id):
    """An ACTIVE enrollment whose pending step is pinned to a (full) mailbox."""
    async with session_factory() as s:
        s.add(SequenceEnrollment(
            id="enr-cap", sequence_id=seeded["sequence_id"],
            mailbox_id=mailbox_id, contact_email="vp@acme.com",
            contact_name="VP", timezone="America/New_York",
            status=EnrollmentStatus.ACTIVE, current_step=0,
        ))
        s.add(SequenceEnrollmentStep(
            id="estep-cap", enrollment_id="enr-cap", step_id="step-1",
            mailbox_id=mailbox_id, status=EnrollmentStepStatus.PENDING,
            scheduled_at=None, custom_subject="Hi", custom_body="<p>Body</p>",
        ))
        await s.commit()
    return "estep-cap"


# ── mailbox_rotation: clean at-capacity reset-time helper ────────────────────

def test_next_capacity_reset_is_next_0005_utc():
    # 12:00 UTC -> reset is 00:05 UTC the NEXT day
    now = datetime(2026, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
    nxt = next_capacity_reset(now)
    assert nxt == datetime(2026, 6, 16, 0, 5, 0, tzinfo=timezone.utc)


def test_next_capacity_reset_before_0005_is_today():
    # 00:02 UTC (in the 00:00-00:05 window) -> reset is 00:05 UTC the SAME day
    now = datetime(2026, 6, 15, 0, 2, 0, tzinfo=timezone.utc)
    nxt = next_capacity_reset(now)
    assert nxt == datetime(2026, 6, 15, 0, 5, 0, tzinfo=timezone.utc)


def test_next_capacity_reset_delay_seconds_positive():
    now = datetime(2026, 6, 15, 23, 0, 0, tzinfo=timezone.utc)
    nxt = next_capacity_reset(now)
    assert (nxt - now).total_seconds() == pytest.approx(65 * 60, abs=1)


# ── worker: at-capacity defers (no raise, no send) ───────────────────────────

@pytest.mark.asyncio
async def test_at_capacity_step_defers_not_raises(seeded, session_factory):
    """Full sticky mailbox -> reserve_send False -> defer (no RuntimeError)."""
    est = await _make_enrollment_step(
        session_factory, seeded, mailbox_id=seeded["full_mailbox_id"]
    )
    q = AsyncMock(return_value="job-deferred")
    with patch.object(ss, "async_session", session_factory), \
         patch.object(ss, "queue_sequence_step", q):
        out = await ss.process_sequence_step(
            {}, est, seeded["tenant_id"]
        )
    assert out == {"deferred": True, "reason": "mailbox_at_capacity"}
    # Re-queued the SAME step with a positive delay toward the next reset.
    q.assert_awaited_once()
    assert q.await_args.kwargs["enrollment_step_id"] == est
    assert q.await_args.kwargs["tenant_id"] == seeded["tenant_id"]
    assert q.await_args.kwargs["delay_seconds"] > 0


@pytest.mark.asyncio
async def test_at_capacity_step_sends_nothing(seeded, session_factory):
    """Deferral must not create a SentEmail row nor mark the step SENT."""
    est = await _make_enrollment_step(
        session_factory, seeded, mailbox_id=seeded["full_mailbox_id"]
    )
    q = AsyncMock(return_value="job-deferred")
    with patch.object(ss, "async_session", session_factory), \
         patch.object(ss, "queue_sequence_step", q):
        await ss.process_sequence_step({}, est, seeded["tenant_id"])

    async with session_factory() as s:
        sent = (await s.execute(
            SentEmail.__table__.select().where(
                SentEmail.enrollment_step_id == est
            )
        )).first()
        assert sent is None
        step = await s.get(SequenceEnrollmentStep, est)
        # step is left in a re-processable state, never SENT
        assert step.status != EnrollmentStepStatus.SENT


@pytest.mark.asyncio
async def test_at_capacity_does_not_burn_a_send_slot(seeded, session_factory):
    """Deferral must not consume the full mailbox's sent_today (no over-send)."""
    from src.models.models import Mailbox
    est = await _make_enrollment_step(
        session_factory, seeded, mailbox_id=seeded["full_mailbox_id"]
    )
    q = AsyncMock(return_value="job-deferred")
    with patch.object(ss, "async_session", session_factory), \
         patch.object(ss, "queue_sequence_step", q):
        await ss.process_sequence_step({}, est, seeded["tenant_id"])
    async with session_factory() as s:
        mb = await s.get(Mailbox, seeded["full_mailbox_id"])
        assert mb.sent_today == 50  # the seeded cap, unchanged
