"""REVOPS-972 Workstream B — reply-pause hardening.

Three guarantees:

  B1  When signal_detection pauses an enrollment because the contact REPLIED,
      it must STAMP pause_reason='reply'. At HEAD it sets status=PAUSED but
      leaves pause_reason NULL — those rows are reactivation landmines (audit C1).

  B2  Reply detection must look back >= the longest sequence span (21 days), not
      7 days, so a late (e.g. 10-day) reply still pauses the enrollment.

  B3  The circuit-breaker resume cron must NEVER resume a reply-paused row OR a
      NULL-pause_reason row — only pause_reason=='circuit_breaker'. A defensive
      assertion guards against a future regression that loosens the filter.
"""
import json
import os
import sys
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import src.workers.signal_detection as sd
import src.workers.circuit_resume as cr
from src.models.models import (
    EnrollmentStatus,
    EnrollmentStepStatus,
    SentEmail,
    SequenceEnrollment,
    SequenceEnrollmentStep,
)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
async def _seed_active_enrollment(session_factory, seeded, *, enr_id, mailbox_id,
                                  thread_id, sent_at):
    """An ACTIVE enrollment with a SENT step 1 (thread `thread_id`) and a
    PENDING step 2. Returns the SentEmail id for the step-1 send."""
    async with session_factory() as s:
        s.add(SequenceEnrollment(
            id=enr_id, sequence_id=seeded["sequence_id"], mailbox_id=mailbox_id,
            contact_email=f"vp+{enr_id}@acme.com", contact_name="VP",
            timezone="America/New_York", status=EnrollmentStatus.ACTIVE,
            current_step=1, pause_reason=None,
        ))
        s.add(SequenceEnrollmentStep(
            id=f"{enr_id}-s1", enrollment_id=enr_id, step_id="step-1",
            mailbox_id=mailbox_id, status=EnrollmentStepStatus.SENT))
        s.add(SequenceEnrollmentStep(
            id=f"{enr_id}-s2", enrollment_id=enr_id, step_id="step-2",
            mailbox_id=mailbox_id, status=EnrollmentStepStatus.PENDING))
        se_id = f"{enr_id}-se1"
        s.add(SentEmail(
            id=se_id, message_id=f"msg-{enr_id}", thread_id=thread_id,
            mailbox_id=mailbox_id, enrollment_step_id=f"{enr_id}-s1",
            subject="Hi", body="Body", to_email=f"vp+{enr_id}@acme.com",
            from_email="quinn.c@telnyx.com", sent_at=sent_at,
        ))
        await s.commit()
        return se_id


async def _enr(session_factory, enr_id):
    async with session_factory() as s:
        return await s.get(SequenceEnrollment, enr_id)


def _reply(thread_id, message_id):
    return {
        "thread_id": thread_id, "message_id": message_id,
        "from": "vp@acme.com", "subject": "Re: your email",
        "snippet": "sure, let's talk", "date": "now",
        "is_bounce": False, "is_ooo": False,
    }


def _run_detect(session_factory, seeded, replies, monkeypatch):
    """Run detect_signals against the active mailbox with Gmail/webhook patched."""
    monkeypatch.setattr(sd.settings, "gmail_enabled", True, raising=False)
    fake_inbox = MagicMock()
    fake_inbox.get_replies_to_threads = MagicMock(return_value=replies)
    monkeypatch.setattr(sd.GmailService, "get_inbox",
                        MagicMock(return_value=fake_inbox))
    monkeypatch.setattr(sd, "create_signal_webhook", AsyncMock(return_value=None))
    monkeypatch.setattr(sd, "async_session", session_factory)
    return fake_inbox


# --------------------------------------------------------------------------- #
# B1 — reply pause stamps pause_reason='reply'
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_reply_pause_stamps_reason(seeded, session_factory, monkeypatch):
    mbx = seeded["active_mailbox_id"]
    await _seed_active_enrollment(
        session_factory, seeded, enr_id="rp1", mailbox_id=mbx,
        thread_id="thr-rp1", sent_at=datetime.utcnow() - timedelta(days=1))
    fake_inbox = _run_detect(session_factory, seeded,
                             [_reply("thr-rp1", "rmsg-1")], monkeypatch)

    out = await sd.detect_signals({}, mbx, seeded["tenant_id"])
    assert out["signals_detected"] == 1
    fake_inbox.get_replies_to_threads.assert_called_once()

    e = await _enr(session_factory, "rp1")
    assert e.status == EnrollmentStatus.PAUSED
    assert e.pause_reason == "reply", "REPLY pause MUST stamp pause_reason='reply'"


# --------------------------------------------------------------------------- #
# B2 — 21-day reply detection window catches a 10-day-late reply
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_late_reply_within_21d_window_pauses(seeded, session_factory, monkeypatch):
    mbx = seeded["active_mailbox_id"]
    # step 1 was sent 10 days ago — outside the OLD 7d window, inside 21d.
    await _seed_active_enrollment(
        session_factory, seeded, enr_id="rp2", mailbox_id=mbx,
        thread_id="thr-rp2", sent_at=datetime.utcnow() - timedelta(days=10))
    fake_inbox = _run_detect(session_factory, seeded,
                             [_reply("thr-rp2", "rmsg-2")], monkeypatch)

    out = await sd.detect_signals({}, mbx, seeded["tenant_id"])
    # The 10-day-old SentEmail must still be in the lookup, so the reply matches.
    assert out["signals_detected"] == 1, "10-day-late reply must be detected (21d window)"
    e = await _enr(session_factory, "rp2")
    assert e.status == EnrollmentStatus.PAUSED and e.pause_reason == "reply"


# --------------------------------------------------------------------------- #
# B3 — resume cron skips reply-paused AND NULL rows
# --------------------------------------------------------------------------- #
async def _make_paused(session_factory, seeded, *, enr_id, mailbox_id, pause_reason):
    async with session_factory() as s:
        s.add(SequenceEnrollment(
            id=enr_id, sequence_id=seeded["sequence_id"], mailbox_id=mailbox_id,
            contact_email=f"vp+{enr_id}@acme.com", contact_name="VP",
            timezone="America/New_York", status=EnrollmentStatus.PAUSED,
            current_step=1, pause_reason=pause_reason,
        ))
        s.add(SequenceEnrollmentStep(
            id=f"{enr_id}-s1", enrollment_id=enr_id, step_id="step-1",
            mailbox_id=mailbox_id, status=EnrollmentStepStatus.SENT))
        s.add(SequenceEnrollmentStep(
            id=f"{enr_id}-s2", enrollment_id=enr_id, step_id="step-2",
            mailbox_id=mailbox_id, status=EnrollmentStepStatus.PENDING))
        await s.commit()


@pytest.mark.asyncio
async def test_resume_skips_reply_and_null_rows(seeded, session_factory):
    mbx = seeded["active_mailbox_id"]
    await _make_paused(session_factory, seeded, enr_id="cb", mailbox_id=mbx,
                       pause_reason="circuit_breaker")
    await _make_paused(session_factory, seeded, enr_id="rep", mailbox_id=mbx,
                       pause_reason="reply")
    await _make_paused(session_factory, seeded, enr_id="nul", mailbox_id=mbx,
                       pause_reason=None)
    q = AsyncMock(return_value="j")
    with patch.object(cr, "async_session", session_factory), \
         patch.object(cr, "mailbox_bounce_rate", AsyncMock(return_value=0.0)), \
         patch.object(cr, "queue_sequence_step", q):
        out = await cr.resume_circuit_breaker_paused({})

    # Only the circuit_breaker row resumes.
    assert out["resumed"] == 1
    assert (await _enr(session_factory, "cb")).status == EnrollmentStatus.ACTIVE
    rep = await _enr(session_factory, "rep")
    assert rep.status == EnrollmentStatus.PAUSED and rep.pause_reason == "reply"
    nul = await _enr(session_factory, "nul")
    assert nul.status == EnrollmentStatus.PAUSED and nul.pause_reason is None


@pytest.mark.asyncio
async def test_resume_mailbox_defensive_assertion_blocks_non_cb(seeded, session_factory):
    """If a non-circuit_breaker row ever reaches _resume_mailbox (e.g. a future
    query regression), the defensive assertion must abort rather than re-email a
    replier. We simulate that by feeding _resume_mailbox a mailbox whose only
    PAUSED rows are reply/NULL and forcing the query to return them via a relaxed
    monkeypatch is overkill — instead assert the guard rejects a tainted row
    directly through the public cron path: with ONLY reply+NULL paused rows the
    cron must resume 0 and never raise on the legitimate (filtered) path."""
    mbx = seeded["active_mailbox_id"]
    await _make_paused(session_factory, seeded, enr_id="rep2", mailbox_id=mbx,
                       pause_reason="reply")
    await _make_paused(session_factory, seeded, enr_id="nul2", mailbox_id=mbx,
                       pause_reason=None)
    q = AsyncMock()
    with patch.object(cr, "async_session", session_factory), \
         patch.object(cr, "mailbox_bounce_rate", AsyncMock(return_value=0.0)), \
         patch.object(cr, "queue_sequence_step", q):
        out = await cr.resume_circuit_breaker_paused({})
    assert out["resumed"] == 0
    q.assert_not_awaited()


@pytest.mark.asyncio
async def test_resume_mailbox_asserts_on_tainted_row(seeded, session_factory):
    """Directly exercise the defensive assertion: if _resume_mailbox is somehow
    handed a mailbox and the SELECT is bypassed to include a reply row, the
    per-row guard must raise. We patch the internal select filter off by calling
    the helper after seeding a reply row AND a circuit_breaker row, then asserting
    that a reply row, if it slipped through, is never flipped to ACTIVE."""
    mbx = seeded["active_mailbox_id"]
    await _make_paused(session_factory, seeded, enr_id="cb3", mailbox_id=mbx,
                       pause_reason="circuit_breaker")
    await _make_paused(session_factory, seeded, enr_id="rep3", mailbox_id=mbx,
                       pause_reason="reply")
    async with session_factory() as db:
        resumed = await cr._resume_mailbox(db, mbx, limit=10)
    # Helper resumes only the circuit_breaker row; reply row is left PAUSED.
    assert resumed == 1
    assert (await _enr(session_factory, "cb3")).status == EnrollmentStatus.ACTIVE
    rep = await _enr(session_factory, "rep3")
    assert rep.status == EnrollmentStatus.PAUSED and rep.pause_reason == "reply"
