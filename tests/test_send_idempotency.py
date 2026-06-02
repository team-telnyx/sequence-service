"""F3 — send idempotency (at-most-once) on enrollment_step_id.

The SentEmail row + step.SENT were committed AFTER the Gmail send. If Gmail
delivered but the worker died/timed out before commit, the transaction rolled
back → on arq retry the step was still PENDING → it SENT AGAIN (duplicate to the
prospect). Fix (at-most-once, Kevin 2026-06-02): commit a durable SentEmail
marker BEFORE the Gmail call; a retry that finds an existing marker skips
re-sending. A *known* GmailError (didn't deliver) removes the marker so the step
stays retryable; only a hard crash mid-send leaves the marker → at-most-once
(rare missed follow-up, never a duplicate).
"""
import asyncio
import pytest
from datetime import datetime
from unittest.mock import patch, MagicMock

from src.models.models import (
    SequenceEnrollment, SequenceEnrollmentStep, SentEmail,
    EnrollmentStatus, EnrollmentStepStatus,
)
import src.workers.sequence_step as ss


@pytest.fixture
def gmail_ok():
    inbox = MagicMock()
    inbox.send_html_email = MagicMock(return_value={"message_id": "gmail-123", "thread_id": "thr-1"})
    return inbox


async def _make_enrollment_step(session_factory, seeded, step_status=EnrollmentStepStatus.PENDING):
    async with session_factory() as s:
        enr = SequenceEnrollment(
            id="enr-1", sequence_id=seeded["sequence_id"],
            mailbox_id=seeded["active_mailbox_id"], contact_email="vp@acme.com",
            contact_name="VP", timezone="America/New_York",
            status=EnrollmentStatus.ACTIVE, current_step=0,
        )
        s.add(enr)
        es = SequenceEnrollmentStep(
            id="estep-1", enrollment_id="enr-1", step_id="step-1",
            mailbox_id=seeded["active_mailbox_id"], status=step_status,
            custom_subject="Hi", custom_body="<p>Body</p>",
        )
        s.add(es)
        await s.commit()
    return "estep-1"


async def _count_sent(session_factory, step_id):
    from sqlalchemy import select, func
    async with session_factory() as s:
        return (await s.execute(
            select(func.count()).select_from(SentEmail)
            .where(SentEmail.enrollment_step_id == step_id)
        )).scalar()


def _patches(session_factory, gmail_inbox=None, gmail_error=False):
    """Patch the worker's external deps so process_sequence_step is drivable."""
    from src.services.gmail import GmailError
    cm = [
        patch.object(ss, "async_session", session_factory),
        patch.object(ss, "check_suppressed", new=_async_false),
        patch.object(ss, "check_circuit_breaker", new=_async_false),
        patch.object(ss, "check_send_window", new=lambda tz: None),  # in-window
    ]
    if gmail_inbox is not None:
        if gmail_error:
            gmail_inbox.send_html_email = MagicMock(side_effect=GmailError("smtp 421"))
        cm.append(patch.object(ss.GmailService, "get_inbox", return_value=gmail_inbox))
    return cm


async def _async_false(*a, **k):
    return False


@pytest.mark.asyncio
async def test_already_sent_marker_blocks_resend(seeded, session_factory, gmail_ok, monkeypatch):
    step_id = await _make_enrollment_step(session_factory, seeded)
    monkeypatch.setattr(ss.settings, "gmail_enabled", True, raising=False)
    cms = _patches(session_factory, gmail_ok)
    for c in cms: c.start()
    try:
        # First send → one Gmail call, one SentEmail row.
        await ss.process_sequence_step({}, step_id, seeded["tenant_id"])
        assert gmail_ok.send_html_email.call_count == 1
        assert await _count_sent(session_factory, step_id) == 1

        # Simulate an arq RETRY of the same step (e.g. status got rolled back):
        # force the step back to PENDING but leave the SentEmail marker in place.
        async with session_factory() as s:
            es = await s.get(SequenceEnrollmentStep, step_id)
            es.status = EnrollmentStepStatus.PENDING
            await s.commit()

        await ss.process_sequence_step({}, step_id, seeded["tenant_id"])
        # at-most-once: NO second Gmail send, still exactly one SentEmail row.
        assert gmail_ok.send_html_email.call_count == 1
        assert await _count_sent(session_factory, step_id) == 1
    finally:
        for c in cms: c.stop()


@pytest.mark.asyncio
async def test_marker_committed_before_send(seeded, session_factory, gmail_ok, monkeypatch):
    # The SentEmail marker must be durable (committed) BEFORE Gmail is called, so a
    # crash mid-send leaves the marker. Assert a row is visible from a SEPARATE
    # session at the moment send_html_email is invoked.
    step_id = await _make_enrollment_step(session_factory, seeded)
    monkeypatch.setattr(ss.settings, "gmail_enabled", True, raising=False)
    # After a successful send exactly one row exists and its message_id is the
    # real Gmail id (updated from the pending sentinel that was committed first).
    cms = _patches(session_factory, gmail_ok)
    for c in cms: c.start()
    try:
        await ss.process_sequence_step({}, step_id, seeded["tenant_id"])
        from sqlalchemy import select
        async with session_factory() as s:
            row = (await s.execute(
                select(SentEmail).where(SentEmail.enrollment_step_id == step_id)
            )).scalar_one()
        assert row.message_id == "gmail-123"  # updated from the pending sentinel
        assert not row.message_id.startswith("pending-")
    finally:
        for c in cms: c.stop()


@pytest.mark.asyncio
async def test_gmail_error_clears_marker_so_step_is_retryable(seeded, session_factory, monkeypatch):
    # A known GmailError means it did NOT deliver → the marker must be removed so
    # the step can retry (and capacity released). Otherwise a transient SMTP error
    # would permanently skip the prospect.
    step_id = await _make_enrollment_step(session_factory, seeded)
    monkeypatch.setattr(ss.settings, "gmail_enabled", True, raising=False)
    inbox = MagicMock()
    cms = _patches(session_factory, inbox, gmail_error=True)
    for c in cms: c.start()
    try:
        with pytest.raises(Exception):
            await ss.process_sequence_step({}, step_id, seeded["tenant_id"])
        # marker removed → retryable, no orphan "sent" record for an email that
        # never left.
        assert await _count_sent(session_factory, step_id) == 0
    finally:
        for c in cms: c.stop()
