"""Email-to-Salesforce BCC (Kevin 2026-07-10).

Every outbound send is BCC'd to the org's Email-to-Salesforce service address so
SFDC auto-logs a completed Task on the matching contact/lead — replacing the
manual post-send task entry. The address lives in settings
(salesforce_bcc_address); blank disables the BCC entirely, and the header must
never leak into the visible To/Cc of the prospect's copy (standard Bcc
semantics: header present in the raw submission, stripped by Gmail on delivery).
"""
import pytest
from unittest.mock import patch, MagicMock

from src.models.models import (
    SequenceEnrollment, SequenceEnrollmentStep,
    EnrollmentStatus, EnrollmentStepStatus,
)
from src.services.gmail import GmailService
import src.workers.sequence_step as ss


SFDC_BCC = "emailtosalesforce@a-33gccorss1hb49mgd2oqu29yotdtrci512h16g9oae45z3evgp.j-1nifjeaq.usa416.le.salesforce.com"


# ---------------------------------------------------------------------------
# GmailService: bcc plumbs into the RFC-822 message
# ---------------------------------------------------------------------------

def test_build_message_sets_bcc_header():
    svc = GmailService(inbox="quinn.c@telnyx.com")
    msg = svc._build_email_message(
        to="vp@acme.com", subject="Hi", body="<p>Body</p>", is_html=True,
        bcc=SFDC_BCC,
    )
    assert msg["Bcc"] == SFDC_BCC
    assert msg["To"] == "vp@acme.com"


def test_build_message_no_bcc_by_default():
    svc = GmailService(inbox="quinn.c@telnyx.com")
    msg = svc._build_email_message(
        to="vp@acme.com", subject="Hi", body="Body",
    )
    assert msg["Bcc"] is None


def test_send_html_email_passes_bcc_through():
    svc = GmailService(inbox="quinn.c@telnyx.com")
    with patch.object(svc, "_send_message", return_value={"message_id": "m", "thread_id": "t"}) as send:
        svc.send_html_email(
            to="vp@acme.com", subject="Hi", html_body="<p>Body</p>", bcc=SFDC_BCC,
        )
    msg = send.call_args[0][0]
    assert msg["Bcc"] == SFDC_BCC


# ---------------------------------------------------------------------------
# Worker: settings.salesforce_bcc_address is applied on every send
# ---------------------------------------------------------------------------

async def _async_false(*a, **k):
    return False


async def _make_enrollment_step(session_factory, seeded):
    async with session_factory() as s:
        enr = SequenceEnrollment(
            id="enr-bcc", sequence_id=seeded["sequence_id"],
            mailbox_id=seeded["active_mailbox_id"], contact_email="vp@acme.com",
            contact_name="VP", timezone="America/New_York",
            status=EnrollmentStatus.ACTIVE, current_step=0,
        )
        s.add(enr)
        es = SequenceEnrollmentStep(
            id="estep-bcc", enrollment_id="enr-bcc", step_id="step-1",
            mailbox_id=seeded["active_mailbox_id"],
            status=EnrollmentStepStatus.PENDING,
            custom_subject="Hi", custom_body="<p>Body</p>",
        )
        s.add(es)
        await s.commit()
    return "estep-bcc"


def _patches(session_factory, gmail_inbox):
    return [
        patch.object(ss, "async_session", session_factory),
        patch.object(ss, "check_suppressed", new=_async_false),
        patch.object(ss, "check_circuit_breaker", new=_async_false),
        patch.object(ss, "check_send_window", new=lambda tz: None),
        patch.object(ss.GmailService, "get_inbox", return_value=gmail_inbox),
    ]


@pytest.mark.asyncio
async def test_worker_sends_with_salesforce_bcc(seeded, session_factory):
    step_id = await _make_enrollment_step(session_factory, seeded)
    inbox = MagicMock()
    inbox.send_html_email = MagicMock(return_value={"message_id": "g-1", "thread_id": "t-1"})

    patches = _patches(session_factory, inbox)
    with patches[0], patches[1], patches[2], patches[3], patches[4], \
         patch.object(ss.settings, "gmail_enabled", True), \
         patch.object(ss.settings, "salesforce_bcc_address", SFDC_BCC):
        await ss.process_sequence_step({}, step_id, seeded["tenant_id"])

    assert inbox.send_html_email.call_count == 1
    assert inbox.send_html_email.call_args.kwargs["bcc"] == SFDC_BCC


@pytest.mark.asyncio
async def test_worker_omits_bcc_when_unset(seeded, session_factory):
    step_id = await _make_enrollment_step(session_factory, seeded)
    inbox = MagicMock()
    inbox.send_html_email = MagicMock(return_value={"message_id": "g-2", "thread_id": "t-2"})

    patches = _patches(session_factory, inbox)
    with patches[0], patches[1], patches[2], patches[3], patches[4], \
         patch.object(ss.settings, "gmail_enabled", True), \
         patch.object(ss.settings, "salesforce_bcc_address", ""):
        await ss.process_sequence_step({}, step_id, seeded["tenant_id"])

    assert inbox.send_html_email.call_count == 1
    assert inbox.send_html_email.call_args.kwargs["bcc"] is None
