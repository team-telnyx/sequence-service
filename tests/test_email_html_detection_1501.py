"""REVOPS-1501 — plain-text step bodies must not be sent as HTML verbatim.

``process_sequence_step`` hardcoded ``is_html=True`` when calling
``build_tracked_email``, so plain-text step bodies (25,877 of 26,000
``sent_emails`` rows) went verbatim into the HTML MIME part — every mail
client collapsed the newlines. Fix: detect HTML structurally via a tag
regex and pass ``is_html=False`` for plain-text bodies so
``build_tracked_email`` runs them through ``plain_text_to_html`` (escapes
entities, converts newlines to ``<br>``, wraps in a basic HTML structure).

Scope: ``src/workers/sequence_step.py`` + this test module only.
``src/services/email_builder.py`` is NOT modified — both of its
``build_tracked_email`` branches are already correct.
"""

import re
from unittest.mock import patch, MagicMock

import pytest

from src.config import get_settings
from src.models.models import (
    SequenceEnrollment,
    SequenceEnrollmentStep,
    EnrollmentStatus,
    EnrollmentStepStatus,
)
import src.workers.sequence_step as ss


settings = get_settings()


async def _async_false(*a, **k):
    return False


# ── _is_html_body — detection unit tests ────────────────────────────────────


def test_plain_body_not_detected_as_html():
    """The canonical Scout compose body (plain text, newlines, bare domain)
    must NOT be flagged as HTML."""
    body = (
        "Hey Marc,\n\nShort version: ...\n\n"
        "Want me to put that together?\n\n"
        "Quinn Taylor\nBusiness Development | Telnyx\ntelnyx.com"
    )
    assert ss._is_html_body(body) is False


def test_html_body_with_p_tag_detected():
    assert ss._is_html_body("<p>Hello</p>") is True


def test_html_body_with_br_tag_detected():
    assert ss._is_html_body("Line 1<br>Line 2") is True


def test_html_body_with_div_tag_detected():
    assert ss._is_html_body("<div>block</div>") is True


def test_html_body_with_a_tag_detected():
    assert ss._is_html_body('<a href="https://telnyx.com">link</a>') is True


def test_html_body_with_html_tag_detected():
    assert ss._is_html_body("<html><body>hi</body></html>") is True


def test_html_detection_case_insensitive():
    assert ss._is_html_body("<P>uppercase</P>") is True
    assert ss._is_html_body("<BR/>self-close") is True


def test_literal_less_than_in_prose_not_detected_as_html():
    """A bare '<' followed by a digit (e.g. 'latency <10ms guaranteed') must
    NOT be misdetected as HTML — '<1' does not match any of the tag names
    p/br/div/a/html followed by '>', ' ', or '/'."""
    body = "Hey Marc,\n\nOur API has latency <10ms guaranteed.\n\nQuinn"
    assert ss._is_html_body(body) is False


def test_empty_string_not_detected_as_html():
    assert ss._is_html_body("") is False


# ── send-path integration tests ─────────────────────────────────────────────
#
# Drive ``process_sequence_step`` end-to-end with a given ``custom_body`` and
# capture the ``html_body`` / ``plain_text_fallback`` kwargs passed to
# ``GmailService.send_html_email`` — the exact values the prospect receives.


async def _drive_send_with_body(session_factory, seeded, monkeypatch, custom_body):
    """Seed an enrollment whose step has ``custom_body``, run
    ``process_sequence_step``, and return the ``(html_body, plain_body)``
    kwargs passed to ``send_html_email``."""
    async with session_factory() as s:
        enr = SequenceEnrollment(
            id="enr-1501",
            sequence_id=seeded["sequence_id"],
            mailbox_id=seeded["active_mailbox_id"],
            contact_email="marc@acme.com",
            contact_name="Marc",
            timezone="America/New_York",
            status=EnrollmentStatus.ACTIVE,
            current_step=0,
        )
        s.add(enr)
        es = SequenceEnrollmentStep(
            id="estep-1501",
            enrollment_id="enr-1501",
            step_id="step-1",
            mailbox_id=seeded["active_mailbox_id"],
            status=EnrollmentStepStatus.PENDING,
            custom_subject="Quick question",
            custom_body=custom_body,
        )
        s.add(es)
        await s.commit()
    step_id = "estep-1501"

    inbox = MagicMock()
    inbox.send_html_email = MagicMock(
        return_value={"message_id": "m-1501", "thread_id": "t-1501"}
    )

    monkeypatch.setattr(ss.settings, "gmail_enabled", True, raising=False)
    cms = [
        patch.object(ss, "async_session", session_factory),
        patch.object(ss, "check_suppressed", new=_async_false),
        patch.object(ss, "check_circuit_breaker", new=_async_false),
        patch.object(ss, "check_send_window", new=lambda tz: None),
        patch.object(ss.GmailService, "get_inbox", return_value=inbox),
    ]
    for c in cms:
        c.start()
    try:
        await ss.process_sequence_step({}, step_id, seeded["tenant_id"])
    finally:
        for c in cms:
            c.stop()

    assert inbox.send_html_email.call_count == 1, "send was not invoked"
    kwargs = inbox.send_html_email.call_args.kwargs
    return kwargs["html_body"], kwargs["plain_text_fallback"]


@pytest.mark.asyncio
async def test_plain_body_sent_as_html_with_br_and_footer(
    seeded, session_factory, monkeypatch
):
    """Plain-text body → html_body contains <br> line breaks and escaped text;
    plain part preserved with compliance footer."""
    plain_body_text = (
        "Hey Marc,\n\nShort version: ...\n\n"
        "Want me to put that together?\n\n"
        "Quinn Taylor\nBusiness Development | Telnyx\nhttps://telnyx.com"
    )
    html_body, plain_body = await _drive_send_with_body(
        session_factory, seeded, monkeypatch, plain_body_text
    )

    # Newlines converted to <br> — the core fix (was verbatim, now structured)
    assert "<br>" in html_body, (
        "plain body was not converted to HTML: no <br> found — "
        "is_html=True was likely still hardcoded"
    )

    # Body text present in the HTML part (escaped, not collapsed)
    assert "Hey Marc," in html_body
    # The URL must be autolinked into a real anchor. Uses https:// (the
    # builder's autolink regex only matches https?://, so a bare-domain
    # assertion would pass trivially with no anchor). The href is tracking-
    # wrapped, so match the anchor by its visible text, not the href.
    assert re.search(r"<a [^>]*>https://telnyx\.com</a>", html_body), (
        "https://telnyx.com was not autolinked into an anchor in the HTML part"
    )

    # Plain part preserved verbatim + compliance footer appended
    assert "Hey Marc," in plain_body
    assert "Quinn Taylor" in plain_body
    assert "Unsubscribe:" in plain_body
    assert settings.physical_address in plain_body


@pytest.mark.asyncio
async def test_genuine_html_body_passed_through_verbatim(
    seeded, session_factory, monkeypatch
):
    """Genuine HTML body (<p>...</p>) → passed through to html_body verbatim,
    exactly as today (is_html=True branch)."""
    html_body_input = "<p>Hello Marc,</p><p>Short version: ...</p>"
    html_body, plain_body = await _drive_send_with_body(
        session_factory, seeded, monkeypatch, html_body_input
    )

    # The original HTML appears verbatim in the output (is_html=True passes
    # the body straight through; only tracking/footer are appended).
    assert "<p>Hello Marc,</p><p>Short version: ...</p>" in html_body

    # The plain part is the html_to_plain_text rendering of the input (not
    # the raw input, which was HTML — proves is_html=True was taken).
    assert "Hello Marc" in plain_body
    assert "Short version" in plain_body


@pytest.mark.asyncio
async def test_literal_less_than_in_prose_escaped_not_misdetected(
    seeded, session_factory, monkeypatch
):
    """Plain body containing a literal '<' in prose ('latency <10ms
    guaranteed') must NOT be misdetected as HTML — the '<' must be escaped
    as &lt; in the HTML part, and newlines must still become <br>."""
    plain_body_with_lt = "Hey Marc,\n\nOur API has latency <10ms guaranteed.\n\nQuinn"
    html_body, plain_body = await _drive_send_with_body(
        session_factory, seeded, monkeypatch, plain_body_with_lt
    )

    # Went through plain_text_to_html (newlines → <br>), NOT the is_html=True
    # verbatim path (which would have left newlines as-is).
    assert "<br>" in html_body, (
        "body with '<10ms' was misdetected as HTML — no <br> found, "
        "meaning is_html=True was taken and the body went verbatim"
    )

    # The literal '<' is escaped as &lt; in the HTML part
    assert "&lt;10ms" in html_body, (
        "literal '<' was not escaped to &lt; in the HTML part"
    )

    # No bare unescaped '<10ms' survived into the HTML part
    assert "<10ms" not in html_body, (
        "bare '<10ms' found in html_body — '<' was not escaped"
    )

    # Plain part keeps the literal '<' as-is (plain text doesn't escape)
    assert "<10ms" in plain_body
