"""Send-side blank-email guard (audit C2 / REVOPS-886).

The Old/New-ICP step templates store '{{subject}}'/'{{body}}'. When Scout content
is absent the worker falls back to render_email on these placeholders, which (with
no matching context var) renders to '' → a blank email was sent to real prospects.
The worker now refuses to send when the resolved subject OR body is blank.
"""
from src.workers.sequence_step import _blank_content
from src.services.template import render_email


# ── _blank_content ──────────────────────────────────────────────────────────

def test_blank_when_subject_empty():
    assert _blank_content("", "real body") is True


def test_blank_when_body_empty():
    assert _blank_content("Real subject", "") is True


def test_blank_when_both_empty():
    assert _blank_content("", "") is True


def test_blank_when_none():
    assert _blank_content(None, None) is True


def test_blank_when_whitespace_only():
    assert _blank_content("   ", "\n\t ") is True


def test_not_blank_with_real_content():
    assert _blank_content("Quick question", "Hi there, ...") is False


# ── the real-world trigger: placeholder template renders blank ───────────────

def test_placeholder_template_renders_blank_and_is_caught():
    # exactly the Old/New-ICP step rows: '{{subject}}'/'{{body}}' with no Scout
    # content in context → renders to empty → must be flagged blank.
    subject, body = render_email("{{subject}}", "{{body}}",
                                 contact_name="Jane", contact_email="jane@acme.com")
    assert subject == "" and body == ""
    assert _blank_content(subject, body) is True
