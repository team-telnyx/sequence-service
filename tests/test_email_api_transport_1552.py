"""REVOPS-1552 — Telnyx Email API transport adapter + per-mailbox selection.

TDD tests written FIRST (fail before implementation). Covers:
  - send_at matrix: past rejected pre-HTTP; future asserts status 'scheduled';
    absent sends immediately on 202.
  - error mapping: 4xx permanent, 5xx/timeout retryable, 429 reputation throttle.
  - transport selection: gmail rows take Gmail path; email_api rows take new path.
  - migration/model: default backfill leaves every mailbox on 'gmail'.
  - payload builder: explicit fields only (no passthrough dict); list_unsubscribe
    custom headers; one-click adds the -Post header.
  - one live sandbox smoke test GATED behind env flag (skipped by default).

Hermetic by default — httpx.MockTransport intercepts all HTTP. No live sends.
"""

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, MagicMock

import httpx
import pytest
from arq.worker import Retry as ArqRetry

from src.models.models import (
    Mailbox,
    SequenceEnrollment,
    SequenceEnrollmentStep,
    EnrollmentStatus,
    EnrollmentStepStatus,
)
import src.workers.sequence_step as ss


# ── helpers ──────────────────────────────────────────────────────────────────


def _mock_client(handler, timeout=30.0):
    """httpx.AsyncClient wired to a MockTransport so no real HTTP is made."""
    return httpx.AsyncClient(transport=httpx.MockTransport(handler), timeout=timeout)


def _resp(status_code, json_body=None, text=None, headers=None):
    """Build an httpx.Response."""
    return httpx.Response(
        status_code,
        json=json_body,
        text=text,
        headers=headers,
        request=httpx.Request("POST", "https://api.telnyx.com/v2/emails"),
    )


def _ok_response(status="queued", msg_id="email-uuid-123"):
    return _resp(202, json_body={"data": {"id": msg_id, "status": status}})


def _scheduled_response(msg_id="email-uuid-sched"):
    return _resp(202, json_body={"data": {"id": msg_id, "status": "scheduled"}})


async def _async_false(*a, **k):
    return False


# Save the real httpx.AsyncClient BEFORE any patching so the mock-client
# factory can call it without infinite recursion (P2-B: mock ONLY the HTTP
# layer, never the transport itself).
_REAL_ASYNC_CLIENT = httpx.AsyncClient


def _real_transport_with_mock_http(handler, timeout=30.0):
    """Return patchers that wire a REAL EmailAPITransport (payload builder +
    error mapping + send_at guard all run under test) but intercept HTTP via
    httpx.MockTransport. P2-B: mock ONLY the HTTP layer, not the transport.

    Returns a list of unittest.mock patchers. Start/stop all together::

        patchers = _real_transport_with_mock_http(handler)
        for p in patchers: p.start()
        try: ...
        finally:
            for p in patchers: p.stop()
    """
    from src.services.email_api import EmailAPITransport
    import src.services.email_api as email_api_mod

    real_transport = EmailAPITransport(
        api_key="test-key-not-a-secret",
        base_url="https://api.telnyx.com/v2",
        timeout=timeout,
    )

    def _client_factory(*args, **kwargs):
        t = kwargs.get("timeout", timeout)
        return _REAL_ASYNC_CLIENT(
            transport=httpx.MockTransport(handler),
            timeout=t,
        )

    return [
        patch.object(EmailAPITransport, "_instance", real_transport),
        patch.object(email_api_mod.httpx, "AsyncClient", _client_factory),
    ]


# ── payload builder ──────────────────────────────────────────────────────────


class TestPayloadBuilder:
    def _make_transport(self):
        from src.services.email_api import EmailAPITransport

        return EmailAPITransport(
            api_key="test-key-not-a-secret", base_url="https://api.telnyx.com/v2"
        )

    def test_explicit_fields_only_no_passthrough(self):
        """Payload must contain ONLY the explicitly-built fields — never
        arbitrary dict passthrough (the API accepts unknown fields silently)."""
        t = self._make_transport()
        payload = t.build_payload(
            from_email="quinn.c@telnyx.com",
            to="prospect@acme.com",
            subject="Hi",
            html_body="<p>Hi</p>",
        )
        # Only the fields we explicitly set; no stray keys.
        expected_keys = {"from", "to", "subject", "html_body"}
        assert set(payload.keys()) == expected_keys, (
            f"unexpected payload keys: {set(payload.keys()) - expected_keys}"
        )

    def test_from_to_subject_html_body_present(self):
        t = self._make_transport()
        payload = t.build_payload(
            from_email="quinn.c@telnyx.com",
            to="prospect@acme.com",
            subject="Hello",
            html_body="<p>Hello</p>",
        )
        assert payload["from"] == "quinn.c@telnyx.com"
        assert payload["to"] == ["prospect@acme.com"]
        assert payload["subject"] == "Hello"
        assert payload["html_body"] == "<p>Hello</p>"

    def test_sender_name_added_as_from_name(self):
        t = self._make_transport()
        payload = t.build_payload(
            from_email="quinn.c@telnyx.com",
            to="p@acme.com",
            subject="S",
            html_body="<p>x</p>",
            sender_name="Quinn Taylor",
        )
        assert payload["from_name"] == "Quinn Taylor"

    def test_plain_body_optional_text_body(self):
        t = self._make_transport()
        payload = t.build_payload(
            from_email="q@telnyx.com",
            to="p@acme.com",
            subject="S",
            html_body="<p>x</p>",
            plain_body="plain text",
        )
        assert payload["text_body"] == "plain text"

    def test_cc_bcc_split_on_comma(self):
        t = self._make_transport()
        payload = t.build_payload(
            from_email="q@telnyx.com",
            to="p@acme.com",
            subject="S",
            html_body="<p>x</p>",
            cc="a@x.com, b@x.com",
            bcc="c@x.com,d@x.com",
        )
        assert payload["cc"] == ["a@x.com", "b@x.com"]
        assert payload["bcc"] == ["c@x.com", "d@x.com"]

    def test_list_unsubscribe_added_as_custom_header(self):
        t = self._make_transport()
        payload = t.build_payload(
            from_email="q@telnyx.com",
            to="p@acme.com",
            subject="S",
            html_body="<p>x</p>",
            list_unsubscribe="<mailto:unsub@telnyx.com>",
            one_click=False,
        )
        assert payload["headers"]["List-Unsubscribe"] == "<mailto:unsub@telnyx.com>"
        assert "List-Unsubscribe-Post" not in payload["headers"]

    def test_one_click_adds_unsubscribe_post_header(self):
        t = self._make_transport()
        payload = t.build_payload(
            from_email="q@telnyx.com",
            to="p@acme.com",
            subject="S",
            html_body="<p>x</p>",
            list_unsubscribe="<https://track.telnyx.com/u/123>",
            one_click=True,
        )
        assert (
            payload["headers"]["List-Unsubscribe-Post"] == "List-Unsubscribe=One-Click"
        )

    def test_future_send_at_added_as_scheduled_at(self):
        t = self._make_transport()
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        payload = t.build_payload(
            from_email="q@telnyx.com",
            to="p@acme.com",
            subject="S",
            html_body="<p>x</p>",
            send_at=future,
        )
        assert "scheduled_at" in payload
        assert payload["scheduled_at"] == future.isoformat()

    def test_naive_send_at_treated_as_utc(self):
        """Naive datetimes (the mailboxes/step scheduled_at convention) are
        treated as UTC, matching the repo's naive-UTC column convention."""
        t = self._make_transport()
        naive_future = datetime.utcnow() + timedelta(hours=1)
        payload = t.build_payload(
            from_email="q@telnyx.com",
            to="p@acme.com",
            subject="S",
            html_body="<p>x</p>",
            send_at=naive_future,
        )
        assert "scheduled_at" in payload

    def test_no_scheduled_at_when_send_at_none(self):
        t = self._make_transport()
        payload = t.build_payload(
            from_email="q@telnyx.com",
            to="p@acme.com",
            subject="S",
            html_body="<p>x</p>",
            send_at=None,
        )
        assert "scheduled_at" not in payload

    def test_sandbox_mode_added_to_payload(self):
        """sandbox_mode=True → payload includes sandbox_mode: True so the API
        accepts the message but does NOT deliver it (P2-A: safe smoke test)."""
        t = self._make_transport()
        payload = t.build_payload(
            from_email="q@telnyx.com",
            to="p@acme.com",
            subject="S",
            html_body="<p>x</p>",
            sandbox_mode=True,
        )
        assert payload.get("sandbox_mode") is True

    def test_sandbox_mode_off_by_default(self):
        t = self._make_transport()
        payload = t.build_payload(
            from_email="q@telnyx.com",
            to="p@acme.com",
            subject="S",
            html_body="<p>x</p>",
        )
        assert "sandbox_mode" not in payload, (
            "sandbox_mode should be omitted when not explicitly set"
        )


# ── send_at guard matrix ─────────────────────────────────────────────────────


class TestSendAtGuard:
    def _make_transport(self):
        from src.services.email_api import EmailAPITransport

        return EmailAPITransport(
            api_key="test-key-not-a-secret", base_url="https://api.telnyx.com/v2"
        )

    @pytest.mark.asyncio
    async def test_past_send_at_rejected_before_http_call(self):
        """A past send_at must raise BEFORE any HTTP request is made — the
        API silently sends immediately for past values (the bug we guard)."""
        from src.services.email_api import EmailAPIConfigError

        t = self._make_transport()
        past = datetime.now(timezone.utc) - timedelta(minutes=5)
        call_count = 0

        def handler(req):
            nonlocal call_count
            call_count += 1
            return _ok_response()

        client = _mock_client(handler)
        with pytest.raises(EmailAPIConfigError):
            await t.send_html_email(
                from_email="q@telnyx.com",
                to="p@acme.com",
                subject="S",
                html_body="<p>x</p>",
                send_at=past,
                _client=client,
            )
        assert call_count == 0, "HTTP call was made despite past send_at — guard failed"
        await client.aclose()

    @pytest.mark.asyncio
    async def test_future_send_at_must_return_status_scheduled(self):
        """Future send_at → payload contains scheduled_at AND a non-'scheduled'
        response status raises (prevents silent immediate send)."""
        from src.services.email_api import EmailAPIError

        t = self._make_transport()
        future = datetime.now(timezone.utc) + timedelta(hours=1)

        # API accepted 202 but returned status 'queued' (sent immediately)
        # instead of 'scheduled' — must raise.
        def handler(req):
            return _resp(202, json_body={"data": {"id": "x", "status": "queued"}})

        client = _mock_client(handler)
        with pytest.raises(EmailAPIError):
            await t.send_html_email(
                from_email="q@telnyx.com",
                to="p@acme.com",
                subject="S",
                html_body="<p>x</p>",
                send_at=future,
                _client=client,
            )
        await client.aclose()

    @pytest.mark.asyncio
    async def test_future_send_at_with_scheduled_status_succeeds(self):
        t = self._make_transport()
        future = datetime.now(timezone.utc) + timedelta(hours=1)

        captured = {}

        def handler(req):
            import json

            body = json.loads(req.content)
            captured["scheduled_at"] = body.get("scheduled_at")
            return _scheduled_response(msg_id="sched-uuid")

        client = _mock_client(handler)
        result = await t.send_html_email(
            from_email="q@telnyx.com",
            to="p@acme.com",
            subject="S",
            html_body="<p>x</p>",
            send_at=future,
            _client=client,
        )
        assert result["message_id"] == "sched-uuid"
        assert result["status"] == "scheduled"
        assert captured["scheduled_at"] is not None, "scheduled_at missing from payload"
        await client.aclose()

    @pytest.mark.asyncio
    async def test_absent_send_at_immediate_send_accepted_on_202(self):
        t = self._make_transport()

        def handler(req):
            return _ok_response(status="queued", msg_id="imm-uuid")

        client = _mock_client(handler)
        result = await t.send_html_email(
            from_email="q@telnyx.com",
            to="p@acme.com",
            subject="S",
            html_body="<p>x</p>",
            _client=client,
        )
        assert result["message_id"] == "imm-uuid"
        assert result["status"] == "queued"
        await client.aclose()

    @pytest.mark.asyncio
    async def test_send_at_exactly_now_rejected(self):
        """send_at <= now must be rejected (boundary: now is in the past by
        the time the HTTP call lands)."""
        from src.services.email_api import EmailAPIConfigError

        t = self._make_transport()
        now = datetime.now(timezone.utc)
        client = _mock_client(lambda r: _ok_response())
        with pytest.raises(EmailAPIConfigError):
            await t.send_html_email(
                from_email="q@telnyx.com",
                to="p@acme.com",
                subject="S",
                html_body="<p>x</p>",
                send_at=now,
                _client=client,
            )
        await client.aclose()


# ── error mapping ────────────────────────────────────────────────────────────


class TestErrorMapping:
    def _make_transport(self):
        from src.services.email_api import EmailAPITransport

        return EmailAPITransport(
            api_key="test-key-not-a-secret", base_url="https://api.telnyx.com/v2"
        )

    @pytest.mark.asyncio
    async def test_4xx_permanent_not_retryable(self):
        from src.services.email_api import EmailAPIPermanentError

        t = self._make_transport()
        client = _mock_client(
            lambda r: _resp(
                422, json_body={"errors": [{"code": "10027", "detail": "bad"}]}
            )
        )
        with pytest.raises(EmailAPIPermanentError) as exc_info:
            await t.send_html_email(
                from_email="q@telnyx.com",
                to="p@acme.com",
                subject="S",
                html_body="<p>x</p>",
                _client=client,
            )
        assert exc_info.value.retryable is False
        assert exc_info.value.status_code == 422
        await client.aclose()

    @pytest.mark.asyncio
    async def test_5xx_retryable(self):
        from src.services.email_api import EmailAPIRetryableError

        t = self._make_transport()
        client = _mock_client(
            lambda r: _resp(
                503, json_body={"errors": [{"code": "10016", "detail": "down"}]}
            )
        )
        with pytest.raises(EmailAPIRetryableError) as exc_info:
            await t.send_html_email(
                from_email="q@telnyx.com",
                to="p@acme.com",
                subject="S",
                html_body="<p>x</p>",
                _client=client,
            )
        assert exc_info.value.retryable is True
        assert exc_info.value.status_code == 503
        await client.aclose()

    @pytest.mark.asyncio
    async def test_429_reputation_throttle_retryable(self):
        from src.services.email_api import EmailAPIReputationError

        t = self._make_transport()
        client = _mock_client(
            lambda r: _resp(
                429,
                json_body={
                    "errors": [{"code": "reputation_suspended", "detail": "poor"}]
                },
            )
        )
        with pytest.raises(EmailAPIReputationError) as exc_info:
            await t.send_html_email(
                from_email="q@telnyx.com",
                to="p@acme.com",
                subject="S",
                html_body="<p>x</p>",
                _client=client,
            )
        assert exc_info.value.retryable is True
        assert exc_info.value.status_code == 429
        await client.aclose()

    @pytest.mark.asyncio
    async def test_timeout_retryable(self):
        from src.services.email_api import EmailAPIRetryableError

        def handler(req):
            raise httpx.TimeoutException("simulated timeout")

        t = self._make_transport()
        client = _mock_client(handler, timeout=0.5)
        with pytest.raises(EmailAPIRetryableError) as exc_info:
            await t.send_html_email(
                from_email="q@telnyx.com",
                to="p@acme.com",
                subject="S",
                html_body="<p>x</p>",
                _client=client,
            )
        assert exc_info.value.retryable is True
        await client.aclose()

    @pytest.mark.asyncio
    async def test_400_permanent(self):
        from src.services.email_api import EmailAPIPermanentError

        t = self._make_transport()
        client = _mock_client(
            lambda r: _resp(
                400,
                json_body={"errors": [{"code": "10015", "detail": "bad idempotency"}]},
            )
        )
        with pytest.raises(EmailAPIPermanentError):
            await t.send_html_email(
                from_email="q@telnyx.com",
                to="p@acme.com",
                subject="S",
                html_body="<p>x</p>",
                _client=client,
            )
        await client.aclose()

    @pytest.mark.asyncio
    async def test_429_captures_retry_after_header(self):
        """429 with Retry-After header → EmailAPIReputationError.retry_after
        is parsed (seconds) so the worker can honor it as the Retry defer."""
        from src.services.email_api import EmailAPIReputationError

        t = self._make_transport()
        client = _mock_client(
            lambda r: _resp(
                429,
                json_body={
                    "errors": [{"code": "reputation_suspended", "detail": "poor"}]
                },
                headers={"Retry-After": "120"},
            )
        )
        with pytest.raises(EmailAPIReputationError) as exc_info:
            await t.send_html_email(
                from_email="q@telnyx.com",
                to="p@acme.com",
                subject="S",
                html_body="<p>x</p>",
                _client=client,
            )
        assert exc_info.value.retryable is True
        assert exc_info.value.retry_after == 120, (
            f"retry_after should be 120 (from Retry-After header), got {exc_info.value.retry_after!r}"
        )
        await client.aclose()

    @pytest.mark.asyncio
    async def test_429_no_retry_after_header_is_none(self):
        """429 without Retry-After → retry_after is None (worker uses exponential)."""
        from src.services.email_api import EmailAPIReputationError

        t = self._make_transport()
        client = _mock_client(
            lambda r: _resp(
                429,
                json_body={
                    "errors": [{"code": "reputation_suspended", "detail": "poor"}]
                },
            )
        )
        with pytest.raises(EmailAPIReputationError) as exc_info:
            await t.send_html_email(
                from_email="q@telnyx.com",
                to="p@acme.com",
                subject="S",
                html_body="<p>x</p>",
                _client=client,
            )
        assert exc_info.value.retry_after is None
        await client.aclose()


# ── config ───────────────────────────────────────────────────────────────────


class TestConfig:
    def test_missing_api_key_raises_config_error(self, monkeypatch):
        from src.services.email_api import EmailAPITransport, EmailAPIConfigError

        monkeypatch.delenv("EMAIL_API_KEY", raising=False)
        with pytest.raises(EmailAPIConfigError):
            EmailAPITransport()

    def test_base_url_configurable(self, monkeypatch):
        from src.services.email_api import EmailAPITransport

        monkeypatch.setenv("EMAIL_API_KEY", "test-key")
        t = EmailAPITransport(base_url="https://staging.telnyx.com/v2")
        assert t.base_url == "https://staging.telnyx.com/v2"

    def test_timeout_configurable(self, monkeypatch):
        from src.services.email_api import EmailAPITransport

        monkeypatch.setenv("EMAIL_API_KEY", "test-key")
        t = EmailAPITransport(timeout=10.0)
        assert t.timeout == 10.0

    def test_default_base_url(self, monkeypatch):
        from src.services.email_api import EmailAPITransport

        monkeypatch.setenv("EMAIL_API_KEY", "test-key")
        t = EmailAPITransport()
        assert t.base_url == "https://api.telnyx.com/v2"


# ── migration / model default ───────────────────────────────────────────────


class TestMigrationDefault:
    async def test_new_mailbox_defaults_to_gmail_transport(
        self, session_factory, seeded
    ):
        """A Mailbox created without specifying transport must default to
        'gmail' — the backfill guarantee (every existing mailbox stays Gmail)."""
        async with session_factory() as s:
            mb = await s.get(Mailbox, seeded["active_mailbox_id"])
            assert mb.transport == "gmail", (
                "new mailbox did not default to gmail transport — "
                "migration backfill invariant broken"
            )

    async def test_transport_can_be_set_to_email_api(self, session_factory, seeded):
        async with session_factory() as s:
            mb = await s.get(Mailbox, seeded["active_mailbox_id"])
            mb.transport = "email_api"
            await s.commit()
            mb2 = await s.get(Mailbox, seeded["active_mailbox_id"])
            assert mb2.transport == "email_api"

    async def test_all_seeded_mailboxes_default_gmail(self, session_factory, seeded):
        """Every mailbox in the seeded fixture (which mirrors the live Scout
        pool) must be 'gmail' by default — no mailbox flips without an
        explicit DB change."""
        async with session_factory() as s:
            from sqlalchemy import select

            result = await s.execute(select(Mailbox))
            for mb in result.scalars().all():
                assert mb.transport == "gmail", (
                    f"mailbox {mb.email} defaulted to {mb.transport!r}, not gmail"
                )


# ── transport selection at the dispatch point ────────────────────────────────


async def _make_enrollment_step(
    session_factory, seeded, mailbox_id, step_status=EnrollmentStepStatus.PENDING
):
    async with session_factory() as s:
        enr = SequenceEnrollment(
            id="enr-1552",
            sequence_id=seeded["sequence_id"],
            mailbox_id=mailbox_id,
            contact_email="vp@acme.com",
            contact_name="VP",
            timezone="America/New_York",
            status=EnrollmentStatus.ACTIVE,
            current_step=0,
        )
        s.add(enr)
        es = SequenceEnrollmentStep(
            id="estep-1552",
            enrollment_id="enr-1552",
            step_id="step-1",
            mailbox_id=mailbox_id,
            status=step_status,
            custom_subject="Hi",
            custom_body="<p>Body</p>",
        )
        s.add(es)
        await s.commit()
    return "estep-1552"


def _worker_patches(session_factory):
    return [
        patch.object(ss, "async_session", session_factory),
        patch.object(ss, "check_suppressed", new=_async_false),
        patch.object(ss, "check_circuit_breaker", new=_async_false),
        patch.object(ss, "check_send_window", new=lambda tz: None),
    ]


class TestTransportSelection:
    """P2-B: dispatch-path tests mock ONLY the HTTP layer (httpx.MockTransport)
    so the real EmailAPITransport payload builder + error mapping run under
    test. The Gmail path still mocks GmailService (Gmail is not the transport
    under test here)."""

    @pytest.mark.asyncio
    async def test_gmail_transport_takes_gmail_path_unchanged(
        self, seeded, session_factory, monkeypatch
    ):
        """transport='gmail' → GmailService.send_html_email called (existing
        path, byte-identical behavior). Email API NOT touched."""
        step_id = await _make_enrollment_step(
            session_factory, seeded, seeded["active_mailbox_id"]
        )
        monkeypatch.setattr(ss.settings, "gmail_enabled", True, raising=False)

        gmail_inbox = MagicMock()
        gmail_inbox.send_html_email = MagicMock(
            return_value={"message_id": "gmail-xyz", "thread_id": "thr-1"}
        )

        # HTTP handler that FAILS if the Email API path is taken — proves the
        # gmail branch was selected.
        def _email_http_should_not_fire(request):
            raise AssertionError("Email API HTTP called for a gmail mailbox")

        cms = _worker_patches(session_factory)
        cms.append(patch.object(ss.GmailService, "get_inbox", return_value=gmail_inbox))
        cms.extend(_real_transport_with_mock_http(_email_http_should_not_fire))
        for c in cms:
            c.start()
        try:
            await ss.process_sequence_step({}, step_id, seeded["tenant_id"])
            assert gmail_inbox.send_html_email.call_count == 1, (
                "Gmail path not taken for transport=gmail"
            )
        finally:
            for c in cms:
                c.stop()

    @pytest.mark.asyncio
    async def test_email_api_transport_takes_api_path(
        self, seeded, session_factory, monkeypatch
    ):
        """transport='email_api' → real EmailAPITransport.send_html_email runs
        (P2-B: only HTTP mocked) and the HTTP POST is made. Gmail NOT touched."""
        async with session_factory() as s:
            mb = await s.get(Mailbox, seeded["active_mailbox_id"])
            mb.transport = "email_api"
            await s.commit()

        step_id = await _make_enrollment_step(
            session_factory, seeded, seeded["active_mailbox_id"]
        )
        monkeypatch.setattr(ss.settings, "gmail_enabled", True, raising=False)

        http_calls = []

        def handler(req):
            http_calls.append(req)
            return _ok_response(status="queued", msg_id="api-uuid-from-http")

        gmail_inbox = MagicMock()
        gmail_inbox.send_html_email = MagicMock(
            side_effect=AssertionError("Gmail called for an email_api mailbox")
        )

        cms = _worker_patches(session_factory)
        cms.append(patch.object(ss.GmailService, "get_inbox", return_value=gmail_inbox))
        cms.extend(_real_transport_with_mock_http(handler))
        for c in cms:
            c.start()
        try:
            await ss.process_sequence_step({}, step_id, seeded["tenant_id"])
            assert len(http_calls) == 1, (
                f"Email API HTTP not called exactly once (got {len(http_calls)})"
            )
            assert gmail_inbox.send_html_email.call_count == 0, (
                "Gmail was called for an email_api mailbox — selection broken"
            )
        finally:
            for c in cms:
                c.stop()

    @pytest.mark.asyncio
    async def test_email_api_send_success_updates_sent_email_message_id(
        self, seeded, session_factory, monkeypatch
    ):
        """Successful Email API send (real transport, HTTP mocked) updates
        SentEmail.message_id with the Telnyx UUID from the HTTP response."""
        from sqlalchemy import select

        async with session_factory() as s:
            mb = await s.get(Mailbox, seeded["active_mailbox_id"])
            mb.transport = "email_api"
            await s.commit()

        step_id = await _make_enrollment_step(
            session_factory, seeded, seeded["active_mailbox_id"]
        )
        monkeypatch.setattr(ss.settings, "gmail_enabled", True, raising=False)

        def handler(req):
            return _ok_response(status="queued", msg_id="telnyx-uuid-abc")

        cms = _worker_patches(session_factory)
        cms.extend(_real_transport_with_mock_http(handler))
        for c in cms:
            c.start()
        try:
            await ss.process_sequence_step({}, step_id, seeded["tenant_id"])
            async with session_factory() as s:
                row = (
                    await s.execute(
                        select(ss.SentEmail).where(
                            ss.SentEmail.enrollment_step_id == step_id
                        )
                    )
                ).scalar_one()
            assert row.message_id == "telnyx-uuid-abc"
            assert not row.message_id.startswith("pending-")
        finally:
            for c in cms:
                c.stop()


# ── P1-A: retry semantics — retryable adapter errors → arq.worker.Retry ──────


class TestRetrySemantics:
    """P1-A: retryable adapter errors (429/5xx/timeout) must raise
    arq.worker.Retry(defer=...) so ARQ actually re-enqueues with backoff.
    Permanent 4xx stays a terminal RuntimeError (the Gmail contract).

    All tests mock ONLY the HTTP layer (P2-B) so the real payload builder +
    error mapping + the worker's Retry/RuntimeError dispatch run under test.
    """

    @pytest.mark.asyncio
    async def test_429_raises_arq_retry_with_deferral(
        self, seeded, session_factory, monkeypatch
    ):
        """429 → arq.worker.Retry raised with a positive deferral (not
        RuntimeError). The marker is removed and capacity released so the
        retry's idempotency pre-check doesn't skip the re-send."""
        from sqlalchemy import select, func

        async with session_factory() as s:
            mb = await s.get(Mailbox, seeded["active_mailbox_id"])
            mb.transport = "email_api"
            await s.commit()

        step_id = await _make_enrollment_step(
            session_factory, seeded, seeded["active_mailbox_id"]
        )
        monkeypatch.setattr(ss.settings, "gmail_enabled", True, raising=False)

        release_calls = []
        original_release = ss.release_send

        async def _tracking_release(db, mailbox_id):
            release_calls.append(mailbox_id)
            return await original_release(db, mailbox_id)

        def handler(req):
            return _resp(
                429,
                json_body={
                    "errors": [{"code": "reputation_suspended", "detail": "poor"}]
                },
            )

        cms = _worker_patches(session_factory)
        cms.extend(_real_transport_with_mock_http(handler))
        cms.append(patch.object(ss, "release_send", side_effect=_tracking_release))
        for c in cms:
            c.start()
        try:
            with pytest.raises(ArqRetry) as exc_info:
                await ss.process_sequence_step({}, step_id, seeded["tenant_id"])
            assert exc_info.value.defer_score is not None, (
                "Retry defer_score is None — no deferral scheduled"
            )
            assert exc_info.value.defer_score > 0, (
                f"Retry defer must be > 0, got {exc_info.value.defer_score}"
            )
            # marker removed so the retry re-sends
            async with session_factory() as s:
                count = (
                    await s.execute(
                        select(func.count())
                        .select_from(ss.SentEmail)
                        .where(ss.SentEmail.enrollment_step_id == step_id)
                    )
                ).scalar()
            assert count == 0, "SentEmail marker not removed before Retry"
            assert seeded["active_mailbox_id"] in release_calls, (
                "release_send not called before Retry — capacity slot leaked"
            )
        finally:
            for c in cms:
                c.stop()

    @pytest.mark.asyncio
    async def test_429_honors_retry_after_header(
        self, seeded, session_factory, monkeypatch
    ):
        """429 with Retry-After: 120 → ArqRetry defer == 120s."""
        async with session_factory() as s:
            mb = await s.get(Mailbox, seeded["active_mailbox_id"])
            mb.transport = "email_api"
            await s.commit()

        step_id = await _make_enrollment_step(
            session_factory, seeded, seeded["active_mailbox_id"]
        )
        monkeypatch.setattr(ss.settings, "gmail_enabled", True, raising=False)

        def handler(req):
            return _resp(
                429,
                json_body={
                    "errors": [{"code": "reputation_suspended", "detail": "poor"}]
                },
                headers={"Retry-After": "120"},
            )

        cms = _worker_patches(session_factory)
        cms.extend(_real_transport_with_mock_http(handler))
        for c in cms:
            c.start()
        try:
            with pytest.raises(ArqRetry) as exc_info:
                await ss.process_sequence_step({}, step_id, seeded["tenant_id"])
            defer_s = (exc_info.value.defer_score or 0) / 1000
            assert defer_s == 120, (
                f"Retry defer should honor Retry-After=120s, got {defer_s}s"
            )
        finally:
            for c in cms:
                c.stop()

    @pytest.mark.asyncio
    async def test_503_raises_arq_retry(self, seeded, session_factory, monkeypatch):
        """5xx (503) → ArqRetry raised with a positive deferral."""
        async with session_factory() as s:
            mb = await s.get(Mailbox, seeded["active_mailbox_id"])
            mb.transport = "email_api"
            await s.commit()

        step_id = await _make_enrollment_step(
            session_factory, seeded, seeded["active_mailbox_id"]
        )
        monkeypatch.setattr(ss.settings, "gmail_enabled", True, raising=False)

        def handler(req):
            return _resp(
                503, json_body={"errors": [{"code": "10016", "detail": "down"}]}
            )

        cms = _worker_patches(session_factory)
        cms.extend(_real_transport_with_mock_http(handler))
        for c in cms:
            c.start()
        try:
            with pytest.raises(ArqRetry) as exc_info:
                await ss.process_sequence_step({}, step_id, seeded["tenant_id"])
            assert (exc_info.value.defer_score or 0) > 0
        finally:
            for c in cms:
                c.stop()

    @pytest.mark.asyncio
    async def test_timeout_raises_arq_retry(self, seeded, session_factory, monkeypatch):
        """httpx timeout → ArqRetry raised (retryable)."""
        async with session_factory() as s:
            mb = await s.get(Mailbox, seeded["active_mailbox_id"])
            mb.transport = "email_api"
            await s.commit()

        step_id = await _make_enrollment_step(
            session_factory, seeded, seeded["active_mailbox_id"]
        )
        monkeypatch.setattr(ss.settings, "gmail_enabled", True, raising=False)

        def handler(req):
            raise httpx.TimeoutException("simulated timeout")

        cms = _worker_patches(session_factory)
        cms.extend(_real_transport_with_mock_http(handler, timeout=0.5))
        for c in cms:
            c.start()
        try:
            with pytest.raises(ArqRetry) as exc_info:
                await ss.process_sequence_step({}, step_id, seeded["tenant_id"])
            assert (exc_info.value.defer_score or 0) > 0
        finally:
            for c in cms:
                c.stop()

    @pytest.mark.asyncio
    async def test_400_terminal_failure_no_retry(
        self, seeded, session_factory, monkeypatch
    ):
        """Permanent 4xx (400) → RuntimeError (terminal, NOT ArqRetry). The
        Gmail-path contract: fail once, let reconcile re-enqueue later.
        Marker removed + capacity released so the step is retryable by
        reconcile."""
        from sqlalchemy import select, func

        async with session_factory() as s:
            mb = await s.get(Mailbox, seeded["active_mailbox_id"])
            mb.transport = "email_api"
            await s.commit()

        step_id = await _make_enrollment_step(
            session_factory, seeded, seeded["active_mailbox_id"]
        )
        monkeypatch.setattr(ss.settings, "gmail_enabled", True, raising=False)

        release_calls = []
        original_release = ss.release_send

        async def _tracking_release(db, mailbox_id):
            release_calls.append(mailbox_id)
            return await original_release(db, mailbox_id)

        def handler(req):
            return _resp(
                400,
                json_body={"errors": [{"code": "10015", "detail": "bad idempotency"}]},
            )

        cms = _worker_patches(session_factory)
        cms.extend(_real_transport_with_mock_http(handler))
        cms.append(patch.object(ss, "release_send", side_effect=_tracking_release))
        for c in cms:
            c.start()
        try:
            # Must be RuntimeError, NOT ArqRetry
            with pytest.raises(RuntimeError) as exc_info:
                await ss.process_sequence_step({}, step_id, seeded["tenant_id"])
            assert not isinstance(exc_info.value, ArqRetry), (
                "400 must be terminal (RuntimeError), not ArqRetry"
            )
            async with session_factory() as s:
                count = (
                    await s.execute(
                        select(func.count())
                        .select_from(ss.SentEmail)
                        .where(ss.SentEmail.enrollment_step_id == step_id)
                    )
                ).scalar()
            assert count == 0, "SentEmail marker not removed after permanent failure"
            assert seeded["active_mailbox_id"] in release_calls, (
                "release_send not called after permanent failure — capacity leaked"
            )
        finally:
            for c in cms:
                c.stop()


# ── P1-B: unknown transport value → terminal config error ──────────────────


class TestUnknownTransport:
    """P1-B: a row with transport='unexpected' must NOT silently fall through
    to Gmail. Explicit dispatch: email_api / gmail / else → terminal error."""

    @pytest.mark.asyncio
    async def test_unknown_transport_raises_terminal_error_no_send(
        self, seeded, session_factory, monkeypatch, engine
    ):
        """transport='unexpected' → no send on EITHER transport, terminal error,
        marker removed + capacity released.

        The DB CHECK constrains SQL writes, but the dispatch must fail safe on
        anything not exactly 'gmail'/'email_api' (ORM writes on a schema
        without the CHECK, future values, etc.). We simulate a bad value
        reaching the dispatch by disabling the CHECK via PRAGMA and writing
        'unexpected' via raw SQL — the ORM path would reject it."""
        from sqlalchemy import select, func, text

        async with engine.begin() as conn:
            await conn.execute(text("PRAGMA ignore_check_constraints = ON"))
            await conn.execute(
                text("UPDATE mailboxes SET transport='unexpected' WHERE id=:id"),
                {"id": seeded["active_mailbox_id"]},
            )

        step_id = await _make_enrollment_step(
            session_factory, seeded, seeded["active_mailbox_id"]
        )
        monkeypatch.setattr(ss.settings, "gmail_enabled", True, raising=False)

        gmail_inbox = MagicMock()
        gmail_inbox.send_html_email = MagicMock(
            side_effect=AssertionError("Gmail called for an unknown transport")
        )

        def _email_http_should_not_fire(request):
            raise AssertionError("Email API HTTP called for an unknown transport")

        release_calls = []
        original_release = ss.release_send

        async def _tracking_release(db, mailbox_id):
            release_calls.append(mailbox_id)
            return await original_release(db, mailbox_id)

        cms = _worker_patches(session_factory)
        cms.append(patch.object(ss.GmailService, "get_inbox", return_value=gmail_inbox))
        cms.extend(_real_transport_with_mock_http(_email_http_should_not_fire))
        cms.append(patch.object(ss, "release_send", side_effect=_tracking_release))
        for c in cms:
            c.start()
        try:
            with pytest.raises(RuntimeError) as exc_info:
                await ss.process_sequence_step({}, step_id, seeded["tenant_id"])
            assert not isinstance(exc_info.value, ArqRetry), (
                "unknown transport must be terminal, not a retry"
            )
            assert gmail_inbox.send_html_email.call_count == 0
            async with session_factory() as s:
                count = (
                    await s.execute(
                        select(func.count())
                        .select_from(ss.SentEmail)
                        .where(ss.SentEmail.enrollment_step_id == step_id)
                    )
                ).scalar()
            assert count == 0, "SentEmail marker not removed after unknown transport"
            assert seeded["active_mailbox_id"] in release_calls, (
                "release_send not called after unknown transport — capacity leaked"
            )
        finally:
            for c in cms:
                c.stop()


# ── live sandbox smoke test (gated, skipped by default) ──────────────────────


@pytest.mark.skipif(
    not (os.environ.get("EMAIL_API_LIVE_SMOKE") and os.environ.get("EMAIL_API_KEY")),
    reason="Set EMAIL_API_LIVE_SMOKE=1 and EMAIL_API_KEY to run the live sandbox smoke test",
)
class TestLiveSandboxSmoke:
    """Gated live smoke test against the Telnyx Email API sandbox.

    Skipped by default (hermetic CI). To run locally::

        export EMAIL_API_LIVE_SMOKE=1
        export EMAIL_API_KEY=<whitelisted salesops key>
        .venv/bin/python -m pytest tests/test_email_api_transport_1552.py::TestLiveSandboxSmoke -v

    P2-A: uses ``sandbox_mode=True`` so the API accepts the message but does NOT
    deliver it. No ``send_at`` (no scheduled residue to cancel). The response
    status must be ``"sandbox"`` (per the EmailMessageStatus enum).
    """

    @pytest.mark.asyncio
    async def test_send_one_sandbox_email(self):
        from src.services.email_api import EmailAPITransport

        t = EmailAPITransport()  # reads EMAIL_API_KEY from env
        result = await t.send_html_email(
            from_email=os.environ.get("EMAIL_API_SMOKE_FROM", "quinn.c@telnyx.com"),
            to=os.environ.get("EMAIL_API_SMOKE_TO", "smoke-test@telnyx.com"),
            subject="[SMOKE] sequence-service Email API transport (REVOPS-1552)",
            html_body="<p>Sandbox smoke test — safe to ignore. Not delivered.</p>",
            sandbox_mode=True,
        )
        assert result["status"] == "sandbox", (
            f"expected status 'sandbox' for sandbox_mode=True, got {result['status']!r}"
        )
