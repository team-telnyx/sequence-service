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
import shutil
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, MagicMock, AsyncMock

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
    Permanent 4xx (r5) terminalizes to a durable FAILED — same contract as
    the r4 max_tries-exhausted path (marker removed, capacity released,
    error preserved in the log + result dict). The reconciler's predicate
    (status == SCHEDULED) excludes FAILED, so a permanent Email API
    rejection can no longer be re-enqueued post-grace. The Gmail path's
    RuntimeError contract is pre-existing semantics, untouched by r5 —
    out of PR scope (documented in the PR body).

    All tests mock ONLY the HTTP layer (P2-B) so the real payload builder +
    error mapping + the worker's Retry/FAILED dispatch run under test.
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
        """r5: Permanent 4xx (400) → durable terminal FAILED (NOT RuntimeError,
        NOT ArqRetry). Same failure-recording contract as the r4 max_tries-
        exhausted path: marker removed, capacity released, error preserved in
        the structured log + result dict. The reconciler's predicate
        (status == SCHEDULED) excludes FAILED, so a permanent Email API
        rejection can no longer be re-enqueued post-grace (the reviewer's
        scratch-PG reproduction showed POST_GRACE_RECONCILED=2 on a 400). The
        Gmail path's RuntimeError contract is pre-existing semantics, untouched
        here — out of PR scope (documented in the PR body).

        RED on 5c6f472: the r4-and-earlier contract raised RuntimeError and
        left the row SCHEDULED, so the reconciler re-enqueued it forever."""
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
            # r5: must return a terminal-failure dict, NOT raise RuntimeError.
            # The r4-and-earlier contract raised RuntimeError and left the row
            # SCHEDULED, so the reconciler re-enqueued it forever
            # (POST_GRACE_RECONCILED=2 on the reviewer's scratch PG).
            result = await ss.process_sequence_step({}, step_id, seeded["tenant_id"])
            assert isinstance(result, dict), (
                f"permanent 4xx must return terminal dict, not raise; "
                f"got {type(result)}"
            )
            assert result.get("failed") is True, (
                f"expected terminal failure result, got {result}"
            )
            assert result.get("reason") == "permanent_error", result
            assert result.get("status_code") == 400, result
            async with session_factory() as s:
                es = await s.get(SequenceEnrollmentStep, step_id)
                assert es.status == EnrollmentStepStatus.FAILED, (
                    f"permanent 4xx must leave row FAILED, got {es.status}"
                )
                count = (
                    await s.execute(
                        select(func.count())
                        .select_from(ss.SentEmail)
                        .where(ss.SentEmail.enrollment_step_id == step_id)
                    )
                ).scalar()
                assert count == 0, (
                    "SentEmail marker not removed after permanent failure"
                )
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


# ── r3 Finding 1: malformed 202 success bodies ────────────────────────────────


class TestMalformedSuccessBody:
    """r3 Finding 1: a 202 with a malformed body (missing data.id, missing
    data.status, or data:null) must NOT be returned as a silent success.

    The Telnyx OpenAPI schema requires data.id and data.status on a 202
    success. A 202 with a malformed body is a server-side anomaly (the server
    accepted the message but returned an unusable response) — same class as
    5xx. The adapter raises EmailAPIRetryableError so the worker raises
    arq.worker.Retry (the next attempt re-reads the response). Returning
    message_id=None or status=None as a success would silently lose the send
    (the caller writes a pending- message_id and never learns the real one).
    """

    def _make_transport(self):
        from src.services.email_api import EmailAPITransport

        return EmailAPITransport(
            api_key="test-key-not-a-secret", base_url="https://api.telnyx.com/v2"
        )

    @pytest.mark.asyncio
    async def test_malformed_202_missing_data_id_raises(self):
        """202 with data.status but no data.id → EmailAPIRetryableError
        (not a success with message_id=None)."""
        from src.services.email_api import EmailAPIRetryableError

        t = self._make_transport()
        client = _mock_client(
            lambda r: _resp(202, json_body={"data": {"status": "queued"}})
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
        assert exc_info.value.status_code == 202
        await client.aclose()

    @pytest.mark.asyncio
    async def test_malformed_202_missing_data_status_raises(self):
        """202 with data.id but no data.status → EmailAPIRetryableError
        (not a success with status=None)."""
        from src.services.email_api import EmailAPIRetryableError

        t = self._make_transport()
        client = _mock_client(
            lambda r: _resp(202, json_body={"data": {"id": "uuid-abc"}})
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
        assert exc_info.value.status_code == 202
        await client.aclose()

    @pytest.mark.asyncio
    async def test_malformed_202_data_null_raises(self):
        """202 with data:null → EmailAPIRetryableError (not a success with
        both message_id=None and status=None)."""
        from src.services.email_api import EmailAPIRetryableError

        t = self._make_transport()
        client = _mock_client(lambda r: _resp(202, json_body={"data": None}))
        with pytest.raises(EmailAPIRetryableError) as exc_info:
            await t.send_html_email(
                from_email="q@telnyx.com",
                to="p@acme.com",
                subject="S",
                html_body="<p>x</p>",
                _client=client,
            )
        assert exc_info.value.retryable is True
        assert exc_info.value.status_code == 202
        await client.aclose()

    @pytest.mark.asyncio
    async def test_malformed_202_via_worker_raises_arq_retry(
        self, seeded, session_factory, monkeypatch
    ):
        """Via the worker path, a malformed 202 produces ArqRetry (not a
        success result, not a terminal RuntimeError). The malformed body is
        a retryable transport error — the worker re-enqueues with backoff."""
        async with session_factory() as s:
            mb = await s.get(Mailbox, seeded["active_mailbox_id"])
            mb.transport = "email_api"
            await s.commit()
        step_id = await _make_enrollment_step(
            session_factory, seeded, seeded["active_mailbox_id"]
        )
        monkeypatch.setattr(ss.settings, "gmail_enabled", True, raising=False)

        def handler(req):
            return _resp(202, json_body={"data": None})

        cms = _worker_patches(session_factory)
        cms.extend(_real_transport_with_mock_http(handler))
        for c in cms:
            c.start()
        try:
            with pytest.raises(ArqRetry):
                await ss.process_sequence_step({}, step_id, seeded["tenant_id"])
        finally:
            for c in cms:
                c.stop()


# ── r3 Finding 2: long Retry-After vs reconciler grace ───────────────────────


class TestRetryAfterDeferRace:
    """r3 Finding 2: long Retry-After must never defer past the point where
    the reconciler would re-enqueue the step as "lost".

    The race: worker gets 429 with Retry-After=1200, raises ArqRetry(defer=1200).
    The step's scheduled_at stays at the original (past) value. The reconciler
    (grace=900s) sees scheduled_at < now - 900 and re-enqueues a second job.
    Both jobs fire → double-enqueue (and potential double-send).

    Fix (two complementary mechanisms):
    1. CAP the defer at reconcile_grace_seconds * retry_defer_grace_fraction
       (default 0.5) — derived from the same config the reconciler reads. If
       Retry-After exceeds the cap, defer at the cap and let the next attempt
       re-read Retry-After.
    2. ADVANCE enrollment_step.scheduled_at = now + capped_defer when deferring
       so the reconciler's own predicate (scheduled_at < now - grace) excludes
       the deferred step — the structural fix that closes the race.
    """

    @pytest.mark.asyncio
    async def test_retry_after_above_cap_is_capped(
        self, seeded, session_factory, monkeypatch
    ):
        """Retry-After=1200 with grace=900, fraction=0.5 → cap=450 → defer
        == 450 (NOT 1200). The next attempt re-reads Retry-After."""
        async with session_factory() as s:
            mb = await s.get(Mailbox, seeded["active_mailbox_id"])
            mb.transport = "email_api"
            await s.commit()
        step_id = await _make_enrollment_step(
            session_factory, seeded, seeded["active_mailbox_id"]
        )
        monkeypatch.setattr(ss.settings, "gmail_enabled", True, raising=False)
        monkeypatch.setattr(ss.settings, "reconcile_grace_seconds", 900, raising=False)
        monkeypatch.setattr(
            ss.settings, "retry_defer_grace_fraction", 0.5, raising=False
        )

        def handler(req):
            return _resp(
                429,
                json_body={
                    "errors": [{"code": "reputation_suspended", "detail": "poor"}]
                },
                headers={"Retry-After": "1200"},
            )

        cms = _worker_patches(session_factory)
        cms.extend(_real_transport_with_mock_http(handler))
        for c in cms:
            c.start()
        try:
            with pytest.raises(ArqRetry) as exc_info:
                await ss.process_sequence_step({}, step_id, seeded["tenant_id"])
            defer_s = (exc_info.value.defer_score or 0) / 1000
            assert defer_s == 450, (
                f"Retry-After=1200 should be capped at 450 (grace=900 * 0.5), "
                f"got {defer_s}"
            )
        finally:
            for c in cms:
                c.stop()

    @pytest.mark.asyncio
    async def test_retry_after_below_cap_honored(
        self, seeded, session_factory, monkeypatch
    ):
        """Retry-After=120 with cap=450 → defer == 120 (honored exactly)."""
        async with session_factory() as s:
            mb = await s.get(Mailbox, seeded["active_mailbox_id"])
            mb.transport = "email_api"
            await s.commit()
        step_id = await _make_enrollment_step(
            session_factory, seeded, seeded["active_mailbox_id"]
        )
        monkeypatch.setattr(ss.settings, "gmail_enabled", True, raising=False)
        monkeypatch.setattr(ss.settings, "reconcile_grace_seconds", 900, raising=False)
        monkeypatch.setattr(
            ss.settings, "retry_defer_grace_fraction", 0.5, raising=False
        )

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
                f"Retry-After=120 (below cap=450) should be honored exactly, "
                f"got {defer_s}"
            )
        finally:
            for c in cms:
                c.stop()

    @pytest.mark.asyncio
    async def test_defer_advances_scheduled_at(
        self, seeded, session_factory, monkeypatch
    ):
        """After a retryable 429 with long Retry-After, scheduled_at is
        advanced to ~now + capped_defer (not left at the original past value).
        This is the structural fix: the reconciler keys on
        scheduled_at < now - grace, so advancing scheduled_at past the cutoff
        prevents the double-enqueue race."""
        async with session_factory() as s:
            mb = await s.get(Mailbox, seeded["active_mailbox_id"])
            mb.transport = "email_api"
            await s.commit()
        step_id = await _make_enrollment_step(
            session_factory,
            seeded,
            seeded["active_mailbox_id"],
            step_status=EnrollmentStepStatus.SCHEDULED,
        )
        original_scheduled_at = datetime.utcnow() - timedelta(hours=2)
        async with session_factory() as s:
            es = await s.get(SequenceEnrollmentStep, step_id)
            es.scheduled_at = original_scheduled_at
            await s.commit()
        monkeypatch.setattr(ss.settings, "gmail_enabled", True, raising=False)
        monkeypatch.setattr(ss.settings, "reconcile_grace_seconds", 900, raising=False)
        monkeypatch.setattr(
            ss.settings, "retry_defer_grace_fraction", 0.5, raising=False
        )

        def handler(req):
            return _resp(
                429,
                json_body={
                    "errors": [{"code": "reputation_suspended", "detail": "poor"}]
                },
                headers={"Retry-After": "1200"},
            )

        cms = _worker_patches(session_factory)
        cms.extend(_real_transport_with_mock_http(handler))
        for c in cms:
            c.start()
        try:
            with pytest.raises(ArqRetry):
                await ss.process_sequence_step({}, step_id, seeded["tenant_id"])
            async with session_factory() as s:
                es = await s.get(SequenceEnrollmentStep, step_id)
                now = datetime.utcnow()
                assert es.scheduled_at > now, (
                    f"scheduled_at should be in the future after defer, "
                    f"got {es.scheduled_at} (now={now})"
                )
                assert es.scheduled_at < now + timedelta(seconds=500), (
                    f"scheduled_at should be ~now+450 (capped defer), "
                    f"got {es.scheduled_at} (now={now})"
                )
        finally:
            for c in cms:
                c.stop()

    @pytest.mark.asyncio
    async def test_reconciler_does_not_double_enqueue_after_defer(
        self, seeded, session_factory, monkeypatch
    ):
        """Reproduce the reviewer's race: worker defers with Retry-After > grace,
        then run reconciler — assert it does NOT re-enqueue (single enqueue).

        Before the fix: the worker raised ArqRetry(defer=1200) but left
        scheduled_at at the original past value. The reconciler (grace=900)
        saw scheduled_at < now - 900 and re-enqueued a second job. Both jobs
        fired → double-enqueue.

        After the fix: the worker caps the defer at 450 and advances
        scheduled_at to now + 450. The reconciler's predicate
        (scheduled_at < now - 900) excludes the deferred step — no re-enqueue.
        """
        import src.workers.reconcile as rec

        async with session_factory() as s:
            mb = await s.get(Mailbox, seeded["active_mailbox_id"])
            mb.transport = "email_api"
            await s.commit()
        step_id = await _make_enrollment_step(
            session_factory,
            seeded,
            seeded["active_mailbox_id"],
            step_status=EnrollmentStepStatus.SCHEDULED,
        )
        original_scheduled_at = datetime.utcnow() - timedelta(hours=2)
        async with session_factory() as s:
            es = await s.get(SequenceEnrollmentStep, step_id)
            es.scheduled_at = original_scheduled_at
            await s.commit()
        monkeypatch.setattr(ss.settings, "gmail_enabled", True, raising=False)
        monkeypatch.setattr(ss.settings, "reconcile_grace_seconds", 900, raising=False)
        monkeypatch.setattr(
            ss.settings, "retry_defer_grace_fraction", 0.5, raising=False
        )
        monkeypatch.setattr(rec.settings, "reconcile_grace_seconds", 900, raising=False)

        def handler(req):
            return _resp(
                429,
                json_body={
                    "errors": [{"code": "reputation_suspended", "detail": "poor"}]
                },
                headers={"Retry-After": "1200"},
            )

        # --- Phase 1: worker processes the step and defers ---
        cms = _worker_patches(session_factory)
        cms.extend(_real_transport_with_mock_http(handler))
        for c in cms:
            c.start()
        try:
            with pytest.raises(ArqRetry) as exc_info:
                await ss.process_sequence_step({}, step_id, seeded["tenant_id"])
            defer_s = (exc_info.value.defer_score or 0) / 1000
            assert defer_s == 450, (
                f"Retry-After=1200 should be capped at 450, got {defer_s}"
            )
        finally:
            for c in cms:
                c.stop()

        # --- Phase 2: reconciler runs — should NOT re-enqueue ---
        queue_mock = AsyncMock(return_value="job-reconcile-test")
        cms2 = [
            patch.object(rec, "async_session", session_factory),
            patch.object(rec, "queue_sequence_step", new=queue_mock),
        ]
        for c in cms2:
            c.start()
        try:
            result = await rec.reconcile_scheduled_steps({})
            assert result["reconciled"] == 0, (
                f"reconciler should not re-enqueue the deferred step, "
                f"got reconciled={result['reconciled']}"
            )
            assert queue_mock.call_count == 0, (
                f"reconciler called queue_sequence_step {queue_mock.call_count} "
                f"time(s) — double-enqueue race not closed"
            )
        finally:
            for c in cms2:
                c.stop()


# ── r4 Finding: exhausted retries must not leave the row SCHEDULED ─────────


class TestRetryExhaustionTerminal:
    """r4 Finding: when ARQ retries are exhausted the row must NOT stay
    SCHEDULED — otherwise the reconciler resurrects a dead job (infinite
    retry storm). The reviewer proved on real ARQ: after max_tries=3
    exhaustion the job records JobExecutionFailed but db_status stays
    SCHEDULED with scheduled_at advanced; once grace passes, reconcile
    re-enqueues it as fresh.

    Fix (single choke point at the ``if e.retryable:`` branch — the ONLY
    site that raises ArqRetry, so every retryable path including r3's
    malformed-202 flows through it): on the LAST permitted attempt
    (``job_try >= settings.worker_max_tries``) the handler converts the
    retryable error to a durable terminal failure — row -> FAILED, marker
    removed, capacity released, underlying error preserved in the
    structured log + the result dict. Earlier attempts still raise
    ArqRetry so ARQ re-enqueues with backoff (today's behavior).
    ``max_tries`` is an explicit Settings field (``worker_max_tries``) read
    by BOTH WorkerSettings and this check — no magic-literal drift.
    """

    @pytest.mark.asyncio
    async def test_last_attempt_503_returns_terminal_failure(
        self, seeded, session_factory, monkeypatch
    ):
        """job_try == max_tries (3) on a 503 → terminal FAILED result, NO
        ArqRetry raised. Row leaves SCHEDULED so the reconciler cannot
        resurrect it. Marker removed + capacity released (same cleanup as
        the retry path)."""
        from sqlalchemy import select, func

        async with session_factory() as s:
            mb = await s.get(Mailbox, seeded["active_mailbox_id"])
            mb.transport = "email_api"
            await s.commit()
        step_id = await _make_enrollment_step(
            session_factory,
            seeded,
            seeded["active_mailbox_id"],
            step_status=EnrollmentStepStatus.SCHEDULED,
        )
        monkeypatch.setattr(ss.settings, "gmail_enabled", True, raising=False)
        monkeypatch.setattr(ss.settings, "worker_max_tries", 3, raising=False)

        release_calls = []
        original_release = ss.release_send

        async def _tracking_release(db, mailbox_id):
            release_calls.append(mailbox_id)
            return await original_release(db, mailbox_id)

        def handler(req):
            return _resp(
                503, json_body={"errors": [{"code": "10016", "detail": "down"}]}
            )

        cms = _worker_patches(session_factory)
        cms.extend(_real_transport_with_mock_http(handler))
        cms.append(patch.object(ss, "release_send", side_effect=_tracking_release))
        for c in cms:
            c.start()
        try:
            result = await ss.process_sequence_step(
                {"job_try": 3}, step_id, seeded["tenant_id"]
            )
            # NOT an ArqRetry raise — a terminal failure result dict.
            assert isinstance(result, dict), (
                f"last attempt must return a terminal result, not raise; got {type(result)}"
            )
            assert result.get("failed") is True, (
                f"expected terminal failure result, got {result}"
            )
            assert result.get("reason") == "max_retries_exhausted", result
            assert result.get("job_try") == 3, result
            assert result.get("max_tries") == 3, result
            # Row left SCHEDULED -> now FAILED (durable terminal, reconciler-proof)
            async with session_factory() as s:
                es = await s.get(SequenceEnrollmentStep, step_id)
                assert es.status == EnrollmentStepStatus.FAILED, (
                    f"step must be FAILED after exhausting retries, got {es.status}"
                )
                count = (
                    await s.execute(
                        select(func.count())
                        .select_from(ss.SentEmail)
                        .where(ss.SentEmail.enrollment_step_id == step_id)
                    )
                ).scalar()
                assert count == 0, "SentEmail marker not removed on terminal failure"
            assert seeded["active_mailbox_id"] in release_calls, (
                "capacity slot not released on terminal failure"
            )
        finally:
            for c in cms:
                c.stop()

    @pytest.mark.asyncio
    async def test_earlier_attempt_503_raises_arq_retry(
        self, seeded, session_factory, monkeypatch
    ):
        """job_try < max_tries (2 < 3) on a 503 → ArqRetry raised (today's
        behavior preserved). The retry path is unchanged for non-final
        attempts — the row stays SCHEDULED so the retry re-sends."""
        async with session_factory() as s:
            mb = await s.get(Mailbox, seeded["active_mailbox_id"])
            mb.transport = "email_api"
            await s.commit()
        step_id = await _make_enrollment_step(
            session_factory,
            seeded,
            seeded["active_mailbox_id"],
            step_status=EnrollmentStepStatus.SCHEDULED,
        )
        monkeypatch.setattr(ss.settings, "gmail_enabled", True, raising=False)
        monkeypatch.setattr(ss.settings, "worker_max_tries", 3, raising=False)

        def handler(req):
            return _resp(
                503, json_body={"errors": [{"code": "10016", "detail": "down"}]}
            )

        cms = _worker_patches(session_factory)
        cms.extend(_real_transport_with_mock_http(handler))
        for c in cms:
            c.start()
        try:
            with pytest.raises(ArqRetry):
                await ss.process_sequence_step(
                    {"job_try": 2}, step_id, seeded["tenant_id"]
                )
            # Row stays SCHEDULED — still retryable by the next ARQ attempt.
            async with session_factory() as s:
                es = await s.get(SequenceEnrollmentStep, step_id)
                assert es.status == EnrollmentStepStatus.SCHEDULED, (
                    f"non-final attempt must leave row SCHEDULED for retry, got {es.status}"
                )
        finally:
            for c in cms:
                c.stop()

    @pytest.mark.asyncio
    async def test_malformed_202_last_attempt_flows_through_same_choke_point(
        self, seeded, session_factory, monkeypatch
    ):
        """Audit: r3's malformed-202 retryable error on the LAST attempt
        flows through the SAME choke point (the single ``if e.retryable:``
        branch) as 5xx/429/timeout — terminal FAILED, no ArqRetry. One
        choke point, not per-site checks."""
        async with session_factory() as s:
            mb = await s.get(Mailbox, seeded["active_mailbox_id"])
            mb.transport = "email_api"
            await s.commit()
        step_id = await _make_enrollment_step(
            session_factory,
            seeded,
            seeded["active_mailbox_id"],
            step_status=EnrollmentStepStatus.SCHEDULED,
        )
        monkeypatch.setattr(ss.settings, "gmail_enabled", True, raising=False)
        monkeypatch.setattr(ss.settings, "worker_max_tries", 3, raising=False)

        def handler(req):
            return _resp(202, json_body={"data": None})  # malformed 202 (r3)

        cms = _worker_patches(session_factory)
        cms.extend(_real_transport_with_mock_http(handler))
        for c in cms:
            c.start()
        try:
            result = await ss.process_sequence_step(
                {"job_try": 3}, step_id, seeded["tenant_id"]
            )
            assert isinstance(result, dict) and result.get("failed") is True, (
                f"malformed-202 on last attempt must return terminal failure, got {result}"
            )
            async with session_factory() as s:
                es = await s.get(SequenceEnrollmentStep, step_id)
                assert es.status == EnrollmentStepStatus.FAILED, (
                    f"malformed-202 last attempt must set FAILED, got {es.status}"
                )
        finally:
            for c in cms:
                c.stop()

    @pytest.mark.asyncio
    async def test_max_tries_read_from_config_no_literal_drift(
        self, seeded, session_factory, monkeypatch
    ):
        """worker_max_tries is an explicit Settings field; raising it to 5
        moves the terminal boundary so job_try=3 is now a RETRY (not
        terminal). Proves the handler reads the config, not a hard-coded 3."""
        async with session_factory() as s:
            mb = await s.get(Mailbox, seeded["active_mailbox_id"])
            mb.transport = "email_api"
            await s.commit()
        step_id = await _make_enrollment_step(
            session_factory,
            seeded,
            seeded["active_mailbox_id"],
            step_status=EnrollmentStepStatus.SCHEDULED,
        )
        monkeypatch.setattr(ss.settings, "gmail_enabled", True, raising=False)
        monkeypatch.setattr(ss.settings, "worker_max_tries", 5, raising=False)

        def handler(req):
            return _resp(
                503, json_body={"errors": [{"code": "10016", "detail": "down"}]}
            )

        cms = _worker_patches(session_factory)
        cms.extend(_real_transport_with_mock_http(handler))
        for c in cms:
            c.start()
        try:
            # With max_tries=5, job_try=3 is NOT the last attempt → ArqRetry.
            with pytest.raises(ArqRetry):
                await ss.process_sequence_step(
                    {"job_try": 3}, step_id, seeded["tenant_id"]
                )
        finally:
            for c in cms:
                c.stop()


# ── r4 REAL-ARQ regression: scratch Redis, exhausted retries not resurrected


@pytest.mark.skipif(
    not shutil.which("redis-server"),
    reason="real-ARQ regression test needs redis-server on PATH",
)
class TestRealArqRetryExhaustion:
    """r4 REAL-ARQ regression — the reviewer's reproduction, as a test.

    Spin up an isolated scratch Redis, enqueue a forever-failing retryable
    (503) step, drive it through max_tries via a REAL arq Worker in burst
    mode, then run the reconciler after grace and assert ZERO re-enqueue
    and the row in its terminal FAILED state. Cleans up scratch resources.

    On b6736c8 (pre-fix) the handler raised ArqRetry on every attempt; ARQ
    recorded JobExecutionFailed after max_tries and left the row SCHEDULED;
    the reconciler re-enqueued it as fresh (infinite retry storm). After
    the r4 fix the handler converts the final attempt to a durable
    terminal FAILED so the reconciler's predicate (status == SCHEDULED)
    excludes it.
    """

    @pytest.mark.asyncio
    async def test_real_arq_exhausted_retries_not_resurrected(
        self, seeded, session_factory, monkeypatch
    ):
        import asyncio
        import shutil as _shutil  # noqa: F401  (skipif guard above)
        import socket
        import subprocess
        import time

        from arq import create_pool
        from arq.connections import RedisSettings
        from arq.worker import Worker, FailedJobs

        # --- 1. scratch Redis on a random free port ---
        def _free_port():
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sk:
                sk.bind(("127.0.0.1", 0))
                return sk.getsockname()[1]

        port = _free_port()
        proc = subprocess.Popen(
            [
                "redis-server",
                "--port",
                str(port),
                "--save",
                "",
                "--appendonly",
                "no",
                "--daemonize",
                "no",
                "--loglevel",
                "warning",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        # wait for PING (max 10s)
        deadline = time.time() + 10
        while time.time() < deadline:
            if (
                subprocess.call(
                    ["redis-cli", "-p", str(port), "PING"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                == 0
            ):
                break
            time.sleep(0.05)
        else:
            proc.terminate()
            proc.wait()
            pytest.skip("could not start scratch redis-server")

        try:
            # --- 2. seed an email_api step that fails 503 forever ---
            async with session_factory() as s:
                mb = await s.get(Mailbox, seeded["active_mailbox_id"])
                mb.transport = "email_api"
                await s.commit()
            step_id = await _make_enrollment_step(
                session_factory,
                seeded,
                seeded["active_mailbox_id"],
                step_status=EnrollmentStepStatus.SCHEDULED,
            )
            monkeypatch.setattr(ss.settings, "gmail_enabled", True, raising=False)
            monkeypatch.setattr(ss.settings, "worker_max_tries", 3, raising=False)
            # tiny grace so the reconciler predicate is exercisable without a
            # long real-time wait.
            monkeypatch.setattr(
                ss.settings, "reconcile_grace_seconds", 1, raising=False
            )

            def handler(req):
                return _resp(
                    503, json_body={"errors": [{"code": "10016", "detail": "down"}]}
                )

            # Patch the worker's DB session + transport + bypass guards.
            cms = _worker_patches(session_factory)
            cms.extend(_real_transport_with_mock_http(handler))
            for c in cms:
                c.start()

            # The 503 has no Retry-After → _compute_retry_defer returns 30s
            # exponential (30/60/120…). That is far too slow for a test. Patch
            # it to defer ~0.2s so 3 attempts complete in well under a second.
            # The patched value is INSIDE the r3 cap (grace_seconds=1 * 0.5 =
            # 0.5) so the scheduled_at advance stays consistent. NB: the call
            # site is sync (``defer_s = _compute_retry_defer(e, ctx)``, no
            # await), so the patch MUST be a plain function, not async.
            def _fast_defer(exc, ctx):
                return 0.2

            monkeypatch.setattr(ss, "_compute_retry_defer", _fast_defer)

            try:
                # --- 3. enqueue the job on the real arq pool ---
                pool = await create_pool(RedisSettings(host="127.0.0.1", port=port))
                await pool.enqueue_job(
                    "process_sequence_step", step_id, seeded["tenant_id"]
                )
                await pool.close()

                # --- 4. run a REAL arq Worker in burst to drain the queue ---
                worker = Worker(
                    functions=[ss.process_sequence_step],
                    redis_settings=RedisSettings(host="127.0.0.1", port=port),
                    burst=True,
                    max_tries=3,
                    retry_jobs=True,
                    max_burst_jobs=20,
                    job_timeout=10,
                    poll_delay=0.05,
                    queue_read_limit=10,
                )
                try:
                    await asyncio.wait_for(
                        worker.run_check(retry_jobs=True, max_burst_jobs=20),
                        timeout=30,
                    )
                except FailedJobs as fj:
                    pytest.fail(
                        "ARQ recorded a job failure after max_tries — the "
                        "handler did NOT convert the final attempt to a "
                        f"terminal result (the r4 bug). {fj}"
                    )
                finally:
                    await worker.close()

                # --- 5. assert the row is FAILED (durable terminal) ---
                async with session_factory() as s:
                    es = await s.get(SequenceEnrollmentStep, step_id)
                    assert es.status == EnrollmentStepStatus.FAILED, (
                        f"after max_tries the step must be FAILED (durable "
                        f"terminal), got {es.status}"
                    )

                # --- 6. reconciler must NOT re-enqueue a FAILED row ---
                import src.workers.reconcile as rec

                monkeypatch.setattr(
                    rec.settings, "reconcile_grace_seconds", 1, raising=False
                )
                queue_mock = AsyncMock(return_value="job-reconcile-test")
                with (
                    patch.object(rec, "async_session", session_factory),
                    patch.object(rec, "queue_sequence_step", new=queue_mock),
                ):
                    result = await rec.reconcile_scheduled_steps({})
                assert result["reconciled"] == 0, (
                    f"reconciler resurrected the FAILED step "
                    f"(reconciled={result['reconciled']}) — the r4 bug is present"
                )
                assert queue_mock.call_count == 0, (
                    f"reconciler called queue_sequence_step "
                    f"{queue_mock.call_count} time(s) — FAILED step was "
                    "resurrected (the r4 bug)"
                )
            finally:
                for c in cms:
                    c.stop()
        finally:
            # --- 7. clean up scratch resources ---
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()


# ── r5: permanent 4xx → durable FAILED, reconciler cannot resurrect ──────────


class TestPermanent4xxReconcileExclusion:
    """r5 BLOCKER 1: a permanent Email API rejection (non-retryable 4xx) must
    terminalize to ``EnrollmentStepStatus.FAILED`` — the same failure-recording
    contract as the r4 max_tries-exhausted path (marker removed, capacity
    released, error preserved). The reconciler's predicate
    (``status == SCHEDULED AND scheduled_at < cutoff``) excludes FAILED, so a
    permanent 4xx can no longer be re-enqueued post-grace.

    The reviewer reproduced the r4 bug on scratch PG: a 400 left the row
    SCHEDULED (the RuntimeError path), and post-grace the reconciler
    re-enqueued it (POST_GRACE_RECONCILED=2). The Gmail-parity argument was
    ruled insufficient now that a durable FAILED state exists. The Gmail
    path's RuntimeError contract is pre-existing semantics, untouched here —
    out of PR scope.

    The r4 ``TestRealArqRetryExhaustion`` test already proves FAILED is
    invisible to the reconciler for the retryable exhaustion path via a real
    ARQ + scratch Redis round-trip; this class covers the permanent-4xx path
    with a unit-level predicate check (the reviewer explicitly ruled that
    sufficient for r5).
    """

    @pytest.mark.asyncio
    async def test_400_permanent_failure_excluded_from_reconcile(
        self, seeded, session_factory, monkeypatch
    ):
        """End-to-end (in-memory): a 400 leaves the row FAILED, then the
        reconciler runs and must NOT re-enqueue it. RED on 5c6f472 because the
        r4-and-earlier contract raised RuntimeError and left the row SCHEDULED,
        so the reconciler would re-enqueue it (POST_GRACE_RECONCILED=2)."""
        from datetime import timedelta
        from sqlalchemy import select, func
        import src.workers.reconcile as rec

        async with session_factory() as s:
            mb = await s.get(Mailbox, seeded["active_mailbox_id"])
            mb.transport = "email_api"
            await s.commit()
        step_id = await _make_enrollment_step(
            session_factory,
            seeded,
            seeded["active_mailbox_id"],
            step_status=EnrollmentStepStatus.SCHEDULED,
        )
        monkeypatch.setattr(ss.settings, "gmail_enabled", True, raising=False)

        def handler(req):
            return _resp(
                400,
                json_body={"errors": [{"code": "10015", "detail": "bad idempotency"}]},
            )

        cms = _worker_patches(session_factory)
        cms.extend(_real_transport_with_mock_http(handler))
        for c in cms:
            c.start()
        try:
            # 1. The send must terminalize to FAILED (not raise, not Retry).
            result = await ss.process_sequence_step(
                {"job_try": 1}, step_id, seeded["tenant_id"]
            )
            assert isinstance(result, dict), (
                f"permanent 4xx must return terminal dict, not raise; "
                f"got {type(result)}"
            )
            assert result.get("failed") is True, result
            assert result.get("reason") == "permanent_error", result
            async with session_factory() as s:
                es = await s.get(SequenceEnrollmentStep, step_id)
                assert es.status == EnrollmentStepStatus.FAILED, (
                    f"permanent 4xx must leave row FAILED, got {es.status}"
                )
                # 2. Push scheduled_at into the past so the reconciler would
                # re-enqueue IF the predicate didn't exclude FAILED. This
                # isolates the test to the status-predicate, not the cutoff.
                es.scheduled_at = datetime.utcnow() - timedelta(hours=2)
                await s.commit()

            # 3. Reconciler must NOT re-enqueue a FAILED row. Short grace so
            # the past scheduled_at is unambiguously overdue.
            monkeypatch.setattr(
                rec.settings, "reconcile_grace_seconds", 1, raising=False
            )
            rec_q = AsyncMock(return_value="job-reconcile")
            rec_cms = [
                patch.object(rec, "async_session", session_factory),
                patch.object(rec, "queue_sequence_step", rec_q),
            ]
            for c in rec_cms:
                c.start()
            try:
                out = await rec.reconcile_scheduled_steps({})
            finally:
                for c in rec_cms:
                    c.stop()
            assert out["reconciled"] == 0, f"reconciler re-enqueued a FAILED row: {out}"
            assert out["scanned"] == 0, (
                f"reconciler selected a FAILED row into the batch: {out}"
            )
            rec_q.assert_not_awaited()

            # 4. Marker removed + capacity released (same contract as r4).
            async with session_factory() as s:
                count = (
                    await s.execute(
                        select(func.count())
                        .select_from(ss.SentEmail)
                        .where(ss.SentEmail.enrollment_step_id == step_id)
                    )
                ).scalar()
                assert count == 0, (
                    "SentEmail marker not removed after permanent failure"
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
