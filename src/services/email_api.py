"""Telnyx Email API transport adapter (REVOPS-1552).

Sends email via POST https://api.telnyx.com/v2/emails (a backward-compatible
alias for POST /v2/email_messages; both are accepted by the API). Honors the
message contract the Gmail path uses (from, to, subject, HTML/plain body, cc,
bcc, reply_to, sender_name, list_unsubscribe) so the send dispatch in
sequence_step.py can treat both transports uniformly.

THREADING GAP (documented, not silently dropped):
  The Gmail path threads replies via raw RFC 5322 In-Reply-To/References headers
  built from a prior message's Message-ID header (gmail.send_html_email
  accepts ``message_id`` and ``thread_id`` for this). The Telnyx Email API
  supports threading via ``in_reply_to_message_id`` — but that field takes a
  TELNYX MESSAGE UUID (the ``id`` returned from a prior POST /v2/emails
  response), NOT an RFC 5322 Message-ID header string. The two ID spaces are
  disjoint. The current sequence-service dispatch sends first-touch emails
  (no reply context), so neither transport passes threading context today.
  When reply threading is needed, pass the Telnyx message UUID from the
  original send via ``in_reply_to_message_id``; do NOT pass a Gmail message_id
  or an RFC 5322 header. This adapter accepts ``in_reply_to_message_id``
  explicitly and omits it when None — it never silently drops a threading
  parameter, and it does not accept the Gmail ``message_id``/``thread_id``
  params because they are not applicable to the Email API.

send_at GUARD (hard requirement, REVOPS-1552):
  The Email API silently ignores invalid or PAST ``scheduled_at`` (alias
  ``send_at``) values and sends immediately (reproduced 2026-08-05, fix not
  landed upstream). This adapter:
    (a) rejects any ``send_at`` in the past CLIENT-SIDE before calling the API
        (EmailAPIConfigError; no HTTP call is made), and
    (b) for future ``send_at``, asserts the response reports ``status:
        "scheduled"`` — any other status raises EmailAPIError so a silent
        immediate send is never mistaken for a successful scheduled send.
  Naive datetimes are treated as UTC (the sequence_enrollment_steps.scheduled_at
  column is naive-UTC; this matches the repo's existing convention — no new
  naive/aware mixing is introduced).

CONFIG:
  EMAIL_API_KEY — env var. MUST be the whitelisted salesops account key;
    non-whitelisted keys fail with error code 10038 at the API (salesops-
    specific behavior; the public schema lists 10006/10036 for auth/
    idempotency errors). Only the key NAME is read from env — the key VALUE
    never appears in the repo.
  EMAIL_API_BASE_URL — defaults to "https://api.telnyx.com/v2". Configurable so
    tests and staging can point elsewhere.
  EMAIL_API_TIMEOUT — request timeout in seconds (default 30.0).

ERROR MAPPING:
  4xx (400/401/403/404/409/413/422) → EmailAPIPermanentError (no retry).
  429 → EmailAPIReputationError (reputation throttle; retryable with backoff).
  5xx (500/502/503) or httpx timeout → EmailAPIRetryableError.
  The MTA queues per-provider server-side (deferrals are not failures), so
  there is NO client-side pacing loop — a single POST is the complete send.

PAYLOAD CONSTRUCTION:
  The API accepts unknown request fields silently. The payload is therefore
  built EXPLICITLY field-by-field; never by passing through an arbitrary dict.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Optional

import httpx
import structlog

logger = structlog.get_logger()

DEFAULT_BASE_URL = "https://api.telnyx.com/v2"
DEFAULT_TIMEOUT = 30.0
EMAILS_PATH = "/emails"  # backward-compatible alias for /email_messages


class EmailAPIError(Exception):
    """Base exception for Email API transport errors.

    Attributes:
        status_code: HTTP status code from the response (None for pre-HTTP
            failures like the send_at guard or a timeout).
        retryable: True for 5xx/429/timeouts (caller may retry with backoff);
            False for 4xx and config errors.
    """

    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        retryable: bool = False,
    ):
        self.status_code = status_code
        self.retryable = retryable
        super().__init__(message)


class EmailAPIPermanentError(EmailAPIError):
    """4xx — permanent failure, no retry."""


class EmailAPIRetryableError(EmailAPIError):
    """5xx or timeout — retryable."""


class EmailAPIReputationError(EmailAPIError):
    """429 — sending suspended (domain reputation band 'poor').

    Retryable with backoff (the reputation band recovers over time).
    ``retry_after`` is the seconds parsed from the ``Retry-After`` response
    header (None when the header is absent — the caller falls back to
    exponential backoff).
    """

    def __init__(self, message, *, retry_after=None, **kwargs):
        super().__init__(message, **kwargs)
        self.retry_after = retry_after


class EmailAPIConfigError(EmailAPIError):
    """Configuration / send-at guard violation — no HTTP call was made."""


class EmailAPITransport:
    """Telnyx Email API send transport.

    A single shared instance is correct because the API key is global (not
    per-mailbox); per-mailbox transport selection happens at the dispatch
    point in sequence_step.py based on Mailbox.transport. Mirrors the
    GmailService.get_inbox singleton pattern so tests can patch
    EmailAPITransport.get_instance uniformly.
    """

    _instance: "EmailAPITransport | None" = None

    @classmethod
    def get_instance(cls) -> "EmailAPITransport":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(
        self,
        *,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        timeout: Optional[float] = None,
    ):
        self.base_url = (
            base_url or os.environ.get("EMAIL_API_BASE_URL") or DEFAULT_BASE_URL
        ).rstrip("/")
        self.api_key = api_key or os.environ.get("EMAIL_API_KEY")
        timeout_env = os.environ.get("EMAIL_API_TIMEOUT")
        self.timeout = float(
            timeout
            if timeout is not None
            else (timeout_env if timeout_env is not None else DEFAULT_TIMEOUT)
        )
        if not self.api_key:
            raise EmailAPIConfigError(
                "EMAIL_API_KEY not configured. Set the whitelisted salesops "
                "account key in the environment (non-whitelisted keys fail "
                "with error code 10038)."
            )

    @staticmethod
    def _normalize_send_at(send_at: Optional[datetime]) -> Optional[datetime]:
        """Reject past send_at; return aware-UTC future time or None.

        The Email API silently sends immediately for past/invalid send_at
        (reproduced 2026-08-05). We reject past values CLIENT-SIDE so a stale
        scheduled_at can never trigger an unintended immediate send. Naive
        datetimes are treated as UTC (the mailboxes/step scheduled_at column
        is naive-UTC; this matches the repo's existing convention).
        """
        if send_at is None:
            return None
        if send_at.tzinfo is None:
            send_at_utc = send_at.replace(tzinfo=timezone.utc)
        else:
            send_at_utc = send_at.astimezone(timezone.utc)
        now_utc = datetime.now(timezone.utc)
        if send_at_utc <= now_utc:
            raise EmailAPIConfigError(
                f"send_at {send_at_utc.isoformat()} is in the past or now; "
                "the Email API would silently send immediately — refusing."
            )
        return send_at_utc

    def build_payload(
        self,
        *,
        from_email: str,
        to: str,
        subject: str,
        html_body: str,
        plain_body: Optional[str] = None,
        cc: Optional[str] = None,
        bcc: Optional[str] = None,
        reply_to: Optional[str] = None,
        sender_name: Optional[str] = None,
        send_at: Optional[datetime] = None,
        list_unsubscribe: Optional[str] = None,
        one_click: bool = False,
        in_reply_to_message_id: Optional[str] = None,
        sandbox_mode: bool = False,
    ) -> dict:
        """Build the POST /v2/emails request body explicitly — field by field.

        The API accepts unknown request fields silently, so the payload is
        NEVER built by passing through an arbitrary dict; only the fields
        listed below are ever sent. ``scheduled_at`` (canonical field name;
        ``send_at`` is a deprecated alias) is used for future scheduling.
        ``sandbox_mode=True`` accepts the message but does NOT deliver it
        (used by the gated live smoke test — no scheduled residue to cancel).
        """
        payload: dict = {
            "from": from_email,
            "to": [to],
            "subject": subject,
            "html_body": html_body,
        }
        if sender_name:
            payload["from_name"] = sender_name
        if plain_body:
            payload["text_body"] = plain_body
        if cc:
            payload["cc"] = [a.strip() for a in cc.split(",") if a.strip()]
        if bcc:
            payload["bcc"] = [a.strip() for a in bcc.split(",") if a.strip()]
        if reply_to:
            payload["reply_to"] = reply_to
        send_at_utc = self._normalize_send_at(send_at)
        if send_at_utc is not None:
            payload["scheduled_at"] = send_at_utc.isoformat()
        if list_unsubscribe:
            headers = {"List-Unsubscribe": list_unsubscribe}
            if one_click:
                headers["List-Unsubscribe-Post"] = "List-Unsubscribe=One-Click"
            payload["headers"] = headers
        if in_reply_to_message_id:
            payload["in_reply_to_message_id"] = in_reply_to_message_id
        if sandbox_mode:
            payload["sandbox_mode"] = True
        return payload

    async def send_html_email(
        self,
        *,
        from_email: str,
        to: str,
        subject: str,
        html_body: str,
        plain_text_fallback: Optional[str] = None,
        cc: Optional[str] = None,
        bcc: Optional[str] = None,
        reply_to: Optional[str] = None,
        sender_name: Optional[str] = None,
        send_at: Optional[datetime] = None,
        list_unsubscribe: Optional[str] = None,
        one_click: bool = False,
        in_reply_to_message_id: Optional[str] = None,
        sandbox_mode: bool = False,
        _client: Optional[httpx.AsyncClient] = None,
    ) -> dict:
        """Send an HTML email via the Telnyx Email API.

        Returns a dict with ``message_id`` (Telnyx UUID), ``thread_id`` (None —
        the Email API does not return a thread_id on create), ``label_ids``
        ([] for Gmail-contract compatibility), and ``status``. Mirrors the
        GmailService.send_html_email return shape so the dispatch point can
        treat both transports uniformly.

        ``sandbox_mode=True`` accepts the message at the API but does NOT
        deliver it — the response status is ``"sandbox"`` (per the
        EmailMessageStatus enum). Used by the gated live smoke test so there
        is no scheduled residue to cancel.

        ``_client`` is for tests only (inject an httpx.AsyncClient with a
        MockTransport); production leaves it None and a short-lived client is
        created/closed per send.
        """
        # Guard runs twice (here and in build_payload) by design: the first
        # call raises before any payload work; the second is a no-op on an
        # already-normalized value. Both are cheap and keep the guard local
        # to the entry point.
        send_at_utc = self._normalize_send_at(send_at)
        payload = self.build_payload(
            from_email=from_email,
            to=to,
            subject=subject,
            html_body=html_body,
            plain_body=plain_text_fallback,
            cc=cc,
            bcc=bcc,
            reply_to=reply_to,
            sender_name=sender_name,
            send_at=send_at,
            list_unsubscribe=list_unsubscribe,
            one_click=one_click,
            in_reply_to_message_id=in_reply_to_message_id,
            sandbox_mode=sandbox_mode,
        )

        url = f"{self.base_url}{EMAILS_PATH}"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        own_client = _client is None
        client = _client or httpx.AsyncClient(timeout=self.timeout)
        try:
            try:
                resp = await client.post(url, json=payload, headers=headers)
            except httpx.TimeoutException as e:
                raise EmailAPIRetryableError(
                    f"Email API request timed out after {self.timeout}s: {e}",
                    status_code=None,
                    retryable=True,
                ) from e

            if resp.status_code == 202:
                body = resp.json()
                data = body.get("data")
                # The Telnyx OpenAPI schema requires data.id and data.status
                # on a 202 success. A 202 with a malformed body (missing data,
                # data.id, or data.status) is a server-side anomaly — the
                # server accepted the message but returned an unusable
                # response. Treat it as a retryable transport error (same
                # class as 5xx): the worker raises arq.worker.Retry and the
                # next attempt re-reads the response. Returning
                # message_id=None or status=None as a success would silently
                # lose the send (the caller writes a pending- message_id and
                # never learns the real Telnyx UUID).
                if (
                    not isinstance(data, dict)
                    or not data.get("id")
                    or not data.get("status")
                ):
                    raise EmailAPIRetryableError(
                        f"Email API returned 202 but the response body is "
                        f"malformed — data.id and data.status are required "
                        f"(OpenAPI schema). Response: {body}",
                        status_code=resp.status_code,
                        retryable=True,
                    )
                status = data.get("status")
                msg_id = data.get("id")
                # send_at guard (b): future send_at must yield status 'scheduled'
                if send_at_utc is not None and status != "scheduled":
                    raise EmailAPIError(
                        f"Email API accepted future send_at but returned "
                        f"status {status!r} (expected 'scheduled') — "
                        f"treating as send failure to prevent a silent "
                        f"immediate send. Response: {body}",
                        status_code=resp.status_code,
                    )
                return {
                    "message_id": msg_id,
                    "thread_id": None,
                    "label_ids": [],
                    "status": status,
                }

            if resp.status_code == 429:
                # Retry-After is not in the 429 schema but we honor it per
                # RFC 7231 so the worker can use it as the Retry defer.
                retry_after = None
                ra_header = resp.headers.get("Retry-After")
                if ra_header is not None:
                    try:
                        retry_after = float(ra_header)
                    except ValueError:
                        retry_after = None
                raise EmailAPIReputationError(
                    f"Email API 429 — sending suspended (reputation band "
                    f"'poor'): {resp.text}",
                    status_code=429,
                    retryable=True,
                    retry_after=retry_after,
                )
            if 400 <= resp.status_code < 500:
                raise EmailAPIPermanentError(
                    f"Email API {resp.status_code} permanent failure: {resp.text}",
                    status_code=resp.status_code,
                    retryable=False,
                )
            # 5xx
            raise EmailAPIRetryableError(
                f"Email API {resp.status_code} retryable failure: {resp.text}",
                status_code=resp.status_code,
                retryable=True,
            )
        finally:
            if own_client:
                await client.aclose()
