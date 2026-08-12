"""REVOPS-1552 — Telnyx Email API webhook receiver + origin-split suppression.

Hermetic tests with real Ed25519 signature vectors. Covers:
  - Valid-signature happy path per event type (delivered/bounce/complaint/
    unsubscribe): suppression written with api_* reason, step/enrollment
    outcome updated.
  - Tampered body / wrong key / stale timestamp / missing headers => 401,
    nothing written (fail closed).
  - Redelivery idempotence (same event_id twice => processed once).
  - Suppression rows carry correct reason + origin; manual/SFDC rows never
    modified.
  - Suppressed address blocks the NEXT send attempt through both the
    email_api and gmail paths (the send-path check at sequence_step.py:243
    consults suppressions BEFORE the transport branch).
  - Malformed-but-signed payload => 4xx, no crash, no partial writes.

All Ed25519 signatures are computed with a test keypair generated at module
load — no live keys, no network, no external dependencies. The test public
key is injected via monkeypatch on the shared Settings singleton.
"""

import base64
import json
import time
from datetime import datetime

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy import select

from src.config import get_settings
from src.models.models import (
    EnrollmentStatus,
    EnrollmentStepStatus,
    Mailbox,
    ProcessedEmailEvent,
    Sequence,
    SequenceEnrollment,
    SequenceEnrollmentStep,
    SequenceStatus,
    SequenceStep,
    SentEmail,
    Suppression,
    SuppressionReason,
)
import src.workers.sequence_step as ss
from unittest.mock import patch


# ── test keypair (generated once at module load) ─────────────────────────────

_test_private_key = Ed25519PrivateKey.generate()
_test_public_key = _test_private_key.public_key()
_test_public_key_b64 = base64.b64encode(
    _test_public_key.public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
).decode()

# A second keypair to test "wrong key" rejection.
_other_private_key = Ed25519PrivateKey.generate()


# ── helpers ──────────────────────────────────────────────────────────────────


def _sign(raw_body: bytes, private_key=None, timestamp=None):
    """Sign ``{timestamp}|{raw_body}`` (Telnyx contract) and return
    ``(signature_b64, ts_str)``."""
    pk = private_key or _test_private_key
    ts = str(timestamp if timestamp is not None else int(time.time()))
    signed_payload = ts.encode("utf-8") + b"|" + raw_body
    sig = pk.sign(signed_payload)
    return base64.b64encode(sig).decode(), ts


def _build_event_body(
    event_type="email.delivered",
    event_id="evt-001",
    message_id="msg-001",
    to_email="vp@acme.com",
):
    """Build a raw Telnyx Email API webhook event JSON body (bytes)."""
    return json.dumps(
        {
            "data": {
                "event_type": event_type,
                "id": event_id,
                "occurred_at": "2026-08-12T10:00:00Z",
                "payload": {
                    "id": message_id,
                    "to": [to_email],
                    "from": "quinn.c@telnyx.com",
                },
            }
        }
    ).encode()


async def _seed_sent_email(
    session_factory,
    seeded,
    message_id="msg-001",
    contact_email="vp@acme.com",
    enrollment_id="enr-webhook",
    step_id="estep-webhook",
):
    """Seed an enrollment + step + SentEmail with a known Telnyx message_id."""
    async with session_factory() as s:
        enr = SequenceEnrollment(
            id=enrollment_id,
            sequence_id=seeded["sequence_id"],
            mailbox_id=seeded["active_mailbox_id"],
            contact_email=contact_email,
            contact_name="VP",
            timezone="America/New_York",
            status=EnrollmentStatus.ACTIVE,
            current_step=1,
        )
        s.add(enr)
        step = SequenceEnrollmentStep(
            id=step_id,
            enrollment_id=enrollment_id,
            step_id="step-1",
            mailbox_id=seeded["active_mailbox_id"],
            status=EnrollmentStepStatus.SENT,
            custom_subject="Hi",
            custom_body="<p>Body</p>",
        )
        s.add(step)
        sent = SentEmail(
            id="sent-" + step_id,
            message_id=message_id,
            mailbox_id=seeded["active_mailbox_id"],
            enrollment_step_id=step_id,
            subject="Hi",
            body="<p>Body</p>",
            to_email=contact_email,
            from_email="quinn.c@telnyx.com",
            sent_at=datetime.utcnow(),
        )
        s.add(sent)
        await s.commit()
    return {"enrollment_id": enrollment_id, "step_id": step_id}


async def _post_signed(client, raw_body, signature_b64, ts_str):
    return await client.post(
        "/webhooks/email-events",
        content=raw_body,
        headers={
            "telnyx-signature-ed25519": signature_b64,
            "telnyx-timestamp": ts_str,
            "Content-Type": "application/json",
        },
    )


def _set_webhook_key(monkeypatch, key_b64=_test_public_key_b64):
    monkeypatch.setattr(get_settings(), "telnyx_webhook_public_key", key_b64)


# ── signature verification (fail closed) ──────────────────────────────────────


class TestSignatureVerification:
    @pytest.mark.asyncio
    async def test_tampered_body_rejected_401(
        self, client, session_factory, seeded, monkeypatch
    ):
        _set_webhook_key(monkeypatch)
        await _seed_sent_email(session_factory, seeded)

        body = _build_event_body()
        sig, ts = _sign(body)
        tampered = body.replace(b"vp@acme.com", b"hacker@evil.com")

        resp = await _post_signed(client, tampered, sig, ts)
        assert resp.status_code == 401

        async with session_factory() as s:
            count = (await s.execute(select(Suppression))).scalars().all()
            assert len(count) == 0, "tampered body must not write any suppression"

    @pytest.mark.asyncio
    async def test_wrong_key_rejected_401(
        self, client, session_factory, seeded, monkeypatch
    ):
        _set_webhook_key(monkeypatch)
        await _seed_sent_email(session_factory, seeded)

        body = _build_event_body(event_type="email.bounced")
        sig, ts = _sign(body, private_key=_other_private_key)

        resp = await _post_signed(client, body, sig, ts)
        assert resp.status_code == 401

        async with session_factory() as s:
            assert len((await s.execute(select(Suppression))).scalars().all()) == 0

    @pytest.mark.asyncio
    async def test_stale_timestamp_rejected_401(
        self, client, session_factory, seeded, monkeypatch
    ):
        _set_webhook_key(monkeypatch)
        await _seed_sent_email(session_factory, seeded)

        body = _build_event_body(event_type="email.bounced")
        old_ts = str(int(time.time()) - 600)
        sig, _ = _sign(body, timestamp=int(old_ts))

        resp = await _post_signed(client, body, sig, old_ts)
        assert resp.status_code == 401

        async with session_factory() as s:
            assert len((await s.execute(select(Suppression))).scalars().all()) == 0

    @pytest.mark.asyncio
    async def test_missing_signature_header_rejected_401(
        self, client, session_factory, seeded, monkeypatch
    ):
        _set_webhook_key(monkeypatch)
        await _seed_sent_email(session_factory, seeded)

        body = _build_event_body(event_type="email.bounced")
        resp = await client.post(
            "/webhooks/email-events",
            content=body,
            headers={
                "telnyx-timestamp": str(int(time.time())),
                "Content-Type": "application/json",
            },
        )
        assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_missing_timestamp_header_rejected_401(
        self, client, session_factory, seeded, monkeypatch
    ):
        _set_webhook_key(monkeypatch)
        await _seed_sent_email(session_factory, seeded)

        body = _build_event_body(event_type="email.bounced")
        sig, _ = _sign(body)
        resp = await client.post(
            "/webhooks/email-events",
            content=body,
            headers={
                "telnyx-signature-ed25519": sig,
                "Content-Type": "application/json",
            },
        )
        assert resp.status_code == 401

    @pytest.mark.asyncio
    async def test_empty_env_key_rejects_all_events_401(
        self, client, session_factory, seeded, monkeypatch
    ):
        monkeypatch.setattr(get_settings(), "telnyx_webhook_public_key", "")
        await _seed_sent_email(session_factory, seeded)

        body = _build_event_body()
        sig, ts = _sign(body)
        resp = await _post_signed(client, body, sig, ts)
        assert resp.status_code == 401


# ── happy path per event type ─────────────────────────────────────────────────


class TestEventProcessing:
    @pytest.mark.asyncio
    async def test_delivered_event(self, client, session_factory, seeded, monkeypatch):
        _set_webhook_key(monkeypatch)
        await _seed_sent_email(session_factory, seeded, message_id="msg-delivered")

        body = _build_event_body(
            event_type="email.delivered",
            event_id="evt-delivered",
            message_id="msg-delivered",
        )
        sig, ts = _sign(body)
        resp = await _post_signed(client, body, sig, ts)
        assert resp.status_code == 200
        assert resp.json()["data"]["processed"] is True

        async with session_factory() as s:
            assert len((await s.execute(select(Suppression))).scalars().all()) == 0
            marker = (
                await s.execute(
                    select(ProcessedEmailEvent).where(
                        ProcessedEmailEvent.id == "evt-delivered"
                    )
                )
            ).scalar_one_or_none()
            assert marker is not None, "dedupe marker must be written"

    @pytest.mark.asyncio
    async def test_bounce_event_suppresses_and_marks_step_bounced(
        self, client, session_factory, seeded, monkeypatch
    ):
        _set_webhook_key(monkeypatch)
        ids = await _seed_sent_email(
            session_factory,
            seeded,
            message_id="msg-bounce",
            contact_email="bouncer@acme.com",
        )

        body = _build_event_body(
            event_type="email.bounced",
            event_id="evt-bounce",
            message_id="msg-bounce",
            to_email="bouncer@acme.com",
        )
        sig, ts = _sign(body)
        resp = await _post_signed(client, body, sig, ts)
        assert resp.status_code == 200

        async with session_factory() as s:
            supps = (await s.execute(select(Suppression))).scalars().all()
            assert len(supps) == 1
            assert supps[0].reason == SuppressionReason.API_BOUNCE
            assert supps[0].email == "bouncer@acme.com"
            assert supps[0].source_enrollment_id == ids["enrollment_id"]
            assert "email_api_bounce" in supps[0].notes

            step = await s.get(SequenceEnrollmentStep, ids["step_id"])
            assert step.status == EnrollmentStepStatus.BOUNCED
            enr = await s.get(SequenceEnrollment, ids["enrollment_id"])
            assert enr.status == EnrollmentStatus.BOUNCED

    @pytest.mark.asyncio
    async def test_complaint_event_suppresses_api_complaint(
        self, client, session_factory, seeded, monkeypatch
    ):
        _set_webhook_key(monkeypatch)
        ids = await _seed_sent_email(
            session_factory,
            seeded,
            message_id="msg-complaint",
            contact_email="complainer@acme.com",
        )

        body = _build_event_body(
            event_type="email.complained",
            event_id="evt-complaint",
            message_id="msg-complaint",
            to_email="complainer@acme.com",
        )
        sig, ts = _sign(body)
        resp = await _post_signed(client, body, sig, ts)
        assert resp.status_code == 200

        async with session_factory() as s:
            supps = (await s.execute(select(Suppression))).scalars().all()
            assert len(supps) == 1
            assert supps[0].reason == SuppressionReason.API_COMPLAINT
            assert supps[0].email == "complainer@acme.com"
            enr = await s.get(SequenceEnrollment, ids["enrollment_id"])
            assert enr.status == EnrollmentStatus.UNSUBSCRIBED

    @pytest.mark.asyncio
    async def test_unsubscribe_event_suppresses_api_unsubscribe(
        self, client, session_factory, seeded, monkeypatch
    ):
        _set_webhook_key(monkeypatch)
        ids = await _seed_sent_email(
            session_factory,
            seeded,
            message_id="msg-unsub",
            contact_email="unsubscriber@acme.com",
        )

        body = _build_event_body(
            event_type="email.unsubscribed",
            event_id="evt-unsub",
            message_id="msg-unsub",
            to_email="unsubscriber@acme.com",
        )
        sig, ts = _sign(body)
        resp = await _post_signed(client, body, sig, ts)
        assert resp.status_code == 200

        async with session_factory() as s:
            supps = (await s.execute(select(Suppression))).scalars().all()
            assert len(supps) == 1
            assert supps[0].reason == SuppressionReason.API_UNSUBSCRIBE
            enr = await s.get(SequenceEnrollment, ids["enrollment_id"])
            assert enr.status == EnrollmentStatus.UNSUBSCRIBED


# ── redelivery idempotence ────────────────────────────────────────────────────


class TestIdempotence:
    @pytest.mark.asyncio
    async def test_redelivery_same_event_id_processed_once(
        self, client, session_factory, seeded, monkeypatch
    ):
        _set_webhook_key(monkeypatch)
        await _seed_sent_email(
            session_factory,
            seeded,
            message_id="msg-redeliver",
            contact_email="redeliver@acme.com",
        )

        body = _build_event_body(
            event_type="email.bounced",
            event_id="evt-redeliver",
            message_id="msg-redeliver",
            to_email="redeliver@acme.com",
        )
        sig, ts = _sign(body)

        resp1 = await _post_signed(client, body, sig, ts)
        assert resp1.status_code == 200
        assert resp1.json()["data"]["processed"] is True

        resp2 = await _post_signed(client, body, sig, ts)
        assert resp2.status_code == 200
        assert resp2.json()["data"]["already_processed"] is True

        async with session_factory() as s:
            assert len((await s.execute(select(Suppression))).scalars().all()) == 1
            markers = (
                (
                    await s.execute(
                        select(ProcessedEmailEvent).where(
                            ProcessedEmailEvent.id == "evt-redeliver"
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert len(markers) == 1


# ── origin split: manual/SFDC rows never modified ────────────────────────────


class TestOriginSplit:
    @pytest.mark.asyncio
    async def test_bounce_does_not_modify_manual_suppression(
        self, client, session_factory, seeded, monkeypatch
    ):
        _set_webhook_key(monkeypatch)
        ids = await _seed_sent_email(
            session_factory,
            seeded,
            message_id="msg-manual",
            contact_email="manualtest@acme.com",
        )

        # Pre-existing MANUAL suppression (operator/SFDC origin)
        async with session_factory() as s:
            s.add(
                Suppression(
                    id="sup-manual",
                    tenant_id=seeded["tenant_id"],
                    email="manualtest@acme.com",
                    domain="acme.com",
                    reason=SuppressionReason.MANUAL,
                    source_enrollment_id=ids["enrollment_id"],
                    notes="manual entry",
                )
            )
            await s.commit()

        body = _build_event_body(
            event_type="email.bounced",
            event_id="evt-manual",
            message_id="msg-manual",
            to_email="manualtest@acme.com",
        )
        sig, ts = _sign(body)
        resp = await _post_signed(client, body, sig, ts)
        assert resp.status_code == 200

        async with session_factory() as s:
            supps = (
                (
                    await s.execute(
                        select(Suppression).where(
                            Suppression.email == "manualtest@acme.com"
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert len(supps) == 1, "must not insert a duplicate suppression"
            assert supps[0].reason == SuppressionReason.MANUAL, (
                "manual/SFDC suppression must not be modified"
            )
            assert supps[0].notes == "manual entry"


# ── suppressed address blocks next send ──────────────────────────────────────


async def _async_false(*a, **k):
    return False


def _worker_patches(session_factory):
    return [
        patch.object(ss, "async_session", session_factory),
        patch.object(ss, "check_circuit_breaker", new=_async_false),
        patch.object(ss, "check_send_window", new=lambda tz: None),
    ]


async def _make_pending_step(session_factory, seeded, mailbox_id, contact_email):
    """Create a SECOND sequence + enrollment with the same contact_email so the
    suppression check can be exercised (the unique constraint is
    (sequence_id, contact_email) — same email in a different sequence is
    allowed)."""
    async with session_factory() as s:
        seq2 = Sequence(
            id="seq-block",
            tenant_id=seeded["tenant_id"],
            name="Block Test",
            status=SequenceStatus.ACTIVE,
        )
        s.add(seq2)
        s.add(
            SequenceStep(
                id="step-block-1",
                sequence_id="seq-block",
                step_number=1,
                subject="Hi",
                body="Body",
            )
        )
        enr = SequenceEnrollment(
            id="enr-block",
            sequence_id="seq-block",
            mailbox_id=mailbox_id,
            contact_email=contact_email,
            contact_name="VP",
            timezone="America/New_York",
            status=EnrollmentStatus.ACTIVE,
            current_step=0,
        )
        s.add(enr)
        es = SequenceEnrollmentStep(
            id="estep-block",
            enrollment_id="enr-block",
            step_id="step-block-1",
            mailbox_id=mailbox_id,
            status=EnrollmentStepStatus.PENDING,
            custom_subject="Hi",
            custom_body="<p>Body</p>",
        )
        s.add(es)
        await s.commit()
    return "estep-block"


class TestSuppressionBlocksSend:
    @pytest.mark.asyncio
    async def test_bounce_suppression_blocks_email_api_send(
        self, client, session_factory, seeded, monkeypatch
    ):
        _set_webhook_key(monkeypatch)
        await _seed_sent_email(
            session_factory,
            seeded,
            message_id="msg-block-api",
            contact_email="blocker@acme.com",
        )

        body = _build_event_body(
            event_type="email.bounced",
            event_id="evt-block-api",
            message_id="msg-block-api",
            to_email="blocker@acme.com",
        )
        sig, ts = _sign(body)
        resp = await _post_signed(client, body, sig, ts)
        assert resp.status_code == 200

        async with session_factory() as s:
            mb = await s.get(Mailbox, seeded["active_mailbox_id"])
            mb.transport = "email_api"
            await s.commit()

        step_id = await _make_pending_step(
            session_factory,
            seeded,
            seeded["active_mailbox_id"],
            "blocker@acme.com",
        )
        monkeypatch.setattr(ss.settings, "gmail_enabled", True, raising=False)

        cms = _worker_patches(session_factory)
        for c in cms:
            c.start()
        try:
            result = await ss.process_sequence_step({}, step_id, seeded["tenant_id"])
            assert result.get("skipped") is True
            assert result.get("reason") == "suppressed", (
                f"email_api path must skip suppressed address, got {result}"
            )
        finally:
            for c in cms:
                c.stop()

    @pytest.mark.asyncio
    async def test_bounce_suppression_blocks_gmail_send(
        self, client, session_factory, seeded, monkeypatch
    ):
        _set_webhook_key(monkeypatch)
        await _seed_sent_email(
            session_factory,
            seeded,
            message_id="msg-block-gmail",
            contact_email="gmailblocker@acme.com",
            enrollment_id="enr-block-gmail",
            step_id="estep-block-gmail",
        )

        body = _build_event_body(
            event_type="email.bounced",
            event_id="evt-block-gmail",
            message_id="msg-block-gmail",
            to_email="gmailblocker@acme.com",
        )
        sig, ts = _sign(body)
        resp = await _post_signed(client, body, sig, ts)
        assert resp.status_code == 200

        step_id = await _make_pending_step(
            session_factory,
            seeded,
            seeded["active_mailbox_id"],
            "gmailblocker@acme.com",
        )
        monkeypatch.setattr(ss.settings, "gmail_enabled", True, raising=False)

        cms = _worker_patches(session_factory)
        for c in cms:
            c.start()
        try:
            result = await ss.process_sequence_step({}, step_id, seeded["tenant_id"])
            assert result.get("skipped") is True
            assert result.get("reason") == "suppressed", (
                f"gmail path must skip suppressed address, got {result}"
            )
        finally:
            for c in cms:
                c.stop()


# ── malformed-but-signed payload ─────────────────────────────────────────────


class TestMalformedPayload:
    @pytest.mark.asyncio
    async def test_signed_but_not_json_returns_400(
        self, client, session_factory, seeded, monkeypatch
    ):
        _set_webhook_key(monkeypatch)
        await _seed_sent_email(session_factory, seeded)

        body = b"this is not json but it is signed"
        sig, ts = _sign(body)
        resp = await _post_signed(client, body, sig, ts)
        assert resp.status_code == 400

        async with session_factory() as s:
            assert len((await s.execute(select(Suppression))).scalars().all()) == 0
            assert (
                len((await s.execute(select(ProcessedEmailEvent))).scalars().all()) == 0
            )

    @pytest.mark.asyncio
    async def test_signed_but_missing_fields_returns_400(
        self, client, session_factory, seeded, monkeypatch
    ):
        _set_webhook_key(monkeypatch)
        await _seed_sent_email(session_factory, seeded)

        body = json.dumps({"data": {"event_type": "email.bounced"}}).encode()
        sig, ts = _sign(body)
        resp = await _post_signed(client, body, sig, ts)
        assert resp.status_code == 400

        async with session_factory() as s:
            assert len((await s.execute(select(Suppression))).scalars().all()) == 0
            assert (
                len((await s.execute(select(ProcessedEmailEvent))).scalars().all()) == 0
            )

    @pytest.mark.asyncio
    async def test_signed_unknown_event_type_returns_400(
        self, client, session_factory, seeded, monkeypatch
    ):
        _set_webhook_key(monkeypatch)
        await _seed_sent_email(session_factory, seeded)

        body = _build_event_body(
            event_type="email.unknown",
            event_id="evt-unknown",
            message_id="msg-001",
        )
        sig, ts = _sign(body)
        resp = await _post_signed(client, body, sig, ts)
        assert resp.status_code == 400


# ── r2: API suppressions are email-scoped (F1) ──────────────────────────────


class TestApiSuppressionIsEmailScoped:
    """Finding 1 (r2 P1): API-event suppressions must be EMAIL-scoped only.

    A bounce for a@storm.test must NOT suppress unrelated@storm.test. The
    guard's domain semantics are NOT changed — a manual domain row still
    blocks domain-wide.
    """

    @pytest.mark.asyncio
    async def test_bounce_does_not_suppress_unrelated_same_domain(
        self, client, session_factory, seeded, monkeypatch
    ):
        from src.services.suppression import check_suppressed

        _set_webhook_key(monkeypatch)
        await _seed_sent_email(
            session_factory,
            seeded,
            message_id="msg-dom-scope",
            contact_email="a@storm.test",
        )

        body = _build_event_body(
            event_type="email.bounced",
            event_id="evt-dom-scope",
            message_id="msg-dom-scope",
            to_email="a@storm.test",
        )
        sig, ts = _sign(body)
        resp = await _post_signed(client, body, sig, ts)
        assert resp.status_code == 200

        async with session_factory() as s:
            assert (
                await check_suppressed(s, "a@storm.test", seeded["tenant_id"]) is True
            ), "bounce target must be suppressed"
            assert (
                await check_suppressed(s, "unrelated@storm.test", seeded["tenant_id"])
                is False
            ), "API bounce must not suppress unrelated contacts at the same domain"

    @pytest.mark.asyncio
    async def test_manual_domain_suppression_still_blocks_domain_wide(
        self, session_factory, seeded
    ):
        """Finding 1: intentional manual domain row still blocks domain-wide
        (the guard's domain semantics are NOT changed by the r2 fix)."""
        from src.services.suppression import check_suppressed

        async with session_factory() as s:
            s.add(
                Suppression(
                    id="sup-dom-manual",
                    tenant_id=seeded["tenant_id"],
                    email="domainblock@storm.test",
                    domain="storm.test",
                    reason=SuppressionReason.MANUAL,
                    notes="intentional domain-wide block",
                )
            )
            await s.commit()

        async with session_factory() as s:
            assert (
                await check_suppressed(s, "anyone@storm.test", seeded["tenant_id"])
                is True
            )
            assert (
                await check_suppressed(s, "other@storm.test", seeded["tenant_id"])
                is True
            )


# ── r2: concurrent redelivery race (F3) ─────────────────────────────────────


class TestConcurrentRedeliveryRace:
    """Finding 3 (r2 P2): a unique-violation on the suppression insert (two
    concurrent bounce events for the same email) is caught and treated as
    'already suppressed' — both requests 200. The marker is still written
    and the enrollment is still marked BOUNCED.
    """

    @pytest.mark.asyncio
    async def test_suppression_unique_violation_caught(self, session_factory, seeded):
        from src.services.email_events import (
            EVENT_BOUNCE,
            EmailEvent,
            process_email_event,
        )

        ids = await _seed_sent_email(
            session_factory,
            seeded,
            message_id="msg-race-svc",
            contact_email="racer@storm.test",
        )

        # Pre-add a Suppression in the session with autoflush OFF — the
        # service's existing-check SELECT won't see it, but the flush will
        # hit the (tenant_id, email) unique constraint — simulating a
        # concurrent insert that won the race between our check and flush.
        async with session_factory() as s:
            s.autoflush = False
            s.add(
                Suppression(
                    id="sup-race-pending",
                    tenant_id=seeded["tenant_id"],
                    email="racer@storm.test",
                    domain=None,
                    reason=SuppressionReason.API_BOUNCE,
                    source_enrollment_id=ids["enrollment_id"],
                    notes="race winner (pending in session)",
                )
            )

            event = EmailEvent(
                event_id="evt-race-svc",
                event_type=EVENT_BOUNCE,
                message_id="msg-race-svc",
                to_email="racer@storm.test",
            )
            # On 7dc3303: raises IntegrityError (uncaught). After r2 fix:
            # catches it, rolls back, writes marker, commits.
            result = await process_email_event(s, event)
            assert result["processed"] is True

        # Marker was written (processing completed despite the race)
        async with session_factory() as s:
            marker = (
                await s.execute(
                    select(ProcessedEmailEvent).where(
                        ProcessedEmailEvent.id == "evt-race-svc"
                    )
                )
            ).scalar_one_or_none()
            assert marker is not None, (
                "marker must be written even after suppression race"
            )

            enr = await s.get(SequenceEnrollment, ids["enrollment_id"])
            assert enr.status == EnrollmentStatus.BOUNCED, (
                "enrollment must still be marked BOUNCED after race-handled suppression"
            )


# ── r2: durable delivered_at (F4) ───────────────────────────────────────────


class TestDeliveredDurability:
    """Finding 4 (r2 P2): delivered events persist a durable delivered_at
    timestamp on SentEmail. No suppression, no enrollment status change.
    Idempotent on redelivery (marker catches it).
    """

    @pytest.mark.asyncio
    async def test_delivered_event_sets_delivered_at(
        self, client, session_factory, seeded, monkeypatch
    ):
        _set_webhook_key(monkeypatch)
        await _seed_sent_email(session_factory, seeded, message_id="msg-delivered-at")

        body = _build_event_body(
            event_type="email.delivered",
            event_id="evt-delivered-at",
            message_id="msg-delivered-at",
        )
        sig, ts = _sign(body)
        resp = await _post_signed(client, body, sig, ts)
        assert resp.status_code == 200

        async with session_factory() as s:
            sent = await s.get(SentEmail, "sent-estep-webhook")
            assert sent.delivered_at is not None, "delivered_at must be set"
            assert len((await s.execute(select(Suppression))).scalars().all()) == 0
            enr = await s.get(SequenceEnrollment, "enr-webhook")
            assert enr.status == EnrollmentStatus.ACTIVE

    @pytest.mark.asyncio
    async def test_delivered_event_idempotent_on_redelivery(
        self, client, session_factory, seeded, monkeypatch
    ):
        _set_webhook_key(monkeypatch)
        await _seed_sent_email(session_factory, seeded, message_id="msg-delivered-idem")

        body = _build_event_body(
            event_type="email.delivered",
            event_id="evt-delivered-idem",
            message_id="msg-delivered-idem",
        )
        sig, ts = _sign(body)

        resp1 = await _post_signed(client, body, sig, ts)
        assert resp1.status_code == 200

        async with session_factory() as s:
            sent = await s.get(SentEmail, "sent-estep-webhook")
            first_delivered_at = sent.delivered_at
            assert first_delivered_at is not None

        # Redeliver the same event — marker catches it (already_processed)
        resp2 = await _post_signed(client, body, sig, ts)
        assert resp2.status_code == 200
        assert resp2.json()["data"]["already_processed"] is True

        async with session_factory() as s:
            sent = await s.get(SentEmail, "sent-estep-webhook")
            assert sent.delivered_at == first_delivered_at, (
                "redelivery must not change delivered_at"
            )
