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


# ── r2/r3: concurrent redelivery race (F3) ──────────────────────────────────


class TestConcurrentRedeliveryRace:
    """Finding 3 (r2 P2 / r3 P1): a unique-violation on the suppression
    insert (two concurrent bounce events for the same email) is handled
    idempotently — both requests 200. The marker is still written and the
    enrollment is still marked BOUNCED.

    r3 update: the r2 test pre-added a Suppression in-session with
    autoflush=False (a single-session SQLite artifact flagged by the
    reviewer). The r3 fix uses PostgreSQL-native ON CONFLICT DO NOTHING,
    which is a raw SQL INSERT — the in-session ORM pre-add interacts
    incorrectly with it (the raw INSERT doesn't see the pending ORM row,
    then the ORM row collides at flush time). The updated test pre-COMMITs
    the suppression in a separate session, simulating a real concurrent
    winner that already committed.
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

        # Pre-COMMIT a suppression in a separate session — simulates a real
        # concurrent winner that already committed the (tenant_id, email)
        # row. The r3 ON CONFLICT DO NOTHING path sees this committed row
        # either via the fast-path SELECT or via the rowcount=0 insert
        # result; either way the loser returns 200 without reprocessing.
        async with session_factory() as s_pre:
            s_pre.add(
                Suppression(
                    id="sup-race-winner",
                    tenant_id=seeded["tenant_id"],
                    email="racer@storm.test",
                    domain=None,
                    reason=SuppressionReason.API_BOUNCE,
                    source_enrollment_id=ids["enrollment_id"],
                    notes="race winner (pre-committed)",
                )
            )
            await s_pre.commit()

        event = EmailEvent(
            event_id="evt-race-svc",
            event_type=EVENT_BOUNCE,
            message_id="msg-race-svc",
            to_email="racer@storm.test",
        )
        async with session_factory() as s:
            # r2 broad-except catches the unique violation; r3 ON CONFLICT
            # DO NOTHING returns rowcount=0. Both paths leave the marker
            # written and the enrollment BOUNCED.
            result = await process_email_event(s, event)
            assert result["processed"] is True

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

            supps = (
                (
                    await s.execute(
                        select(Suppression).where(
                            Suppression.email == "racer@storm.test"
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert len(supps) == 1, (
                "only the pre-committed suppression should exist — "
                "the loser must not write a duplicate"
            )
            assert supps[0].id == "sup-race-winner", (
                "the pre-committed (winner's) suppression must stand — "
                "one-way sync, never modified"
            )


# ── r3: PG-native idempotent inserts (F1 + F2) ────────────────────────────


class TestR3IdempotentInserts:
    """r3 Finding 1 (P1): INSERT ... ON CONFLICT DO NOTHING for both the
    suppression and the dedupe marker. Rowcount discriminates winner (1)
    from loser (0). Loser returns 200 duplicate-ok without reprocessing.

    These tests exercise the ON CONFLICT DO NOTHING path directly — the
    dialect-aware helpers select PostgreSQL's insert().on_conflict_do_nothing()
    in production and SQLite's insert().on_conflict_do_nothing() in the test
    harness (both support the same ON CONFLICT (cols) DO NOTHING syntax;
    SQLite ≥3.24, Python 3.12 ships SQLite 3.4x+).
    """

    @pytest.mark.asyncio
    async def test_marker_insert_returns_zero_on_conflict(self, session_factory):
        """The marker INSERT ... ON CONFLICT (id) DO NOTHING returns
        rowcount=0 when a concurrent request already committed the same
        event_id marker. This is the winner/loser arbitration point."""
        from src.services.email_events import (
            EVENT_BOUNCE,
            EmailEvent,
            _idempotent_insert_marker,
        )

        # Pre-commit the marker (concurrent winner)
        async with session_factory() as s_pre:
            s_pre.add(
                ProcessedEmailEvent(
                    id="evt-marker-conflict",
                    event_type=EVENT_BOUNCE,
                    processed_at=datetime.utcnow(),
                )
            )
            await s_pre.commit()

        # Loser tries the idempotent insert — rowcount=0
        async with session_factory() as s:
            event = EmailEvent(
                event_id="evt-marker-conflict",
                event_type=EVENT_BOUNCE,
                message_id="msg-x",
                to_email="x@storm.test",
            )
            rowcount = await _idempotent_insert_marker(s, event)
            assert rowcount == 0, (
                f"loser marker insert must return rowcount=0, got {rowcount}"
            )

    @pytest.mark.asyncio
    async def test_suppression_insert_returns_zero_on_conflict(
        self, session_factory, seeded
    ):
        """The suppression INSERT ... ON CONFLICT (tenant_id, email) DO
        NOTHING returns rowcount=0 when a concurrent request already
        committed a suppression for the same (tenant_id, email). The
        existing row is never modified (one-way sync)."""
        from src.services.email_events import (
            EVENT_BOUNCE,
            EmailEvent,
            _idempotent_insert_suppression,
        )

        # Pre-commit the suppression (concurrent winner)
        async with session_factory() as s_pre:
            s_pre.add(
                Suppression(
                    id="sup-conflict-winner",
                    tenant_id=seeded["tenant_id"],
                    email="conflict@storm.test",
                    domain=None,
                    reason=SuppressionReason.API_BOUNCE,
                    source_enrollment_id=None,
                    notes="race winner (pre-committed)",
                )
            )
            await s_pre.commit()

        # Loser tries the idempotent insert — rowcount=0
        async with session_factory() as s:
            event = EmailEvent(
                event_id="evt-sup-conflict",
                event_type=EVENT_BOUNCE,
                message_id="msg-y",
                to_email="conflict@storm.test",
            )
            rowcount = await _idempotent_insert_suppression(
                s,
                tenant_id=seeded["tenant_id"],
                email="conflict@storm.test",
                domain=None,
                reason=SuppressionReason.API_BOUNCE,
                enrollment_id="enr-synth",
                event=event,
            )
            assert rowcount == 0, (
                f"loser suppression insert must return rowcount=0, got {rowcount}"
            )

        # Verify the winner's row stands, no duplicate
        async with session_factory() as s:
            supps = (
                (
                    await s.execute(
                        select(Suppression).where(
                            Suppression.email == "conflict@storm.test"
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert len(supps) == 1
            assert supps[0].id == "sup-conflict-winner"


class TestR3ConcurrentIdenticalRedelivery:
    """r3 Finding 1 (P1): identical concurrent redelivery yields [200, 200]
    on the PG path, with exactly one suppression row and one marker row.

    Limitation (stated per reviewer guidance): the test harness cannot do
    true parallel concurrency against a single SQLite in-memory DB. This
    test simulates the exact autoflush collision sequence the reviewer
    hit on real PG by interleaving two sessions: the loser's existing-marker
    SELECT runs before the winner commits, then the winner commits, then
    the loser's process_email_event call re-runs its own SELECT and observes
    the committed marker. On real PG with true concurrency, the loser's
    marker INSERT (ON CONFLICT DO NOTHING) returns rowcount=0 instead of
    raising IntegrityError, and the loser returns 200 already-processed.
    The end-state contract (one marker, one suppression, both 200) is the
    same in both the simulated and the true-concurrent cases.
    """

    @pytest.mark.asyncio
    async def test_concurrent_identical_redelivery_both_200(
        self, session_factory, seeded
    ):
        from src.services.email_events import (
            EVENT_BOUNCE,
            EmailEvent,
            process_email_event,
        )

        ids = await _seed_sent_email(
            session_factory,
            seeded,
            message_id="msg-race-r3",
            contact_email="r3racer@storm.test",
            enrollment_id="enr-race-r3",
            step_id="estep-race-r3",
        )
        _ = ids  # seeding only; ids not referenced in assertions below

        # Open BOTH sessions. The loser runs its existing-marker SELECT
        # before the winner commits (simulating the race window), then the
        # winner does the full processing and commits, then the loser
        # calls process_email_event (which re-runs its own SELECT and
        # observes the committed marker).
        async with session_factory() as s_loser, session_factory() as s_winner:
            # Loser pre-checks the marker — sees nothing (winner hasn't
            # committed yet). This simulates the race window the reviewer
            # hit on real PG.
            pre_existing = await s_loser.execute(
                select(ProcessedEmailEvent.id).where(
                    ProcessedEmailEvent.id == "evt-race-r3"
                )
            )
            assert pre_existing.scalar_one_or_none() is None, (
                "pre-condition: loser must not see the marker before winner commits"
            )

            # Winner runs the full processing and commits.
            winner_event = EmailEvent(
                event_id="evt-race-r3",
                event_type=EVENT_BOUNCE,
                message_id="msg-race-r3",
                to_email="r3racer@storm.test",
            )
            winner_result = await process_email_event(s_winner, winner_event)
            assert winner_result.get("processed") is True, (
                f"winner should process, got {winner_result}"
            )

            # Loser continues — process_email_event re-runs its own
            # existing-marker SELECT, which now sees the committed marker,
            # and returns already_processed. On real PG with true
            # concurrency, the loser's marker INSERT (ON CONFLICT DO
            # NOTHING) returns rowcount=0 instead. Either path: 200,
            # no reprocessing.
            loser_event = EmailEvent(
                event_id="evt-race-r3",
                event_type=EVENT_BOUNCE,
                message_id="msg-race-r3",
                to_email="r3racer@storm.test",
            )
            loser_result = await process_email_event(s_loser, loser_event)
            assert loser_result.get("already_processed") is True, (
                f"loser must return already_processed, got {loser_result}"
            )

        # Final state: exactly one marker, exactly one suppression.
        async with session_factory() as s:
            markers = (
                (
                    await s.execute(
                        select(ProcessedEmailEvent).where(
                            ProcessedEmailEvent.id == "evt-race-r3"
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert len(markers) == 1, f"exactly one marker, got {len(markers)}"

            supps = (
                (
                    await s.execute(
                        select(Suppression).where(
                            Suppression.email == "r3racer@storm.test"
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert len(supps) == 1, f"exactly one suppression, got {len(supps)}"


class TestR3DistinctEventSameAddress:
    """r3 contract (3): two DISTINCT events (different event_id) for the
    same email both return 200. Each writes its own marker (distinct
    event_id). Only ONE suppression row exists (the (tenant_id, email)
    unique constraint — the second event's suppression insert is a no-op
    via ON CONFLICT DO NOTHING).
    """

    @pytest.mark.asyncio
    async def test_distinct_events_same_address_both_200_one_suppression(
        self, session_factory, seeded
    ):
        from src.services.email_events import (
            EVENT_BOUNCE,
            EmailEvent,
            process_email_event,
        )

        ids = await _seed_sent_email(
            session_factory,
            seeded,
            message_id="msg-distinct",
            contact_email="distinct@storm.test",
            enrollment_id="enr-distinct",
            step_id="estep-distinct",
        )
        _ = ids  # seeding only; ids not referenced in assertions below

        # Event A: bounce for distinct@storm.test
        event_a = EmailEvent(
            event_id="evt-distinct-a",
            event_type=EVENT_BOUNCE,
            message_id="msg-distinct",
            to_email="distinct@storm.test",
        )
        async with session_factory() as s:
            result_a = await process_email_event(s, event_a)
            assert result_a.get("processed") is True, (
                f"event A should process, got {result_a}"
            )

        # Event B: distinct event_id, same email — suppression insert is
        # a no-op (ON CONFLICT DO NOTHING), marker is distinct and writes.
        event_b = EmailEvent(
            event_id="evt-distinct-b",
            event_type=EVENT_BOUNCE,
            message_id="msg-distinct",
            to_email="distinct@storm.test",
        )
        async with session_factory() as s:
            result_b = await process_email_event(s, event_b)
            assert result_b.get("processed") is True, (
                f"event B should process, got {result_b}"
            )

        # Final state: two markers (distinct event_ids), one suppression
        async with session_factory() as s:
            markers = (
                (
                    await s.execute(
                        select(ProcessedEmailEvent).where(
                            ProcessedEmailEvent.id.in_(
                                ["evt-distinct-a", "evt-distinct-b"]
                            )
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert len(markers) == 2, (
                f"two distinct markers (one per event_id), got {len(markers)}"
            )

            supps = (
                (
                    await s.execute(
                        select(Suppression).where(
                            Suppression.email == "distinct@storm.test"
                        )
                    )
                )
                .scalars()
                .all()
            )
            assert len(supps) == 1, (
                f"exactly one suppression (unique on tenant_id, email), "
                f"got {len(supps)}"
            )


class TestR3IntegrityPropagation:
    """r3 Finding 2 (P1): the broad `except IntegrityError` at the r2
    code path mislabeled EVERY integrity failure as a duplicate race —
    the reviewer's probe swallowed an FK violation (propagated=False,
    rows=0). With the r3 ON CONFLICT DO NOTHING approach the broad except
    is removed entirely; any IntegrityError that is NOT the expected
    duplicate constraint propagates loudly. No 200, no marker.

    These tests verify that real integrity errors (FK, NOT NULL) on the
    suppression INSERT propagate. SQLite FK enforcement is enabled per-
    connection via PRAGMA foreign_keys=ON to match PostgreSQL production
    behavior (PG enforces FKs unconditionally).
    """

    @pytest.mark.asyncio
    async def test_fk_violation_on_suppression_insert_propagates(
        self, session_factory, seeded
    ):
        """An FK violation on source_enrollment_id (referencing a
        non-existent enrollment) propagates loudly — NOT swallowed by
        a blanket IntegrityError handler. The r2 broad-except swallowed
        this (reviewer probe: propagated=False, rows=0).

        This directly exercises the r3 contract: with ON CONFLICT DO
        NOTHING handling the duplicate-race case structurally, the broad
        ``except IntegrityError`` is removed entirely, so any
        non-duplicate IntegrityError (FK, NOT NULL) propagates to the
        caller. In the full ``process_email_event`` flow this means the
        exception reaches the API handler (500, no 200) and the
        marker-first transaction rolls back (no marker committed) —
        verified by design: the marker is in the same transaction as
        the suppression insert, so an exception in either rolls back
        both. A full-flow FK race (parent deleted mid-transaction
        between the chain load and the suppression insert) requires
        true concurrency to reproduce and is not simulated here; the
        direct test of ``_write_suppression`` is the precise contract
        the reviewer flagged (broad except swallowing FK).
        """
        from sqlalchemy import text
        from sqlalchemy.exc import IntegrityError

        from src.services.email_events import (
            EVENT_BOUNCE,
            EmailEvent,
            _write_suppression,
        )

        # Enable FK enforcement on this session's connection (SQLite
        # default is OFF; PG is always ON).
        async with session_factory() as s:
            await s.execute(text("PRAGMA foreign_keys=ON"))
            # Sanity-check: PRAGMA took effect
            fk_on = (await s.execute(text("PRAGMA foreign_keys"))).scalar()
            assert fk_on == 1, "PRAGMA foreign_keys=ON must take effect"

            event = EmailEvent(
                event_id="evt-fk-test",
                event_type=EVENT_BOUNCE,
                message_id="msg-fk-test",
                to_email="fktest@storm.test",
            )
            # source_enrollment_id references a non-existent enrollment —
            # FK violation on fk_suppressions_source_enrollment_id_sequence_enrollments.
            with pytest.raises(IntegrityError) as exc_info:
                await _write_suppression(
                    s,
                    tenant_id=seeded["tenant_id"],
                    email="fktest@storm.test",
                    domain=None,
                    reason=SuppressionReason.API_BOUNCE,
                    enrollment_id="nonexistent-enrollment-id",
                    event=event,
                )
            # The error must NOT be a duplicate-key (23505) on the
            # suppression unique constraint — it's an FK violation (23503).
            # We don't assert the exact sqlstate (SQLite vs PG differ in
            # diag exposure) but we DO assert the error propagates loudly
            # (no broad except swallowing it).
            assert exc_info.value is not None
            await s.rollback()


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
