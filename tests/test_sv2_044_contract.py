"""SV2-044 — versioned enrollment contract + idempotency + negative auth tests.

Tests the docs/10 §Sequence enrollment contract:
  - same key + same digest → status=existing + original enrollment
  - same key + different digest → 409 conflict
  - first time → status=reserved, enrollment_id, capacity_date
  - concurrent identical requests → exactly ONE enrollment
  - missing/blank/wrong X-API-Key → 401 (negative auth)
"""

import asyncio
import uuid
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.api.main import app
from src.models.base import Base, get_db
from src.models.models import (
    Mailbox,
    MailboxStatus,
    SequenceEnrollment,
    Tenant,
)

SCOUT_TENANT_ID = "tenant-scout"
SCOUT_API_KEY = "test-scout-key"  # pragma: allowlist secret


@pytest_asyncio.fixture
async def engine():
    eng = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with eng.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield eng
    await eng.dispose()


@pytest_asyncio.fixture
async def session_factory(engine):
    return async_sessionmaker(engine, expire_on_commit=False)


@pytest_asyncio.fixture
async def seeded(session_factory):
    async with session_factory() as db:
        tenant = Tenant(id=SCOUT_TENANT_ID, name="Scout", api_key=SCOUT_API_KEY)
        db.add(tenant)
        mb = Mailbox(
            id="mb-active",
            tenant_id=SCOUT_TENANT_ID,
            email="quinn.c@telnyx.com",
            status=MailboxStatus.ACTIVE,
            weight=1,
            daily_send_limit=50,
            sent_today=0,
        )
        db.add(mb)
        await db.commit()
    return {"tenant_id": SCOUT_TENANT_ID, "api_key": SCOUT_API_KEY}


@pytest_asyncio.fixture
async def client(session_factory, seeded, monkeypatch):
    import src.api.enrollments as enrollments_mod
    import src.api.main as main_mod

    monkeypatch.setattr(
        enrollments_mod,
        "queue_sequence_step",
        AsyncMock(return_value="job-test"),
    )
    monkeypatch.setattr(main_mod.settings, "sequence_service_api_key", SCOUT_API_KEY)

    async def _override_get_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_get_db
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
    app.dependency_overrides.clear()


def _make_request(idempotency_key: str, contact_id: str = "contact-1") -> dict:
    return {
        "contract_version": 1,
        "idempotency_key": idempotency_key,
        "correlation_id": str(uuid.uuid4()),
        "tenant_id": "tenant-scout",
        "account_id": str(uuid.uuid4()),
        "contact_id": contact_id,
        "cohort_id": "cohort-1",
        "mailbox_policy": "scout-default",
        "policy_version": "1.0.0",
        "content_version": "1.0.0",
        "steps": [
            {"step_number": 1, "delay_seconds": 0, "subject": "Hi", "body": "Body 1"},
            {"step_number": 2, "delay_seconds": 86400, "subject": "Follow up", "body": "Body 2"},
        ],
    }


# ── Idempotency tests ─────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_first_request_returns_reserved(client, seeded):
    key = f"scout-v2/enrollment/{uuid.uuid4()}"
    req = _make_request(key)
    resp = await client.post(
        "/v1/enrollments",
        headers={"X-API-Key": seeded["api_key"]},
        json=req,
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["contract_version"] == 1
    assert body["status"] == "reserved"
    assert body["enrollment_id"] is not None
    assert body["idempotency_key"] == key
    assert body["capacity_date"] is not None


@pytest.mark.asyncio
async def test_same_key_same_digest_returns_existing(client, seeded):
    key = f"scout-v2/enrollment/{uuid.uuid4()}"
    req = _make_request(key)

    first = await client.post(
        "/v1/enrollments",
        headers={"X-API-Key": seeded["api_key"]},
        json=req,
    )
    assert first.status_code == 201
    first_enrollment_id = first.json()["enrollment_id"]

    second = await client.post(
        "/v1/enrollments",
        headers={"X-API-Key": seeded["api_key"]},
        json=req,
    )
    assert second.status_code == 200
    body = second.json()
    assert body["status"] == "existing"
    assert body["enrollment_id"] == first_enrollment_id
    assert body["idempotency_key"] == key


@pytest.mark.asyncio
async def test_same_key_different_digest_returns_409(client, seeded):
    key = f"scout-v2/enrollment/{uuid.uuid4()}"
    req1 = _make_request(key, contact_id="contact-A")
    req2 = _make_request(key, contact_id="contact-B")

    first = await client.post(
        "/v1/enrollments",
        headers={"X-API-Key": seeded["api_key"]},
        json=req1,
    )
    assert first.status_code == 201

    second = await client.post(
        "/v1/enrollments",
        headers={"X-API-Key": seeded["api_key"]},
        json=req2,
    )
    assert second.status_code == 409
    body = second.json()
    assert body["status"] == "rejected"
    assert body["reason_code"] == "sequence.idempotency_key_conflict"
    assert body["idempotency_key"] == key


@pytest.mark.asyncio
async def test_concurrent_same_key_same_digest_one_enrollment(client, seeded, session_factory):
    """Concurrent identical requests → exactly ONE enrollment (no double send)."""
    key = f"scout-v2/enrollment/{uuid.uuid4()}"
    req = _make_request(key)

    # Fire two concurrent requests with the same key + same digest.
    results = await asyncio.gather(
        client.post("/v1/enrollments", headers={"X-API-Key": seeded["api_key"]}, json=req),
        client.post("/v1/enrollments", headers={"X-API-Key": seeded["api_key"]}, json=req),
    )

    statuses = [r.json()["status"] for r in results]
    code_counts = {r.status_code for r in results}

    # One should be 201/reserved, the other 200/existing (or pending).
    assert "reserved" in statuses, f"Expected one reserved, got {statuses}"
    assert all(s in ("reserved", "existing") for s in statuses), (
        f"Expected reserved+existing, got {statuses}"
    )

    # Verify exactly ONE enrollment exists for this idempotency key.
    async with session_factory() as db:
        from sqlalchemy import select, func

        count = await db.execute(
            select(func.count())
            .select_from(SequenceEnrollment)
            .where(SequenceEnrollment.external_ref == req["contact_id"])
        )
        enrollment_count = count.scalar_one()
        assert enrollment_count == 1, (
            f"Concurrent identical requests created {enrollment_count} enrollments "
            f"(expected 1). Statuses: {statuses}, codes: {code_counts}"
        )


# ── Negative auth tests ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_missing_api_key_returns_401(client, seeded):
    req = _make_request(f"scout-v2/enrollment/{uuid.uuid4()}")
    resp = await client.post("/v1/enrollments", json=req)
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Missing API key"


@pytest.mark.asyncio
async def test_blank_api_key_returns_401(client, seeded):
    req = _make_request(f"scout-v2/enrollment/{uuid.uuid4()}")
    resp = await client.post(
        "/v1/enrollments",
        headers={"X-API-Key": ""},
        json=req,
    )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_wrong_api_key_returns_401(client, seeded):
    req = _make_request(f"scout-v2/enrollment/{uuid.uuid4()}")
    resp = await client.post(
        "/v1/enrollments",
        headers={"X-API-Key": "totally-wrong-key"},
        json=req,
    )
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Invalid API key"


@pytest.mark.asyncio
async def test_no_api_key_on_legacy_enrollments_returns_401(client, seeded):
    """Existing /api/enrollments also requires auth (not just /v1)."""
    resp = await client.get("/api/enrollments/")
    assert resp.status_code == 401


# ── Contract version validation ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_unsupported_contract_version_rejected(client, seeded):
    req = _make_request(f"scout-v2/enrollment/{uuid.uuid4()}")
    req["contract_version"] = 999
    resp = await client.post(
        "/v1/enrollments",
        headers={"X-API-Key": seeded["api_key"]},
        json=req,
    )
    assert resp.status_code == 400


# ── ReplyIntent contract (SV2-044 r3) ─────────────────────────────────────
# The versioned reply-intent taxonomy lives in src/contracts.py (the contract
# module the v2 compat test imports). These tests assert the SERVER side of
# the contract — if the server enum diverges from the docs/10 spec, the test
# fails here. The v2 side (tests/contract/test_sequence_contract_compat.py)
# asserts the same spec from the client side. Together they prove both repos
# agree — a divergent enum is a contract break that bites on at least one side.
# This is NOT self-fulfilling: the expected set is the docs/10 spec
# (hard-coded), not derived from the enum under test.

from src.contracts import (  # noqa: E402
    REPLY_INTENT_CONTRACT_VERSION as _SERVER_REPLY_INTENT_VERSION,
    ReplyIntent as _ServerReplyIntent,
)


class TestReplyIntentContractServerSide:
    """Server-side ReplyIntent contract — bites if the server enum diverges
    from the docs/10 spec. The matching client-side test in
    tests/contract/test_sequence_contract_compat.py bites if the v2 enum
    diverges. Both sides must agree — a divergent enum breaks the contract
    visibly on at least one side.
    """

    def test_reply_intent_contract_version_is_1(self) -> None:
        assert _SERVER_REPLY_INTENT_VERSION == 1, (
            f"server REPLY_INTENT_CONTRACT_VERSION is "
            f"{_SERVER_REPLY_INTENT_VERSION!r}, expected 1 (docs/10 spec)"
        )

    def test_reply_intent_values_match_spec(self) -> None:
        """The server enum values must match the docs/10 spec EXACTLY. The
        expected set is hard-coded from the spec (not derived from the enum
        under test) so the test is NOT self-fulfilling — adding/removing/renaming
        a value fails the test loudly.
        """
        expected = {
            "positive_interest",
            "positive_meeting",
            "negative_not_interested",
            "negative_wrong_person",
            "out_of_office",
            "autoresponder",
            "unsubscribe_request",
            "unknown",
        }
        actual = {r.value for r in _ServerReplyIntent}
        assert actual == expected, (
            f"server ReplyIntent diverges from docs/10 spec: "
            f"missing={expected - actual}, extra={actual - expected}"
        )

    def test_reply_intent_has_exactly_eight_values(self) -> None:
        assert len(_ServerReplyIntent) == 8, (
            f"server ReplyIntent has {len(_ServerReplyIntent)} values, expected 8 (docs/10 spec)"
        )

    def test_bounce_is_not_a_reply_intent(self) -> None:
        """Bounce is structurally detected, NOT a reply intent. v1's poller
        conflated bounce with REPLY (154/168 of the live backlog); the r3
        ingest path classifies bounce DISTINCTLY via the is_bounce flag
        BEFORE any ReplyIntent classification, and reply_intent stays NULL
        for BOUNCE signals. This test asserts the enum itself does not
        contain a bounce value — the structural separation is encoded at
        the type level, not just the ingest call site.
        """
        values = {r.value for r in _ServerReplyIntent}
        assert "bounce" not in values
        assert "BOUNCE" not in values

    def test_referral_is_not_a_reply_intent(self) -> None:
        """Referral routing is out of scope (REVOPS-1458). The enum must not
        contain a referral value — a future classifier that adds one would
        break this assertion and force an explicit contract-version bump.
        """
        values = {r.value for r in _ServerReplyIntent}
        assert "referral" not in values
