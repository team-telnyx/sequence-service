"""SV2-044 r5 (FAIL 1) — REAL cross-app integration test for the enrollment
lookup routing fix.

The r4 mock test (tests/unit/providers/test_sequence_enrollment_client.py on
the v2 side) fabricated a 200 response and never exercised FastAPI routing.
The real idempotency key ``scout-v2/enrollment/<uuid>`` CONTAINS SLASHES, and
the r4 seq-svc route declared a single-segment ``{key}`` path param — so
FastAPI returned 404 for every real key → the client treated the enrollment
as absent and re-POSTed (duplicate risk). The reviewer proved this with
integrated evidence: ``RAW_LOOKUP_STATUS=404``, ``REAL_LOOKUP_FINDS_EXISTING=False``.

r5 moves the key to a query parameter and this test drives the REAL v2 client
against the REAL seq-svc FastAPI app via ``httpx.ASGITransport`` on real
PostgreSQL, with a SEEDED existing enrollment whose key is the real
slash-bearing format. It asserts ``REAL_LOOKUP_FINDS_EXISTING=True``, that
reconciliation issues ZERO POSTs when the record exists, and exactly one
POST (after the GET) when absent. It exercises FastAPI routing + param
parsing + auth end-to-end — no fabricated-status mock.
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest
from sqlalchemy import func, select
from httpx import ASGITransport, AsyncClient

from src.api.main import app
from src.models.base import get_db
from src.models.models import IdempotencyRecord, SequenceEnrollment

from tests.conftest import PG_TEST_ENABLED, SCOUT_API_KEY, SCOUT_TENANT_ID

# Cross-repo import: add the v2 repo root to sys.path so we can import the
# REAL v2 enrollment client (not a stub). The v2 client is a thin SEAM
# adapter (httpx + structlog + pydantic) — its deps are available in the
# seq-svc venv.
_V2_REPO_ROOT = str(Path(__file__).resolve().parents[2].anchor)
_V2_REPO = str(Path("/Users/kevinward/repos/scout-outbound-v2"))
if _V2_REPO not in sys.path:
    sys.path.insert(0, _V2_REPO)

from packages.providers.sequence_enrollment_client import (  # noqa: E402
    IDEMPOTENCY_KEY_PREFIX,
    SequenceEnrollmentClient,
)
from packages.contracts.sequence import (  # noqa: E402
    ENROLLMENT_CONTRACT_VERSION,
    EnrollmentStatusContract,
)

pytestmark = pytest.mark.skipif(
    not PG_TEST_ENABLED,
    reason="SEQUENCE_TEST_DB not set; PG integration tests are opt-in",
)


def _make_request(idempotency_key: str, contact_id: str = "contact-routing-1") -> dict:
    return {
        "contract_version": ENROLLMENT_CONTRACT_VERSION,
        "idempotency_key": idempotency_key,
        "correlation_id": str(uuid.uuid4()),
        "tenant_id": SCOUT_TENANT_ID,
        "account_id": str(uuid.uuid4()),
        "contact_id": contact_id,
        "cohort_id": "cohort-routing",
        "mailbox_policy": "scout-default",
        "policy_version": "1.0.0",
        "content_version": "1.0.0",
        "steps": [
            {"step_number": 1, "delay_seconds": 0, "subject": "Hi", "body": "Body 1"},
        ],
    }


def _make_enroll_kwargs(workflow_uuid: str) -> dict:
    return {
        "workflow_uuid": workflow_uuid,
        "correlation_id": str(uuid.uuid4()),
        "tenant_id": SCOUT_TENANT_ID,
        "account_id": str(uuid.uuid4()),
        "contact_id": f"contact-{workflow_uuid[:8]}",
        "cohort_id": "cohort-routing",
        "mailbox_policy": "scout-default",
        "policy_version": "1.0.0",
        "content_version": "1.0.0",
        "steps": [
            {"step_number": 1, "delay_seconds": 0, "subject": "Hi", "body": "Body 1"},
        ],
    }


@pytest.fixture
async def v2_client_and_http(pg_session_factory, pg_seeded, monkeypatch):
    """Provide the REAL v2 SequenceEnrollmentClient wired to the REAL seq-svc
    app via ASGITransport on PG. Same auth + queue mocking as pg_client.
    """
    import src.api.enrollments as enrollments_mod
    from unittest.mock import AsyncMock

    monkeypatch.setattr(
        enrollments_mod, "queue_sequence_step", AsyncMock(return_value="job-test")
    )
    monkeypatch.setattr("src.api.main.settings.sequence_service_api_key", SCOUT_API_KEY)

    async def _override_get_db():
        async with pg_session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_get_db
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as http_client:
        v2_client = SequenceEnrollmentClient(
            base_url="http://test", api_key=SCOUT_API_KEY
        )
        yield v2_client, http_client
    app.dependency_overrides.clear()


# ── 1. REAL_LOOKUP_FINDS_EXISTING=True (the load-bearing proof) ────────────


@pytest.mark.asyncio
async def test_real_lookup_finds_existing_enrollment(
    v2_client_and_http, pg_session_factory, pg_seeded
):
    """Drive the REAL v2 client against the REAL seq-svc FastAPI app via
    ASGITransport on PG. Seed an enrollment with a real slash-bearing key
    ``scout-v2/enrollment/<uuid>``, then GET-lookup via the client. The r4
    routing bug (single-segment ``{key}`` path param) returned 404 for every
    real key → REAL_LOOKUP_FINDS_EXISTING=False. r5 moves the key to a query
    param so FastAPI routing + param parsing works with slash-bearing keys.

    This test exercises the FULL stack: client URL building → httpx request →
    ASGITransport → FastAPI route matching → Query param parsing → X-API-Key
    auth middleware → SQLAlchemy query → IdempotencyRecord lookup → response.
    No fabricated-status mock.
    """
    v2_client, http_client = v2_client_and_http
    workflow_uuid = str(uuid.uuid4())
    expected_key = f"{IDEMPOTENCY_KEY_PREFIX}/{workflow_uuid}"

    post_resp = await http_client.post(
        "/v1/enrollments",
        headers={"X-API-Key": pg_seeded["api_key"]},
        json=_make_request(expected_key),
    )
    assert post_resp.status_code == 201, (
        f"seeding POST failed: {post_resp.status_code} {post_resp.text}"
    )

    result = await v2_client.lookup_by_idempotency_key(
        workflow_uuid=workflow_uuid, _client=http_client
    )

    REAL_LOOKUP_FINDS_EXISTING = result is not None
    assert REAL_LOOKUP_FINDS_EXISTING, (
        "REAL_LOOKUP_FINDS_EXISTING=False — the GET returned 404 for a real "
        "slash-bearing key (routing bug). The r4 single-segment {key} path "
        "param does not match keys containing slashes."
    )
    assert result.status is EnrollmentStatusContract.EXISTING
    assert result.enrollment_id is not None
    assert result.idempotency_key == expected_key

    async with pg_session_factory() as db:
        rec = await db.execute(
            select(IdempotencyRecord).where(
                IdempotencyRecord.scope == "enrollment",
                IdempotencyRecord.idempotency_key == expected_key,
            )
        )
        record = rec.scalar_one_or_none()
        assert record is not None, "seeded IdempotencyRecord not found"
        assert record.status == "completed"


# ── 2. Reconciliation issues ZERO POSTs when the record exists ────────────


@pytest.mark.asyncio
async def test_reconcile_zero_post_when_record_exists(
    v2_client_and_http, pg_session_factory, pg_seeded
):
    """Seed an enrollment, then call ``reconcile_by_idempotency_key`` with the
    SAME workflow_uuid. The GET-lookup finds the existing record (200/existing)
    → the client returns it WITHOUT issuing a POST. Prove zero POSTs by
    asserting the enrollment count and idempotency record count do NOT
    increase after the reconcile call.
    """
    v2_client, http_client = v2_client_and_http
    workflow_uuid = str(uuid.uuid4())
    expected_key = f"{IDEMPOTENCY_KEY_PREFIX}/{workflow_uuid}"

    post_resp = await http_client.post(
        "/v1/enrollments",
        headers={"X-API-Key": pg_seeded["api_key"]},
        json=_make_request(expected_key),
    )
    assert post_resp.status_code == 201

    async with pg_session_factory() as db:
        enr_before = (
            await db.execute(
                select(func.count())
                .select_from(SequenceEnrollment)
                .where(
                    SequenceEnrollment.external_ref
                    == _make_request(expected_key)["contact_id"]
                )
            )
        ).scalar_one()
        rec_before = (
            await db.execute(
                select(func.count())
                .select_from(IdempotencyRecord)
                .where(IdempotencyRecord.idempotency_key == expected_key)
            )
        ).scalar_one()

    resp = await v2_client.reconcile_by_idempotency_key(
        **_make_enroll_kwargs(workflow_uuid), _client=http_client
    )

    assert resp.status is EnrollmentStatusContract.EXISTING
    assert resp.enrollment_id is not None

    async with pg_session_factory() as db:
        enr_after = (
            await db.execute(
                select(func.count())
                .select_from(SequenceEnrollment)
                .where(
                    SequenceEnrollment.external_ref
                    == _make_request(expected_key)["contact_id"]
                )
            )
        ).scalar_one()
        rec_after = (
            await db.execute(
                select(func.count())
                .select_from(IdempotencyRecord)
                .where(IdempotencyRecord.idempotency_key == expected_key)
            )
        ).scalar_one()

    assert enr_after == enr_before, (
        f"ZERO_POST_WHEN_PRESENT=False: enrollment count increased "
        f"({enr_before} -> {enr_after}) — a POST was issued after the GET "
        f"found an existing record"
    )
    assert rec_after == rec_before, (
        f"idempotency record count increased ({rec_before} -> {rec_after}) "
        f"— a POST created a new record after the GET found an existing one"
    )


# ── 3. Reconciliation issues exactly ONE POST when the record is absent ───


@pytest.mark.asyncio
async def test_reconcile_one_post_when_record_absent(
    v2_client_and_http, pg_session_factory, pg_seeded
):
    """Use a FRESH workflow_uuid (no existing record). The GET-lookup returns
    404 (no prior attempt landed) → the client retries the POST. Prove exactly
    one POST by asserting the enrollment count and idempotency record count
    each increase by exactly 1 after the reconcile call. The response must be
    RESERVED (the POST landed and created a new enrollment).
    """
    v2_client, http_client = v2_client_and_http
    workflow_uuid = str(uuid.uuid4())
    expected_key = f"{IDEMPOTENCY_KEY_PREFIX}/{workflow_uuid}"

    async with pg_session_factory() as db:
        enr_before = (
            await db.execute(select(func.count()).select_from(SequenceEnrollment))
        ).scalar_one()
        rec_before = (
            await db.execute(
                select(func.count())
                .select_from(IdempotencyRecord)
                .where(IdempotencyRecord.idempotency_key == expected_key)
            )
        ).scalar_one()

    resp = await v2_client.reconcile_by_idempotency_key(
        **_make_enroll_kwargs(workflow_uuid), _client=http_client
    )

    assert resp.status is EnrollmentStatusContract.RESERVED
    assert resp.enrollment_id is not None

    async with pg_session_factory() as db:
        enr_after = (
            await db.execute(select(func.count()).select_from(SequenceEnrollment))
        ).scalar_one()
        rec_after = (
            await db.execute(
                select(func.count())
                .select_from(IdempotencyRecord)
                .where(IdempotencyRecord.idempotency_key == expected_key)
            )
        ).scalar_one()

    assert enr_after == enr_before + 1, (
        f"ONE_POST_WHEN_ABSENT=False: enrollment count changed by "
        f"{enr_after - enr_before} (expected +1) — the GET-then-POST path "
        f"did not issue exactly one POST"
    )
    assert rec_after == rec_before + 1, (
        f"idempotency record count changed by {rec_after - rec_before} (expected +1)"
    )
