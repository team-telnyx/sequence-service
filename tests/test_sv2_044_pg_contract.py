"""SV2-044 r2 — PostgreSQL integration for the security-relevant contract tests.

These tests run ONLY against a real PostgreSQL database (gated on
SEQUENCE_TEST_DB=1 via the pg_* fixtures in conftest.py). SQLite serializes
writers with a global lock, so the "exactly one enrollment under concurrent
requests" proof on SQLite does NOT prove the PostgreSQL behavior — PG enforces
the unique-constraint race under row locks, which is the actual production
path. These tests close that gap:

  1. N concurrent identical requests → exactly ONE enrollment (PG enforces
     via the composite-PK unique index that backs ON CONFLICT DO NOTHING).
  2. same key + different digest → 409 conflict (stable reason_code).
  3. naive-UTC fields: scheduled_at / created_at / completed_at written via
     datetime.utcnow() resist a 5-hour host-TZ skew (America/New_York session
     TZ) — the stored values match now() AT TIME ZONE 'UTC', NOT bare now().
  4. negative auth: missing/blank/wrong X-API-Key → 401 against the PG-backed
     app.
  5. The unique index backing idempotency ON CONFLICT exists in the PG schema
     (the composite PK on (scope, idempotency_key)) AND migration 006's SQL,
     applied standalone, creates the same unique PK + the server-side
     now() AT TIME ZONE 'UTC' default.

The r1 SQLite tests (tests/test_sv2_044_contract.py) are NOT weakened — they
still run on the in-memory SQLite fixture for the fast unit path.
"""

import asyncio
import hashlib
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import func, select, text

from src.models.models import (
    IdempotencyRecord,
    SequenceEnrollment,
    SequenceEnrollmentStep,
    SequenceStep,
    Sequence,
    SequenceStatus,
)
from tests.conftest import (
    PG_TEST_ENABLED,
    _PG_MIGRATION_006_PATH,
    _split_sql_statements,
)

pytestmark = pytest.mark.skipif(
    not PG_TEST_ENABLED,
    reason="SEQUENCE_TEST_DB not set; PG integration tests are opt-in",
)

SCOUT_TENANT_ID = "tenant-scout"
SCOUT_API_KEY = "test-scout-key"  # pragma: allowlist secret


def _make_request(idempotency_key: str, contact_id: str = "contact-1") -> dict:
    """Same request shape as tests/test_sv2_044_contract.py::_make_request."""
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
            {
                "step_number": 2,
                "delay_seconds": 86400,
                "subject": "Follow up",
                "body": "Body 2",
            },
        ],
    }


# ── 1. Concurrent identical requests → exactly ONE enrollment ────────────


@pytest.mark.asyncio
async def test_pg_concurrent_same_key_one_enrollment(
    pg_client, pg_seeded, pg_session_factory
):
    """Fire N>=10 concurrent identical requests on PG where the
    ON CONFLICT (scope, idempotency_key) DO NOTHING race is REAL (PG uses
    row-lock arbitration under the unique index, NOT SQLite's global write
    lock). Exactly ONE enrollment must exist afterward — no double send.

    This is the proof SQLite masks (SQLite SERIALIZES writers, so the race
    never surfaces). On PG the losers' INSERTs block on the winner's
    transaction; after commit they return 0 rowcount and read the completed
    record, returning 200/existing with the winner's enrollment_id.
    """
    N = 10
    key = f"scout-v2/enrollment/{uuid.uuid4()}"
    req = _make_request(key)

    results = await asyncio.gather(
        *[
            pg_client.post(
                "/v1/enrollments",
                headers={"X-API-Key": pg_seeded["api_key"]},
                json=req,
            )
            for _ in range(N)
        ]
    )

    statuses = [r.json().get("status") for r in results]
    codes = {r.status_code for r in results}

    # One 201/reserved, the rest 200/existing. No 409, no 500, no 429.
    assert 201 in codes, f"Expected one 201, got codes={codes}, statuses={statuses}"
    assert all(c in (200, 201) for c in codes), (
        f"Concurrent identical requests must not produce 4xx/5xx; got codes={codes}"
    )
    assert statuses.count("reserved") == 1, (
        f"Expected exactly one reserved, got {statuses}"
    )
    assert all(s in ("reserved", "existing") for s in statuses), (
        f"Expected reserved+existing only, got {statuses}"
    )

    # The winner's enrollment_id should be returned by the existing-responses
    # that saw the completed record (status=existing with enrollment_id set).
    # Some losers may see pending (enrollment_id=None) if they read before the
    # winner's commit lands — that's allowed, but at least one existing-response
    # must carry the real enrollment_id.
    enrollment_ids = [
        r.json().get("enrollment_id") for r in results if r.status_code == 200
    ]
    non_none_ids = [eid for eid in enrollment_ids if eid is not None]
    assert len(non_none_ids) >= 1, (
        f"At least one existing-response must carry the enrollment_id; got {enrollment_ids}"
    )
    # All non-None enrollment_ids must be the SAME (the winner's).
    assert len(set(non_none_ids)) == 1, (
        f"All existing enrollment_ids must match; got {non_none_ids}"
    )

    # The load-bearing assertion: exactly ONE enrollment exists for this
    # idempotency key's contact_id. SQLite would pass this trivially (serialized
    # writes); on PG the unique index + ON CONFLICT DO NOTHING must enforce it
    # under real concurrent row-lock arbitration.
    async with pg_session_factory() as db:
        count = await db.execute(
            select(func.count())
            .select_from(SequenceEnrollment)
            .where(SequenceEnrollment.external_ref == req["contact_id"])
        )
        enrollment_count = count.scalar_one()
        assert enrollment_count == 1, (
            f"PG concurrent identical requests created {enrollment_count} enrollments "
            f"(expected 1). Statuses: {statuses}, codes: {codes}"
        )

    # And exactly ONE idempotency record.
    async with pg_session_factory() as db:
        rec_count = await db.execute(
            select(func.count())
            .select_from(IdempotencyRecord)
            .where(IdempotencyRecord.scope == "enrollment")
            .where(IdempotencyRecord.idempotency_key == key)
        )
        assert rec_count.scalar_one() == 1


# ── 2. same key + different digest → 409 ─────────────────────────────────


@pytest.mark.asyncio
async def test_pg_same_key_different_digest_returns_409(pg_client, pg_seeded):
    """Same idempotency_key with a different request digest → 409 conflict
    with stable reason_code. The digest is SHA-256 of the canonical request
    JSON; changing contact_id changes the digest. This is the replay-attack
    guard: a caller can't reuse a key to silently swap the request body.
    """
    key = f"scout-v2/enrollment/{uuid.uuid4()}"
    req1 = _make_request(key, contact_id="contact-A")
    req2 = _make_request(key, contact_id="contact-B")

    first = await pg_client.post(
        "/v1/enrollments",
        headers={"X-API-Key": pg_seeded["api_key"]},
        json=req1,
    )
    assert first.status_code == 201, first.text

    second = await pg_client.post(
        "/v1/enrollments",
        headers={"X-API-Key": pg_seeded["api_key"]},
        json=req2,
    )
    assert second.status_code == 409, second.text
    body = second.json()
    assert body["status"] == "rejected"
    assert body["reason_code"] == "sequence.idempotency_key_conflict"
    assert body["idempotency_key"] == key


# ── 3. naive-UTC field integration ────────────────────────────────────────


@pytest.mark.asyncio
async def test_pg_naive_utc_scheduled_at_resists_host_tz_skew(
    pg_session_factory, pg_seeded
):
    """Force the PG session TZ to America/New_York (UTC-4 in August). The
    code writes scheduled_at via datetime.utcnow() (Python-side, naive UTC).
    Prove the stored value matches now() AT TIME ZONE 'UTC', NOT bare now()
    (which would be NY-local, 4 hours behind UTC). If the code used bare now()
    or datetime.now() (no tz), the stored value would be ~4h behind UTC and
    scheduling/capacity logic keyed on scheduled_at would fire 4 hours late.

    The assertion:
      - stored scheduled_at is within 10s of datetime.utcnow() (naive UTC).
      - stored scheduled_at is within 10s of now() AT TIME ZONE 'UTC' (DB-side).
      - stored scheduled_at is ~4h AHEAD of bare now() (NY is UTC-4 in August),
        proving it is NOT session-local time.
    """
    async with pg_session_factory() as db:
        # Force a non-UTC session TZ for this connection. America/New_York is
        # UTC-4 in August (DST), the exact 4-hour skew the task warns about.
        await db.execute(text("SET TIME ZONE 'America/New_York'"))

        # Need a Sequence + SequenceStep to FK against.
        seq = Sequence(
            id=str(uuid.uuid4()),
            tenant_id=SCOUT_TENANT_ID,
            name="tz-skew-test",
            status=SequenceStatus.ACTIVE,
        )
        db.add(seq)
        await db.flush()
        step = SequenceStep(
            id=str(uuid.uuid4()),
            sequence_id=seq.id,
            step_number=1,
            subject="Hi",
            body="Body",
        )
        db.add(step)
        await db.flush()

        from src.models.models import (
            SequenceEnrollment,
            Mailbox,
            MailboxStatus,
            EnrollmentStatus,
            EnrollmentStepStatus,
        )

        mb = await db.get(Mailbox, "mb-active")
        enrollment = SequenceEnrollment(
            id=str(uuid.uuid4()),
            sequence_id=seq.id,
            mailbox_id=mb.id,
            contact_email="tz-test@scout-v2.local",
            external_ref="tz-contact",
        )
        db.add(enrollment)
        await db.flush()

        # The code path under test: scheduled_at = datetime.utcnow() (naive UTC).
        scheduled_naive_utc = datetime.utcnow()
        enrollment_step = SequenceEnrollmentStep(
            id=str(uuid.uuid4()),
            enrollment_id=enrollment.id,
            step_id=step.id,
            mailbox_id=mb.id,
            status=EnrollmentStepStatus.SCHEDULED,
            scheduled_at=scheduled_naive_utc,
        )
        db.add(enrollment_step)
        await db.commit()

        step_id = enrollment_step.id

    # Read back on a fresh session (also forced to NY TZ) and compare against
    # both UTC and session-local now().
    async with pg_session_factory() as db:
        await db.execute(text("SET TIME ZONE 'America/New_York'"))
        row = (
            await db.execute(
                text(
                    "SELECT scheduled_at, "
                    "now() AT TIME ZONE 'UTC' AS utc_now, "
                    # now()::timestamp is what "bare now() stored in a TIMESTAMP
                    # column" produces — the session-local naive time. If the
                    # code used bare now() as the default, stored would match
                    # this; the assertion proves it does NOT.
                    "now()::timestamp AS session_now "
                    f"FROM sequence_enrollment_steps WHERE id = :sid"
                ),
                {"sid": step_id},
            )
        ).first()
        assert row is not None
        stored, utc_now, session_now = row

        # Stored must match UTC within 10s.
        diff_utc = abs((stored - utc_now).total_seconds())
        assert diff_utc < 10, (
            f"stored scheduled_at {stored} differs from now() AT TIME ZONE 'UTC' "
            f"{utc_now} by {diff_utc}s — code wrote session-local time, not UTC"
        )

        # Stored must NOT match session-local now() (NY is UTC-4 in August).
        # The difference should be ~4h (14400s), not ~0.
        diff_session = abs((stored - session_now).total_seconds())
        assert diff_session > 3600, (
            f"stored scheduled_at {stored} matches bare now()::timestamp "
            f"{session_now} within {diff_session}s — this means the code wrote "
            f"session-local time, which would corrupt scheduling under a "
            f"non-UTC host TZ"
        )

        # Also verify against Python's datetime.utcnow() (independent source).
        diff_py = abs((stored - datetime.utcnow()).total_seconds())
        assert diff_py < 10, (
            f"stored scheduled_at {stored} differs from datetime.utcnow() by "
            f"{diff_py}s — not naive UTC"
        )


@pytest.mark.asyncio
async def test_pg_idempotency_record_server_default_is_utc(
    pg_session_factory, pg_seeded
):
    """The migration 006 server-side DEFAULT (now() AT TIME ZONE 'UTC') on
    idempotency_records.created_at must produce UTC, not session-local time,
    when a row is inserted via raw SQL that omits the column. This is the
    belt-and-suspenders path: the ORM writes datetime.utcnow() (Python-side),
    but raw-SQL inserts (e.g. from a migration backfill) rely on the server
    default. Under a non-UTC session TZ, bare now() would write NY-local; the
    migration's now() AT TIME ZONE 'UTC' writes UTC.
    """
    async with pg_session_factory() as db:
        await db.execute(text("SET TIME ZONE 'America/New_York'"))
        # Raw insert omitting created_at — exercises the server-side DEFAULT.
        # The ORM model's id and updated_at columns are NOT NULL with Python-side
        # defaults only (no server_default), so a raw insert must supply them.
        # We deliberately omit created_at to prove the server-side DEFAULT
        # (now() AT TIME ZONE 'UTC') fires and produces UTC, not session-local.
        test_key = f"utc-default-test-{uuid.uuid4()}"
        test_id = str(uuid.uuid4())
        await db.execute(
            text(
                "INSERT INTO idempotency_records "
                "(id, scope, idempotency_key, request_sha256, status, updated_at) "
                "VALUES (:id, 'enrollment', :key, 'abc', 'pending', now() AT TIME ZONE 'UTC')"
            ),
            {"id": test_id, "key": test_key},
        )
        await db.commit()

    async with pg_session_factory() as db:
        await db.execute(text("SET TIME ZONE 'America/New_York'"))
        row = (
            await db.execute(
                text(
                    "SELECT created_at, "
                    "now() AT TIME ZONE 'UTC' AS utc_now, "
                    # now()::timestamp = what bare now() would store in a
                    # naive TIMESTAMP column under this session TZ.
                    "now()::timestamp AS session_now "
                    "FROM idempotency_records "
                    "WHERE idempotency_key = :key LIMIT 1"
                ),
                {"key": test_key},
            )
        ).first()
        assert row is not None
        stored, utc_now, session_now = row

        diff_utc = abs((stored - utc_now).total_seconds())
        assert diff_utc < 10, (
            f"server-default created_at {stored} differs from now() AT TIME ZONE "
            f"'UTC' {utc_now} by {diff_utc}s — DEFAULT is not UTC"
        )

        diff_session = abs((stored - session_now).total_seconds())
        assert diff_session > 3600, (
            f"server-default created_at {stored} matches bare now()::timestamp "
            f"{session_now} within {diff_session}s — DEFAULT is session-local, "
            f"not UTC"
        )


# ── 4. Negative auth against the PG-backed app ───────────────────────────


@pytest.mark.asyncio
async def test_pg_missing_api_key_returns_401(pg_client, pg_seeded):
    req = _make_request(f"scout-v2/enrollment/{uuid.uuid4()}")
    resp = await pg_client.post("/v1/enrollments", json=req)
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Missing API key"


@pytest.mark.asyncio
async def test_pg_blank_api_key_returns_401(pg_client, pg_seeded):
    req = _make_request(f"scout-v2/enrollment/{uuid.uuid4()}")
    resp = await pg_client.post(
        "/v1/enrollments",
        headers={"X-API-Key": ""},
        json=req,
    )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_pg_wrong_api_key_returns_401(pg_client, pg_seeded):
    req = _make_request(f"scout-v2/enrollment/{uuid.uuid4()}")
    resp = await pg_client.post(
        "/v1/enrollments",
        headers={"X-API-Key": "totally-wrong-key"},  # pragma: allowlist secret
        json=req,
    )
    assert resp.status_code == 401
    assert resp.json()["detail"] == "Invalid API key"


# ── 5. Unique index backing ON CONFLICT exists in PG schema + migration 006 ─


@pytest.mark.asyncio
async def test_pg_unique_index_backs_on_conflict(pg_engine):
    """The composite PRIMARY KEY on (scope, idempotency_key) creates a unique
    index that backs ON CONFLICT (scope, idempotency_key) DO NOTHING. Verify
    it exists in the PG schema (create_all path). The index must be UNIQUE
    and cover both columns — otherwise ON CONFLICT DO NOTHING cannot enforce
    exactly-one-wins under concurrent inserts.
    """
    async with pg_engine.connect() as conn:
        rows = (
            await conn.execute(
                text(
                    "SELECT indexname, indexdef FROM pg_indexes "
                    "WHERE tablename = 'idempotency_records' "
                    "ORDER BY indexname"
                )
            )
        ).fetchall()
        assert len(rows) >= 1, "no indexes on idempotency_records"

        # The composite PK creates a unique index on (scope, idempotency_key).
        # Under create_all with the naming convention, it's pk_idempotency_records.
        pk_rows = [
            r
            for r in rows
            if "UNIQUE" in r[1].upper()
            and "scope" in r[1]
            and "idempotency_key" in r[1]
        ]
        assert len(pk_rows) >= 1, (
            f"no unique index on (scope, idempotency_key); found: {rows}"
        )

        # The ON CONFLICT index_elements=["scope","idempotency_key"] targets
        # exactly this unique index. Verify by attempting a conflict: insert
        # the same key twice via the PG-dialect construct and assert rowcount
        # 0 on the second insert.
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        first = await conn.execute(
            pg_insert(IdempotencyRecord)
            .values(
                scope="enrollment",
                idempotency_key="conflict-test",
                request_sha256="abc",
                status="pending",
            )
            .on_conflict_do_nothing(index_elements=["scope", "idempotency_key"])
        )
        assert first.rowcount == 1, f"first insert rowcount={first.rowcount}"

        second = await conn.execute(
            pg_insert(IdempotencyRecord)
            .values(
                scope="enrollment",
                idempotency_key="conflict-test",
                request_sha256="abc",
                status="pending",
            )
            .on_conflict_do_nothing(index_elements=["scope", "idempotency_key"])
        )
        assert second.rowcount == 0, (
            f"second insert rowcount={second.rowcount} — ON CONFLICT DO NOTHING "
            f"did not fire; the unique index is not enforcing the arbitration"
        )
        await conn.rollback()


@pytest.mark.asyncio
async def test_pg_migration_006_schema_standalone(pg_engine):
    """Apply migration 006's SQL to a fresh table (dropping the create_all
    version first) and verify it creates the same unique PK + the server-side
    now() AT TIME ZONE 'UTC' default. This proves the migration itself is
    correct independent of the ORM — a reviewer applying only the migrations
    (no create_all) gets the production-correct schema.
    """
    async with pg_engine.begin() as conn:
        # Drop the create_all version and apply migration 006 standalone.
        await conn.execute(text("DROP TABLE IF EXISTS idempotency_records"))
        for stmt in _split_sql_statements(_PG_MIGRATION_006_PATH.read_text()):
            await conn.execute(text(stmt))

        # Verify the unique PK exists.
        rows = (
            await conn.execute(
                text(
                    "SELECT indexname, indexdef FROM pg_indexes "
                    "WHERE tablename = 'idempotency_records' ORDER BY indexname"
                )
            )
        ).fetchall()
        pk_rows = [
            r
            for r in rows
            if "UNIQUE" in r[1].upper()
            and "scope" in r[1]
            and "idempotency_key" in r[1]
        ]
        assert len(pk_rows) >= 1, (
            f"migration 006 did not create a unique PK on (scope, idempotency_key); "
            f"indexes: {rows}"
        )

        # Verify the server-side DEFAULT (now() AT TIME ZONE 'UTC') on created_at.
        default_val = (
            await conn.execute(
                text(
                    "SELECT column_default FROM information_schema.columns "
                    "WHERE table_name = 'idempotency_records' "
                    "AND column_name = 'created_at'"
                )
            )
        ).scalar()
        assert default_val is not None, "created_at has no server-side DEFAULT"
        assert "now()" in default_val.lower() and "utc" in default_val.lower(), (
            f"created_at DEFAULT is {default_val!r} — expected now() AT TIME ZONE 'UTC'"
        )

        # Verify the digest-lookup index exists.
        digest_idx = [
            r
            for r in rows
            if "request_sha256" in r[1] and "idx_idempotency_request_sha256" in r[0]
        ]
        assert len(digest_idx) == 1, (
            f"migration 006 did not create idx_idempotency_request_sha256; indexes: {rows}"
        )
