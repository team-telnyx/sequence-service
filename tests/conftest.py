"""Shared pytest fixtures for sequence-service tests.

The fast unit path uses an in-memory SQLite database (aiosqlite) with the real
SQLAlchemy models, seeded with realistic tenant / sequence / mailbox rows.
The FastAPI `get_db` dependency is overridden to use the test session, and the
ARQ queue call is mocked so tests never touch Redis.

SV2-044 r2 — PostgreSQL integration fixtures:
A second fixture set (`pg_engine` / `pg_session_factory` / `pg_seeded` /
`pg_client`) runs the security-relevant tests against a real, disposable
PostgreSQL database. SQLite serializes writers with a global lock, so a
"exactly one enrollment under concurrent requests" proof on SQLite does NOT
prove the PostgreSQL behavior — PG enforces the unique-constraint race under
row locks, which is the actual production path. The PG fixtures are opt-in via
`SEQUENCE_TEST_DB=1` (mirrors scout-outbound-v2's `SCOUT_TEST_DB` pattern); when
the flag is unset, every PG-marked test skips cleanly and only the SQLite unit
path runs. The naive-UTC TIMESTAMP column defaults are also exercised under a
non-UTC session TZ (America/New_York) to prove the 5-hour host-offset skew
cannot corrupt scheduling/capacity logic.
"""

import os
import re
import uuid
from pathlib import Path
from unittest.mock import AsyncMock

import asyncpg
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.api.main import app
from src.models.base import Base, get_db
from src.models.models import (
    Tenant,
    Mailbox,
    MailboxStatus,
    Sequence,
    SequenceStatus,
    SequenceStep,
)

# tenant-scout's allowlisted mailboxes (see src/config.py SCOUT_MAILBOXES)
SCOUT_TENANT_ID = "tenant-scout"
SCOUT_API_KEY = "test-scout-key"


@pytest_asyncio.fixture
async def engine():
    """Fresh in-memory SQLite engine per test, with all tables created."""
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
    """Seed a scout tenant, a sequence with 2 steps, and several mailboxes.

    Mailboxes (all tenant-scout allowlisted emails) with varied capacity:
      - quinn.c: ACTIVE, capacity 50 - 10 = 40   (allowed + capacity)
      - quinn.d: ACTIVE, capacity 50 - 50 = 0     (allowed but AT CAPACITY)
      - quinn.e: PAUSED, capacity 50 - 0  = 50     (allowed but NOT active)
      - quinn.f: ACTIVE, capacity 50 - 5  = 45     (rotation fodder)
    Returns a dict of the seeded ids/emails for assertions.
    """
    async with session_factory() as db:
        tenant = Tenant(id=SCOUT_TENANT_ID, name="Scout", api_key=SCOUT_API_KEY)
        db.add(tenant)

        mb_active = Mailbox(
            id="mb-active",
            tenant_id=SCOUT_TENANT_ID,
            email="quinn.c@telnyx.com",
            status=MailboxStatus.ACTIVE,
            weight=1,
            daily_send_limit=50,
            sent_today=10,
        )
        mb_full = Mailbox(
            id="mb-full",
            tenant_id=SCOUT_TENANT_ID,
            email="quinn.d@telnyx.com",
            status=MailboxStatus.ACTIVE,
            weight=1,
            daily_send_limit=50,
            sent_today=50,
        )
        mb_paused = Mailbox(
            id="mb-paused",
            tenant_id=SCOUT_TENANT_ID,
            email="quinn.e@telnyx.com",
            status=MailboxStatus.PAUSED,
            weight=1,
            daily_send_limit=50,
            sent_today=0,
        )
        mb_other = Mailbox(
            id="mb-other",
            tenant_id=SCOUT_TENANT_ID,
            email="quinn.f@telnyx.com",
            status=MailboxStatus.ACTIVE,
            weight=1,
            daily_send_limit=50,
            sent_today=5,
        )
        db.add_all([mb_active, mb_full, mb_paused, mb_other])

        seq = Sequence(
            id="seq-1",
            tenant_id=SCOUT_TENANT_ID,
            name="Test Seq",
            status=SequenceStatus.ACTIVE,
        )
        db.add(seq)
        db.add(
            SequenceStep(
                id="step-1",
                sequence_id="seq-1",
                step_number=1,
                subject="Hi",
                body="Body 1",
            )
        )
        db.add(
            SequenceStep(
                id="step-2",
                sequence_id="seq-1",
                step_number=2,
                subject="Follow up",
                body="Body 2",
            )
        )
        await db.commit()

    return {
        "tenant_id": SCOUT_TENANT_ID,
        "api_key": SCOUT_API_KEY,
        "sequence_id": "seq-1",
        "active_mailbox_id": "mb-active",
        "active_mailbox_email": "quinn.c@telnyx.com",
        "full_mailbox_id": "mb-full",
        "full_mailbox_email": "quinn.d@telnyx.com",
        "paused_mailbox_email": "quinn.e@telnyx.com",
        # quinn.a is a QUINN mailbox -> NOT in tenant-scout allowlist
        "not_allowed_email": "quinn.a@telnyx.com",
    }


@pytest_asyncio.fixture
async def client(session_factory, monkeypatch):
    """AsyncClient wired to the app with get_db overridden and queue mocked.

    SV2-044: auth is now env-var-based (SEQUENCE_SERVICE_API_KEY), not DB-tenant.
    The middleware reads settings.sequence_service_api_key; patch it to the
    test key so authenticated requests succeed. The old async_session DB-lookup
    patch is no longer needed (the middleware no longer opens a DB session for auth).
    """
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


# ── PostgreSQL integration fixtures (SV2-044 r2) ──────────────────────────
#
# Gated on SEQUENCE_TEST_DB=1. The in-memory SQLite path stays the fast unit
# default; the PG path is opt-in for tests that prove behavior SQLite masks
# (real unique-constraint races, naive-UTC TIMESTAMP defaults under a non-UTC
# session TZ). Without SEQUENCE_TEST_DB, every PG-marked test skips cleanly.

PG_TEST_ENABLED = os.environ.get("SEQUENCE_TEST_DB", "") in ("1", "true", "True")
PG_ADMIN_DSN = os.environ.get(
    "SEQUENCE_TEST_ADMIN_URL", "postgresql://kevinward@localhost:5432/postgres"
)

# SV2-044 r5 (FAIL 2): pure-migration PG fixture — NO create_all.
# The r4 fixture ran Base.metadata.create_all then dropped+re-applied only
# the SV2-044 tables. That hybrid masked migration/ORM drift in other tables
# (transport, delivered_at, external_ref, enum values) because create_all
# created the ORM version and the migrations' ADD COLUMN IF NOT EXISTS was a
# no-op. r5 adds a baseline migration 000_baseline.sql that creates the
# pre-migration base schema (everything create_all would produce for tables
# NOT covered by 001-007), so the full chain 000->007 reproduces the COMPLETE
# schema with NO create_all. A drift assertion compares the final schema to
# the ORM metadata so any future ORM/migration divergence fails loudly.
_MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"
_MIGRATIONS_IN_ORDER = [
    "000_baseline.sql",
    "001_scout_only_collapse.sql",
    "002_mailbox_transport.sql",
    "003_enrollment_step_failed_status.sql",
    "004_suppression_api_reasons.sql",
    "005_sent_email_delivered_at.sql",
    "006_idempotency_records_sv2_044.sql",
    "007_signals_reply_intent_sv2_044.sql",
    "008_email_events_poller_cursor.sql",
]

# Migration 006 — kept for the standalone test
# (test_sv2_044_pg_contract.test_pg_migration_006_schema_standalone).
_PG_MIGRATION_006_PATH = _MIGRATIONS_DIR / "006_idempotency_records_sv2_044.sql"


def _split_sql_statements(sql_text: str) -> list[str]:
    """Split a SQL script into individual statements for execution.

    SV2-044 r4 (FAIL 3): the r3 splitter was a naive split on ';' — sufficient
    for migration 006 (no DO blocks) but it BREAKS migrations 001-005 which
    contain ``DO $$ ... END $$`` blocks with semicolons inside the function
    body. r4 adds proper handling for:
      - ``$$`` and ``$tag$`` dollar-quoted strings (may contain ';')
      - ``'...'`` single-quoted strings with ``''`` escape (may contain ';')
      - ``--`` line comments (may contain ';')
    so a ';' inside a DO block / string / comment does NOT split the
    statement. Strips full-line ``--`` comments from each statement;
    inline comments are preserved (PG handles them).
    """
    statements: list[str] = []
    current: list[str] = []
    i = 0
    n = len(sql_text)
    while i < n:
        # Line comment — skip to end of line (a ';' inside does not split).
        if sql_text[i : i + 2] == "--":
            eol = sql_text.find("\n", i)
            if eol == -1:
                current.append(sql_text[i:])
                i = n
            else:
                current.append(sql_text[i : eol + 1])
                i = eol + 1
            continue

        # Dollar-quoted string: $tag$ ... $tag$ (tag may be empty: $$).
        if sql_text[i] == "$":
            m = re.match(r"\$[A-Za-z_0-9]*\$", sql_text[i:])
            if m:
                tag = m.group()
                current.append(tag)
                i += len(tag)
                close = sql_text.find(tag, i)
                if close == -1:
                    # Unterminated — append the rest verbatim.
                    current.append(sql_text[i:])
                    i = n
                else:
                    current.append(sql_text[i:close])
                    current.append(tag)
                    i = close + len(tag)
                continue

        # Single-quoted string: '...' with '' as escape.
        if sql_text[i] == "'":
            current.append("'")
            i += 1
            while i < n:
                if sql_text[i] == "'":
                    if i + 1 < n and sql_text[i + 1] == "'":
                        current.append("''")
                        i += 2
                    else:
                        current.append("'")
                        i += 1
                        break
                else:
                    current.append(sql_text[i])
                    i += 1
            continue

        ch = sql_text[i]
        if ch == ";":
            stmt = "".join(current).strip()
            if stmt:
                lines = [
                    line
                    for line in stmt.splitlines()
                    if not line.strip().startswith("--")
                ]
                cleaned = "\n".join(lines).strip()
                if cleaned:
                    statements.append(cleaned)
            current = []
            i += 1
        else:
            current.append(ch)
            i += 1

    stmt = "".join(current).strip()
    if stmt:
        lines = [
            line for line in stmt.splitlines() if not line.strip().startswith("--")
        ]
        cleaned = "\n".join(lines).strip()
        if cleaned:
            statements.append(cleaned)
    return statements


# SV2-044 r5 (FAIL 2): drift assertion — compares the migration-derived schema
# to the ORM metadata so any future ORM/migration divergence fails loudly.
# Compares column names, PG types, nullability, VARCHAR lengths, DateTime
# tz-awareness, and enum value sets. Does NOT compare constraint/index names
# (they differ between create_all naming convention and migration explicit
# names) — only semantic structure.
#
# SV2-044 r6 (FAIL 2): the r5 checker was LENIENT — it mapped ORM class names
# ``VARCHAR``/``TIMESTAMP`` but the actual SQLAlchemy class names are
# ``String``/``DateTime`` (uppercased to ``STRING``/``DATETIME``), so the
# lookup returned None and the entire String/DateTime branch was SKIPPED. It
# also accepted both ``json`` and ``jsonb`` for ``JSON`` columns. That
# leniency masked 6 real migration/ORM mismatches (TEXT vs VARCHAR(n), JSON
# vs JSONB). r6 makes the checker STRICT:
#   - ``String(n)`` → assert DB is ``character varying(n)`` with the SAME
#     length n (None for unbounded). Fails on ``text`` or any length mismatch.
#   - ``DateTime`` → assert DB is ``timestamp without time zone`` (naive) or
#     ``timestamp with time zone`` (tz-aware), matching ``col.type.timezone``.
#     No skip.
#   - ``JSON`` → ``json`` ONLY; ``JSONB`` → ``jsonb`` ONLY. No cross-accept.
#   - Unknown ORM types FAIL (no silent skip — a new type must be added to
#     the map explicitly so it can never drift unnoticed).

# ORM type class name (uppercased) → expected PG information_schema.data_type.
# The key is type(col_obj.type).__name__.upper() — e.g. ``String`` → ``STRING``.
_ORM_TYPE_TO_PG = {
    "STRING": "character varying",
    "TEXT": "text",
    "INTEGER": "integer",
    "BOOLEAN": "boolean",
    "JSON": "json",
    "JSONB": "jsonb",
}

# Enum type name → expected values (after migrations 000..007 applied).
# mailboxstatus/sequencestatus/etc. use MEMBER NAMES (uppercase).
# reply_intent uses .value (lowercase, via values_callable).
# suppression_reason: 4 base + 3 from migration 004.
# enrollmentstepstatus: 5 base + 1 (FAILED) from migration 003.
_EXPECTED_ENUM_VALUES = {
    "mailboxstatus": {"ACTIVE", "PAUSED", "WARMING", "DISABLED"},
    "sequencestatus": {"DRAFT", "ACTIVE", "PAUSED", "ARCHIVED"},
    "enrollmentstatus": {"ACTIVE", "PAUSED", "COMPLETED", "BOUNCED", "UNSUBSCRIBED"},
    "enrollmentstepstatus": {
        "PENDING",
        "SCHEDULED",
        "SENT",
        "SKIPPED",
        "BOUNCED",
        "FAILED",
    },
    "signaltype": {"REPLY", "OPEN", "CLICK", "BOUNCE", "UNSUBSCRIBE", "OUT_OF_OFFICE"},
    "suppression_reason": {
        "UNSUBSCRIBE",
        "BOUNCE",
        "COMPLAINT",
        "MANUAL",
        "API_BOUNCE",
        "API_COMPLAINT",
        "API_UNSUBSCRIBE",
    },
    "reply_intent": {
        "positive_interest",
        "positive_meeting",
        "negative_not_interested",
        "negative_wrong_person",
        "out_of_office",
        "autoresponder",
        "unsubscribe_request",
        "unknown",
    },
}


async def _assert_schema_matches_orm(conn) -> None:
    """Assert the migration-derived schema matches the ORM metadata.

    Catches: missing/extra columns, wrong types, wrong VARCHAR lengths, wrong
    DateTime tz-awareness, wrong nullability, missing/extra enum values, and
    unknown ORM types (no silent skip). Does NOT compare constraint names
    (naming convention differences between create_all and migrations are
    expected).

    SV2-044 r6 (FAIL 2): STRICT — String(n) checks character_maximum_length,
    DateTime checks tz-awareness, JSON/JSONB are strict (no cross-accept),
    and unknown types FAIL instead of being silently skipped. Dispatches on
    the exact type class name (not isinstance) because SQLAlchemy's ``Text``
    subclasses ``String`` — isinstance would mis-route Text columns into the
    varchar branch.
    """
    import src.models.models  # noqa: F401 — register all models

    for table_name, table_obj in Base.metadata.tables.items():
        rows = (
            await conn.execute(
                text(
                    "SELECT column_name, data_type, udt_name, is_nullable, "
                    "character_maximum_length "
                    "FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = :t "
                    "ORDER BY ordinal_position"
                ),
                {"t": table_name},
            )
        ).fetchall()
        db_cols = {r[0]: r for r in rows}
        orm_cols = {c.name: c for c in table_obj.columns}
        assert set(db_cols) == set(orm_cols), (
            f"drift: table {table_name} column mismatch — "
            f"DB has {set(db_cols)}, ORM has {set(orm_cols)}, "
            f"missing_in_db={set(orm_cols) - set(db_cols)}, "
            f"missing_in_orm={set(db_cols) - set(orm_cols)}"
        )
        for col_name, col_obj in orm_cols.items():
            db_col_name, db_data_type, db_udt_name, db_is_nullable, db_char_len = (
                db_cols[col_name]
            )
            # Dispatch on the EXACT type class name, not isinstance —
            # SQLAlchemy's Text subclasses String, so isinstance would
            # misroute Text columns into the varchar branch.
            type_name = type(col_obj.type).__name__
            if type_name == "Enum":
                assert db_data_type == "USER-DEFINED", (
                    f"drift: {table_name}.{col_name} expected enum (USER-DEFINED), "
                    f"got {db_data_type}"
                )
                assert db_udt_name == col_obj.type.name, (
                    f"drift: {table_name}.{col_name} enum type name mismatch — "
                    f"DB has {db_udt_name!r}, ORM has {col_obj.type.name!r}"
                )
            elif type_name == "DateTime":
                if col_obj.type.timezone:
                    expected_dt = "timestamp with time zone"
                else:
                    expected_dt = "timestamp without time zone"
                assert db_data_type == expected_dt, (
                    f"drift: {table_name}.{col_name} DateTime tz mismatch — "
                    f"DB has {db_data_type!r}, expected {expected_dt!r} "
                    f"(ORM timezone={col_obj.type.timezone})"
                )
            elif type_name == "String":
                assert db_data_type == "character varying", (
                    f"drift: {table_name}.{col_name} type mismatch — "
                    f"DB has {db_data_type!r}, expected 'character varying' "
                    f"(ORM String, length={col_obj.type.length})"
                )
                orm_length = col_obj.type.length
                assert orm_length == db_char_len, (
                    f"drift: {table_name}.{col_name} VARCHAR length mismatch — "
                    f"DB has character_maximum_length={db_char_len}, "
                    f"ORM has length={orm_length}"
                )
            else:
                type_key = type_name.upper()
                expected_pg = _ORM_TYPE_TO_PG.get(type_key)
                assert expected_pg is not None, (
                    f"drift: {table_name}.{col_name} has unmapped ORM type "
                    f"{type_key!r} ({type(col_obj.type).__module__}."
                    f"{type(col_obj.type).__name__}) — add it to _ORM_TYPE_TO_PG "
                    f"or handle it explicitly above; the checker must not "
                    f"silently skip any type"
                )
                assert db_data_type == expected_pg, (
                    f"drift: {table_name}.{col_name} type mismatch — "
                    f"DB has {db_data_type!r}, expected {expected_pg!r} "
                    f"(ORM type {type_key})"
                )
            expected_nullable = "YES" if col_obj.nullable else "NO"
            assert db_is_nullable == expected_nullable, (
                f"drift: {table_name}.{col_name} nullability mismatch — "
                f"DB has {db_is_nullable!r}, ORM has {expected_nullable!r}"
            )

    for enum_name, expected_vals in _EXPECTED_ENUM_VALUES.items():
        rows = (
            await conn.execute(
                text(
                    "SELECT e.enumlabel FROM pg_enum e "
                    "JOIN pg_type t ON e.enumtypid = t.oid "
                    "WHERE t.typname = :n ORDER BY e.enumsortorder"
                ),
                {"n": enum_name},
            )
        ).fetchall()
        db_vals = {r[0] for r in rows}
        assert db_vals == expected_vals, (
            f"drift: enum {enum_name} values mismatch — "
            f"DB has {db_vals}, expected {expected_vals}, "
            f"missing={expected_vals - db_vals}, extra={db_vals - expected_vals}"
        )


@pytest_asyncio.fixture(scope="session")
async def _pg_db_url():
    """Session-scoped disposable PG DB URL. Skips the whole session if disabled.

    If SEQUENCE_TEST_DATABASE_URL is set, the caller owns that DB (schema is
    applied per-test but the DB is NOT dropped). Otherwise a fresh DB named
    sequence_test_<pid>_<uuid> is created on the admin server and dropped with
    WITH (FORCE) on session teardown.
    """
    if not PG_TEST_ENABLED:
        pytest.skip("SEQUENCE_TEST_DB not set; PG integration tests are opt-in")

    override = os.environ.get("SEQUENCE_TEST_DATABASE_URL")
    if override:
        yield override
        return

    admin = await asyncpg.connect(PG_ADMIN_DSN)
    db_name = f"sequence_test_{os.getpid()}_{uuid.uuid4().hex[:8]}"
    try:
        await admin.execute(f'CREATE DATABASE "{db_name}"')
    finally:
        await admin.close()

    yield f"postgresql+asyncpg://kevinward@localhost:5432/{db_name}"

    admin = await asyncpg.connect(PG_ADMIN_DSN)
    try:
        await admin.execute(f'DROP DATABASE IF EXISTS "{db_name}" WITH (FORCE)')
    finally:
        await admin.close()


@pytest_asyncio.fixture
async def pg_engine(_pg_db_url):
    """Fresh PG engine per test. Schema is PURELY migration-derived (r5):
    applies migrations 000..007 on a fresh DB with NO create_all.

    SV2-044 r5 (FAIL 2): the r4 fixture ran Base.metadata.create_all then
    dropped+re-applied only the SV2-044 tables. That hybrid masked
    migration/ORM drift in other tables (transport, delivered_at,
    external_ref, enum values) because create_all created the ORM version
    and the migrations' ADD COLUMN IF NOT EXISTS was a no-op. r5 adds a
    baseline migration 000_baseline.sql that creates the pre-migration base
    schema, so the full chain 000->007 reproduces the COMPLETE schema with
    NO create_all. A drift assertion (_assert_schema_matches_orm) compares
    the final schema to the ORM metadata so any future ORM/migration
    divergence fails loudly.

    The DB is session-scoped (one disposable DB per pytest session) but the
    schema is fully reset per test via DROP SCHEMA CASCADE so every test starts
    from a clean slate — no cross-test contamination.
    """
    eng = create_async_engine(_pg_db_url, echo=False)
    async with eng.begin() as conn:
        await conn.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        for mig_name in _MIGRATIONS_IN_ORDER:
            mig_path = _MIGRATIONS_DIR / mig_name
            for stmt in _split_sql_statements(mig_path.read_text()):
                await conn.execute(text(stmt))
        await _assert_schema_matches_orm(conn)
    yield eng
    await eng.dispose()


@pytest_asyncio.fixture
async def pg_session_factory(pg_engine):
    return async_sessionmaker(pg_engine, expire_on_commit=False)


@pytest_asyncio.fixture
async def pg_seeded(pg_session_factory):
    """Same seed shape as `seeded` but on PG — minimal (one tenant, one
    ACTIVE mailbox with spare capacity). Used by all PG-marked contract tests.
    """
    async with pg_session_factory() as db:
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
async def pg_client(pg_session_factory, pg_seeded, monkeypatch):
    """AsyncClient wired to the app with get_db overridden to the PG session
    factory. Same auth + queue mocking as the SQLite `client` fixture.
    """
    import src.api.enrollments as enrollments_mod
    import src.api.main as main_mod

    monkeypatch.setattr(
        enrollments_mod,
        "queue_sequence_step",
        AsyncMock(return_value="job-test"),
    )
    monkeypatch.setattr(main_mod.settings, "sequence_service_api_key", SCOUT_API_KEY)

    async def _override_get_db():
        async with pg_session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override_get_db
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
    app.dependency_overrides.clear()
