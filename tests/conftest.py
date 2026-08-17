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

# SQL to pre-create the suppression_reason enum type before Base.metadata.create_all
# runs. The ORM declares Enum(SuppressionReason, name="suppression_reason",
# create_type=False) — create_type=False means SQLAlchemy will NOT emit a
# CREATE TYPE for it during create_all (the migration owns it), so the type
# must already exist or CREATE TABLE suppressions fails on a fresh PG DB.
# All 7 values (4 base + 3 from migration 004) are created up front so create_all
# and the migration-applied schema agree.
_PG_CREATE_SUPPRESSION_REASON_ENUM = (
    "CREATE TYPE suppression_reason AS ENUM ("
    "  'UNSUBSCRIBE', 'BOUNCE', 'COMPLAINT', 'MANUAL',"
    "  'API_BOUNCE', 'API_COMPLAINT', 'API_UNSUBSCRIBE'"
    ")"
)

# SV2-044 r4 (FAIL 3): pre-create the reply_intent enum before create_all.
# The ORM declares Enum(ReplyIntent, values_callable=..., name="reply_intent",
# create_type=False) — create_type=False means SQLAlchemy will NOT emit a
# CREATE TYPE during create_all (migration 007 owns it), so the type must
# already exist or CREATE TABLE signals fails on a fresh PG DB. All 8
# docs/10 values (lowercase, matching migration 007's CREATE TYPE).
_PG_CREATE_REPLY_INTENT_ENUM = (
    "CREATE TYPE reply_intent AS ENUM ("
    "  'positive_interest', 'positive_meeting', 'negative_not_interested',"
    "  'negative_wrong_person', 'out_of_office', 'autoresponder',"
    "  'unsubscribe_request', 'unknown'"
    ")"
)

# SV2-044 r4 (FAIL 3): apply ALL migrations 001-007 (not just 006). The r3
# fixture applied only 006 standalone — that masked drift between the ORM
# and the other migrations (e.g. 007's reply_intent enum values diverging
# from the ORM's emission). r4 applies every migration in order against the
# create_all schema so any drift surfaces on the very next ORM insert.
_MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"
_MIGRATIONS_IN_ORDER = [
    "001_scout_only_collapse.sql",
    "002_mailbox_transport.sql",
    "003_enrollment_step_failed_status.sql",
    "004_suppression_api_reasons.sql",
    "005_sent_email_delivered_at.sql",
    "006_idempotency_records_sv2_044.sql",
    "007_signals_reply_intent_sv2_044.sql",
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
                lines = [line for line in stmt.splitlines() if not line.strip().startswith("--")]
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
        lines = [line for line in stmt.splitlines() if not line.strip().startswith("--")]
        cleaned = "\n".join(lines).strip()
        if cleaned:
            statements.append(cleaned)
    return statements


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
    """Fresh PG engine per test. Schema mirrors the production path: the app's
    lifespan hook runs Base.metadata.create_all on startup, so the fixture does
    the same — THEN applies ALL migrations 001-007 in order so the
    migration-applied schema matches what a real PostgreSQL deploy gets.

    SV2-044 r4 (FAIL 3): the r3 fixture applied ONLY migration 006 standalone
    — that masked drift between the ORM and the other migrations (e.g. 007's
    reply_intent enum values diverging from the ORM's emission). The r3
    ORM ``Enum(ReplyIntent, name="reply_intent", create_type=False)`` emitted
    the enum MEMBER NAMES (uppercase) by default, NOT the ``.value``
    (lowercase docs/10 strings), so an ORM insert of a Signal with a
    reply_intent raised ``InvalidTextRepresentationError: invalid input
    value for enum reply_intent: "UNKNOWN"`` against the migration-applied
    schema. r4:
      - The ORM column now uses ``values_callable`` so SQLAlchemy emits
        ``.value`` (matching migration 007's lowercase labels).
      - The fixture applies ALL migrations 001-007 (not just 006) so any
        drift between the ORM and ANY migration surfaces on the very next
        ORM insert — not just the one migration a standalone test exercises.
      - The ``reply_intent`` enum is pre-created (like ``suppression_reason``)
        so ``create_all`` can create the ``signals`` table with the
        ``reply_intent`` column referencing the type. Migration 007's
        ``CREATE TYPE`` then re-creates it (the fixture drops the
        pre-created type + column first so the migration owns them, same
        pattern as 006 for ``idempotency_records``).

    The DB is session-scoped (one disposable DB per pytest session) but the
    schema is fully reset per test via DROP SCHEMA CASCADE so every test starts
    from a clean slate — no cross-test contamination of idempotency records,
    enrollments, or constraint state.
    """
    eng = create_async_engine(_pg_db_url, echo=False)
    async with eng.begin() as conn:
        # Full schema reset — drops all tables, types, and indexes from the
        # public schema so every test starts clean. The session-scoped DB is
        # reused (avoiding CREATE/DROP DATABASE per test) but the schema is
        # disposable. CASCADE drops dependent objects (e.g. the suppression_reason
        # enum that the suppressions table depends on).
        await conn.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        await conn.execute(text("CREATE SCHEMA public"))
        # Pre-create the suppression_reason enum (create_type=False in the model).
        await conn.execute(text(_PG_CREATE_SUPPRESSION_REASON_ENUM))
        # r4: pre-create the reply_intent enum (create_type=False in the model —
        # migration 007 owns it). All 8 docs/10 lowercase values, matching
        # migration 007's CREATE TYPE. create_all can then create the signals
        # table with the reply_intent column referencing this type.
        await conn.execute(text(_PG_CREATE_REPLY_INTENT_ENUM))
        # Create all tables per the ORM — this is what the app's lifespan does
        # in production (src/api/main.py lifespan -> Base.metadata.create_all).
        # The other enum types (mailboxstatus, sequencestatus, etc.) are created
        # automatically since they use create_type=True by default.
        await conn.run_sync(Base.metadata.create_all)
        # r4: drop the create_all versions of migration-owned objects so the
        # migrations are the schema-of-record (production deploy applies the
        # migrations, not create_all). The fixture then re-applies ALL
        # migrations 001-007 in order. If a migration and the ORM drift, the
        # migration wins (production deploy applies the migration, not
        # create_all), so the fixture must run against the migration schema
        # to catch the drift — not mask it with create_all.
        await conn.execute(text("DROP TABLE IF EXISTS idempotency_records"))
        # Drop the create_all reply_intent column + type so migration 007's
        # CREATE TYPE + ADD COLUMN re-create them (proves the migration owns
        # the type + column, not create_all).
        await conn.execute(text("ALTER TABLE signals DROP COLUMN IF EXISTS reply_intent"))
        await conn.execute(text("DROP TYPE IF EXISTS reply_intent"))
        # r4: apply ALL migrations 001-007 in order. The smarter
        # _split_sql_statements handles $$ DO blocks in 001-005 (the r3
        # naive split-on-; broke on these).
        for mig_name in _MIGRATIONS_IN_ORDER:
            mig_path = _MIGRATIONS_DIR / mig_name
            for stmt in _split_sql_statements(mig_path.read_text()):
                await conn.execute(text(stmt))
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
