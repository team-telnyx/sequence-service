"""Shared pytest fixtures for sequence-service tests.

Uses an in-memory SQLite database (aiosqlite) with the real SQLAlchemy models,
seeded with realistic tenant / sequence / mailbox rows. The FastAPI `get_db`
dependency is overridden to use the test session, and the ARQ queue call is
mocked so tests never touch Redis.
"""

import uuid
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
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
            id="mb-active", tenant_id=SCOUT_TENANT_ID, email="quinn.c@telnyx.com",
            status=MailboxStatus.ACTIVE, weight=1, daily_send_limit=50, sent_today=10,
        )
        mb_full = Mailbox(
            id="mb-full", tenant_id=SCOUT_TENANT_ID, email="quinn.d@telnyx.com",
            status=MailboxStatus.ACTIVE, weight=1, daily_send_limit=50, sent_today=50,
        )
        mb_paused = Mailbox(
            id="mb-paused", tenant_id=SCOUT_TENANT_ID, email="quinn.e@telnyx.com",
            status=MailboxStatus.PAUSED, weight=1, daily_send_limit=50, sent_today=0,
        )
        mb_other = Mailbox(
            id="mb-other", tenant_id=SCOUT_TENANT_ID, email="quinn.f@telnyx.com",
            status=MailboxStatus.ACTIVE, weight=1, daily_send_limit=50, sent_today=5,
        )
        db.add_all([mb_active, mb_full, mb_paused, mb_other])

        seq = Sequence(
            id="seq-1", tenant_id=SCOUT_TENANT_ID, name="Test Seq",
            status=SequenceStatus.ACTIVE,
        )
        db.add(seq)
        db.add(SequenceStep(id="step-1", sequence_id="seq-1", step_number=1,
                            subject="Hi", body="Body 1"))
        db.add(SequenceStep(id="step-2", sequence_id="seq-1", step_number=2,
                            subject="Follow up", body="Body 2"))
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
    """AsyncClient wired to the app with get_db overridden and queue mocked."""
    # Avoid Redis: stub the queue call used by create_enrollment.
    import src.api.enrollments as enrollments_mod
    monkeypatch.setattr(
        enrollments_mod, "queue_sequence_step",
        AsyncMock(return_value="job-test"),
    )

    async def _override_get_db():
        async with session_factory() as session:
            yield session

    # The tenant-auth middleware also opens its own async_session() to look up
    # the tenant by api key; point that at the test engine too.
    monkeypatch.setattr("src.api.main.async_session", session_factory)

    app.dependency_overrides[get_db] = _override_get_db
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
    app.dependency_overrides.clear()
