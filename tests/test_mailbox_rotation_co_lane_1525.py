"""REVOPS-1525 — the .co warm-up lane must be selectable by mailbox rotation.

The warm-up flip puts the 8 quinn.c–j@telnyx.co inboxes on the Email API
transport. SCOUT_MAILBOXES now includes them, so the hardcoded allowlist filter
in select_mailbox (mailbox_rotation.py:82-83) must NOT exclude a .co mailbox
that is ACTIVE with spare capacity. Conversely a PAUSED .co mailbox must not
be selected (the status filter is upstream of the allowlist filter, but the
combination is what production relies on).

These run ONLY against a real PostgreSQL database (gated on SEQUENCE_TEST_DB=1
via the pg_* fixtures in conftest.py). SQLite would also pass, but rotation
runs against PG in production and the migrations add the transport CHECK
constraint + the unique (tenant_id, email) index that the .co rows exercise —
proving the seed survives the real schema, not just the in-memory one.
"""

import pytest

from src.models.models import Mailbox, MailboxStatus
from src.services.mailbox_rotation import select_mailbox


@pytest.mark.asyncio
async def test_pg_active_co_mailbox_with_capacity_is_selectable(
    pg_session_factory, pg_seeded
):
    """An ACTIVE .co mailbox with spare daily capacity IS selectable by
    select_mailbox — the allowlist filter admits the .co warm-up lane."""
    async with pg_session_factory() as db:
        db.add(
            Mailbox(
                id="mb-co-active",
                tenant_id=pg_seeded["tenant_id"],
                email="quinn.c@telnyx.co",
                status=MailboxStatus.ACTIVE,
                weight=1,
                daily_send_limit=50,
                sent_today=0,
                transport="email_api",
            )
        )
        await db.commit()

    async with pg_session_factory() as db:
        selected = await select_mailbox(
            db,
            tenant_id=pg_seeded["tenant_id"],
            exclude_ids=["mb-active"],
            min_available=1,
        )
    assert selected is not None
    assert selected.email == "quinn.c@telnyx.co"


@pytest.mark.asyncio
async def test_pg_paused_co_mailbox_is_not_selectable(pg_session_factory, pg_seeded):
    """A PAUSED .co mailbox is NOT selectable even though it is in the
    allowlist — the status filter (MailboxStatus.ACTIVE) is upstream of the
    allowlist filter and must hold for the warm-up lane exactly as it does
    for the gmail lane."""
    async with pg_session_factory() as db:
        db.add(
            Mailbox(
                id="mb-co-paused",
                tenant_id=pg_seeded["tenant_id"],
                email="quinn.d@telnyx.co",
                status=MailboxStatus.PAUSED,
                weight=1,
                daily_send_limit=50,
                sent_today=0,
                transport="email_api",
            )
        )
        await db.commit()

    async with pg_session_factory() as db:
        selected = await select_mailbox(
            db,
            tenant_id=pg_seeded["tenant_id"],
            exclude_ids=["mb-active"],
            min_available=1,
        )
    # The only .co mailbox in the candidate pool is PAUSED → nothing selectable
    # once the seeded ACTIVE .com mailbox is excluded. None is the clean
    # "at-capacity / unavailable" signal — it must NOT fall back to the PAUSED
    # .co row just because the .co lane is now allowlisted.
    assert selected is None
