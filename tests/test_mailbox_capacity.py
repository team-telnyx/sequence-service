"""F4/F5 — mailbox capacity must be atomic and not leak on failed sends.

F4: reserve_send did SELECT → check sent_today < limit → sent_today += 1 → commit,
    with no row lock / atomic guard. At worker_concurrency > 1 two steps read the
    same sent_today and both increment, over-sending past daily_send_limit. Fix:
    a single conditional UPDATE ... WHERE sent_today < daily_send_limit and treat
    rowcount==0 as "at capacity".

F5: reserve_send committed sent_today += 1 BEFORE the Gmail send; a GmailError
    then failed the step but the increment stayed → every failed/bounced attempt
    permanently burned a slot. Fix: release_send() to give the slot back, called
    on send failure.
"""
import asyncio
import pytest

from src.services.mailbox_rotation import reserve_send, release_send
from src.models.models import Mailbox


async def _sent_today(session_factory, mb_id):
    async with session_factory() as s:
        m = await s.get(Mailbox, mb_id)
        return m.sent_today


@pytest.mark.asyncio
async def test_reserve_increments_when_below_limit(seeded, session_factory):
    async with session_factory() as s:
        ok = await reserve_send(s, seeded["active_mailbox_id"])  # 10/50
    assert ok is True
    assert await _sent_today(session_factory, seeded["active_mailbox_id"]) == 11


@pytest.mark.asyncio
async def test_reserve_fails_at_capacity(seeded, session_factory):
    async with session_factory() as s:
        ok = await reserve_send(s, seeded["full_mailbox_id"])  # 50/50
    assert ok is False
    assert await _sent_today(session_factory, seeded["full_mailbox_id"]) == 50  # unchanged


@pytest.mark.asyncio
async def test_reserve_never_exceeds_limit_under_concurrency(seeded, session_factory):
    # mb_active starts 10/50 → exactly 40 reservations should succeed, no more,
    # even when fired concurrently. The atomic conditional UPDATE guarantees this.
    async def one():
        async with session_factory() as s:
            return await reserve_send(s, seeded["active_mailbox_id"])
    results = await asyncio.gather(*[one() for _ in range(60)])
    assert sum(1 for r in results if r) == 40
    assert await _sent_today(session_factory, seeded["active_mailbox_id"]) == 50  # never over


@pytest.mark.asyncio
async def test_release_gives_slot_back(seeded, session_factory):
    async with session_factory() as s:
        await reserve_send(s, seeded["active_mailbox_id"])  # 10 → 11
    async with session_factory() as s:
        await release_send(s, seeded["active_mailbox_id"])  # 11 → 10
    assert await _sent_today(session_factory, seeded["active_mailbox_id"]) == 10


@pytest.mark.asyncio
async def test_release_floors_at_zero(seeded, session_factory):
    # mb_paused starts at 0 — releasing must not go negative.
    async with session_factory() as s:
        await release_send(s, "mb-paused")
    assert await _sent_today(session_factory, "mb-paused") == 0
