"""Mailbox rotation service for weighted selection."""

import random
from datetime import datetime, timedelta, timezone

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.models import Mailbox, MailboxStatus
from src.config import TENANT_MAILBOX_MAP, ALL_ALLOWED_MAILBOXES

# Daily capacity (sent_today) is reset at 00:05 UTC (com.scout.mailbox-reset /
# the arq reset cron). When a mailbox is at the hard 75/day cap we defer the
# send to just after this reset rather than crashing. See next_capacity_reset.
CAPACITY_RESET_HOUR = 0
CAPACITY_RESET_MINUTE = 5


def next_capacity_reset(now: datetime | None = None) -> datetime:
    """Return the next UTC datetime at which daily send capacity resets (00:05 UTC).

    The seq-service capacity counter (Mailbox.sent_today) is zeroed at 00:05 UTC.
    A worker that finds its sticky mailbox at the hard cap re-queues its step to
    just after this reset instead of raising/retrying. If now is already inside
    the 00:00 to 00:05 window the reset is the SAME day's 00:05; otherwise it is
    the NEXT day's 00:05. now defaults to the current UTC time.
    """
    if now is None:
        now = datetime.now(timezone.utc)
    reset_today = now.replace(
        hour=CAPACITY_RESET_HOUR, minute=CAPACITY_RESET_MINUTE,
        second=0, microsecond=0,
    )
    if now < reset_today:
        return reset_today
    return reset_today + timedelta(days=1)


def seconds_until_capacity_reset(now: datetime | None = None) -> int:
    """Whole seconds from now until the next 00:05 UTC capacity reset (>=1)."""
    if now is None:
        now = datetime.now(timezone.utc)
    delta = (next_capacity_reset(now) - now).total_seconds()
    return max(1, int(delta))


async def select_mailbox(
    db: AsyncSession,
    tenant_id: str,
    exclude_ids: list[str] | None = None,
    min_available: int = 1,
) -> Mailbox | None:
    """
    Select a mailbox using weighted random selection.

    Considers:
    - Daily send limit vs sent today
    - Mailbox weight
    - Active status only
    - HARDCODED tenant mailbox allocation (cannot be bypassed)

    Returns None when no active, allowed mailbox has at least `min_available`
    capacity remaining. None is the clean "at-capacity / unavailable" signal the
    caller maps to a defer/429 (it never raises for capacity).
    """
    exclude_ids = exclude_ids or []

    # Get active mailboxes with available capacity
    query = (
        select(Mailbox)
        .where(
            Mailbox.tenant_id == tenant_id,
            Mailbox.status == MailboxStatus.ACTIVE,
            Mailbox.id.notin_(exclude_ids),
        )
    )

    result = await db.execute(query)
    mailboxes = result.scalars().all()

    # HARDCODED ENFORCEMENT: Only allow mailboxes in the tenant's allocation
    allowed_emails = TENANT_MAILBOX_MAP.get(tenant_id, ALL_ALLOWED_MAILBOXES)
    mailboxes = [m for m in mailboxes if m.email in allowed_emails]

    # Filter by available sends
    available = [
        m for m in mailboxes
        if (m.daily_send_limit - m.sent_today) >= min_available
    ]

    if not available:
        return None

    # Weighted random selection
    weights = []
    for mailbox in available:
        # Boost weight based on remaining capacity
        capacity_ratio = (mailbox.daily_send_limit - mailbox.sent_today) / mailbox.daily_send_limit
        adjusted_weight = mailbox.weight * (1 + capacity_ratio)
        weights.append(adjusted_weight)

    selected = random.choices(available, weights=weights, k=1)[0]
    return selected


async def reserve_send(db: AsyncSession, mailbox_id: str) -> bool:
    """
    Atomically reserve a send slot.

    F4: a single conditional UPDATE (sent_today += 1 WHERE sent_today <
    daily_send_limit) instead of SELECT-then-increment, so concurrent workers
    can never both read the same sent_today and over-send past the daily limit.
    rowcount == 1 means we got a slot; 0 means the mailbox was at capacity (or
    not found).

    The bool return is the clean at-capacity signal: False == "at the hard cap,
    no slot". Callers DEFER (re-queue to the next 00:05 reset) on False; the API
    layer maps it to 429. This function never raises for capacity, and the
    atomic 75/day enforcement is unchanged.
    """
    result = await db.execute(
        update(Mailbox)
        .where(
            Mailbox.id == mailbox_id,
            Mailbox.sent_today < Mailbox.daily_send_limit,
        )
        .values(sent_today=Mailbox.sent_today + 1)
    )
    await db.commit()
    return result.rowcount == 1


async def release_send(db: AsyncSession, mailbox_id: str) -> None:
    """
    Release a previously-reserved send slot (sent_today -= 1, floored at 0).

    F5: reserve_send runs before the Gmail send so we respect the cap up front,
    but a failed send must NOT permanently consume capacity. Callers release the
    slot when the send fails. Floored at 0 so a double-release never goes negative.
    """
    await db.execute(
        update(Mailbox)
        .where(Mailbox.id == mailbox_id, Mailbox.sent_today > 0)
        .values(sent_today=Mailbox.sent_today - 1)
    )
    await db.commit()


async def reset_all_sent_today(db: AsyncSession, tenant_id: str) -> int:
    """Reset sent_today for all mailboxes in a tenant. Returns count updated."""
    result = await db.execute(
        update(Mailbox)
        .where(Mailbox.tenant_id == tenant_id)
        .values(sent_today=0)
    )
    await db.commit()
    return result.rowcount
