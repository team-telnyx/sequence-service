"""Mailbox rotation service for weighted selection."""

import random
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.models import Mailbox, MailboxStatus
from src.config import TENANT_MAILBOX_MAP, ALL_ALLOWED_MAILBOXES


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
    Reserve a send slot by incrementing sent_today.
    
    Returns False if mailbox is at capacity.
    """
    result = await db.execute(
        select(Mailbox).where(Mailbox.id == mailbox_id)
    )
    mailbox = result.scalar_one_or_none()
    
    if not mailbox:
        return False
    
    if mailbox.sent_today >= mailbox.daily_send_limit:
        return False
    
    mailbox.sent_today += 1
    await db.commit()
    
    return True


async def reset_all_sent_today(db: AsyncSession, tenant_id: str) -> int:
    """Reset sent_today for all mailboxes in a tenant. Returns count updated."""
    result = await db.execute(
        update(Mailbox)
        .where(Mailbox.tenant_id == tenant_id)
        .values(sent_today=0)
    )
    await db.commit()
    return result.rowcount
