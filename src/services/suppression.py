"""Suppression list service — check before sending any email."""

from sqlalchemy import select, or_
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.models import Suppression


async def check_suppressed(db: AsyncSession, email: str, tenant_id: str) -> bool:
    """
    Check if an email is suppressed (exact match or domain-level).
    
    Returns True if the email should NOT be contacted.
    """
    email_lower = email.lower()
    domain = email_lower.split("@")[1] if "@" in email_lower else None
    
    conditions = [Suppression.email == email_lower]
    if domain:
        # Also check domain-level suppression
        conditions.append(Suppression.domain == domain)
    
    result = await db.execute(
        select(Suppression.id).where(
            Suppression.tenant_id == tenant_id,
            or_(*conditions),
        ).limit(1)
    )
    
    return result.scalar_one_or_none() is not None
