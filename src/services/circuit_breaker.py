"""Circuit breaker: auto-pause enrollments when mailbox bounce rate is too high."""

from datetime import datetime, timedelta
from typing import Optional

import structlog
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import get_settings
from src.models.models import (
    SentEmail,
    Signal,
    SignalType,
    SequenceEnrollment,
    EnrollmentStatus,
)

logger = structlog.get_logger()
settings = get_settings()


async def mailbox_bounce_rate(db: AsyncSession, mailbox_id: str) -> Optional[float]:
    """Bounce rate for a mailbox over the circuit-breaker window.

    Returns None when there are no sends in the window (no signal either way).
    Shared by the breaker (pause) and the auto-resume so both use one definition.
    """
    window_start = datetime.utcnow() - timedelta(hours=settings.circuit_breaker_window_hours)
    total_sends = (await db.execute(
        select(func.count(SentEmail.id)).where(
            SentEmail.mailbox_id == mailbox_id,
            SentEmail.sent_at >= window_start,
        )
    )).scalar() or 0
    if total_sends == 0:
        return None
    bounce_count = (await db.execute(
        select(func.count(Signal.id))
        .join(SentEmail, Signal.sent_email_id == SentEmail.id)
        .where(
            SentEmail.mailbox_id == mailbox_id,
            SentEmail.sent_at >= window_start,
            Signal.type == SignalType.BOUNCE,
        )
    )).scalar() or 0
    return bounce_count / total_sends


async def check_circuit_breaker(db: AsyncSession, mailbox_id: str, tenant_id: str) -> bool:
    """
    Check if the mailbox's bounce rate exceeds the threshold.

    Returns True if the circuit is TRIPPED (should NOT send).
    Returns False if safe to send.
    """
    if not settings.circuit_breaker_enabled:
        return False

    bounce_rate = await mailbox_bounce_rate(db, mailbox_id)
    if bounce_rate is None:
        return False

    if bounce_rate >= settings.circuit_breaker_threshold:
        logger.warning(
            "Circuit breaker TRIPPED",
            mailbox_id=mailbox_id,
            bounce_rate=bounce_rate,
            threshold=settings.circuit_breaker_threshold,
        )
        await _pause_enrollments_for_mailbox(db, mailbox_id, tenant_id)
        return True

    return False


async def _pause_enrollments_for_mailbox(
    db: AsyncSession, mailbox_id: str, tenant_id: str
) -> int:
    """Pause all active enrollments using this mailbox and fire webhooks."""
    from src.services.webhooks import create_enrollment_webhook

    result = await db.execute(
        select(SequenceEnrollment).where(
            SequenceEnrollment.mailbox_id == mailbox_id,
            SequenceEnrollment.status == EnrollmentStatus.ACTIVE,
        )
    )
    enrollments = result.scalars().all()

    count = 0
    for enrollment in enrollments:
        enrollment.status = EnrollmentStatus.PAUSED
        enrollment.pause_reason = "circuit_breaker"
        count += 1

    await db.commit()

    # Fire webhooks after commit
    for enrollment in enrollments:
        try:
            await create_enrollment_webhook(db, enrollment, "paused")
        except Exception as e:
            logger.error("Failed to fire circuit_breaker webhook", error=str(e))

    logger.info(
        "Circuit breaker paused enrollments",
        mailbox_id=mailbox_id,
        count=count,
    )
    return count
