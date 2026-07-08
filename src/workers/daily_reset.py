"""Daily mailbox counter reset (REVOPS-1231).

The sequence_service tracks per-mailbox `sent_today` to enforce the 75/day
cap. Without a daily reset, the counter accumulates indefinitely and all
mailboxes stay at capacity forever — compose stops enrolling, execute loops
skip everything, and the pipeline goes silent.

This cron resets `sent_today` to 0 for all Scout mailboxes at 00:05 UTC
(matching the assumption in enrollments.py line 32). The 5-minute offset
avoids racing any midnight boundary processing.
"""

import structlog

from src.models.base import async_session
from src.services.mailbox_rotation import reset_all_sent_today

logger = structlog.get_logger()

TENANT_ID = "tenant-scout"


async def reset_daily_send_counts(ctx: dict) -> dict:
    """Reset sent_today for all mailboxes. Returns {"reset": n}."""
    async with async_session() as db:
        count = await reset_all_sent_today(db, TENANT_ID)
    logger.info("daily_send_reset_complete", mailboxes_reset=count)
    return {"reset": count}
