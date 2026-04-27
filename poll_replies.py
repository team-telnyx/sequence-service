#!/usr/bin/env python3
"""
Reply polling trigger — queues detect_signals arq jobs for all active mailboxes.

Called every 15 minutes by launchd (com.scout.reply-polling).
This script queues the actual work; the arq worker processes it.
"""
import asyncio
import os
import sys
import logging

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger('poll_replies')

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))


async def main():
    import asyncpg
    from arq import create_pool
    from arq.connections import RedisSettings

    # Direct DB connection (no SQLAlchemy) to avoid async driver requirement
    db_url = os.getenv("DATABASE_URL", "postgresql://kevinward@localhost:5432/sequence_service")
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    gmail_enabled = os.getenv("GMAIL_ENABLED", "false").lower() == "true"

    if not gmail_enabled:
        logger.info("Gmail disabled — skipping reply polling")
        return

    pool = await asyncpg.create_pool(db_url, min_size=1, max_size=3)
    try:
        mailboxes = await pool.fetch(
            "SELECT id, email, tenant_id FROM mailboxes WHERE status = 'ACTIVE'"
        )
    finally:
        await pool.close()

    if not mailboxes:
        logger.info("No active mailboxes to poll")
        return

    redis_pool = await create_pool(RedisSettings.from_dsn(redis_url))
    queued = 0

    for mailbox in mailboxes:
        await redis_pool.enqueue_job(
            'detect_signals',
            mailbox_id=str(mailbox['id']),
            tenant_id=str(mailbox['tenant_id']),
        )
        queued += 1
        logger.info(f"Queued signal detection for {mailbox['email']}")

    await redis_pool.aclose()
    logger.info(f"Reply polling complete — queued {queued} detect_signals jobs")


if __name__ == "__main__":
    asyncio.run(main())
