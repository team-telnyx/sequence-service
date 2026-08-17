"""Thin runner for the Email API events pull-poller (REVOPS-1525).

Runs ONE poll cycle per invocation (matches the poll_replies.py precedent —
launchd schedules this on a ~2-minute interval; see the PR body for the
plist wiring, NOT installed by this PR). fcntl.flock provides cross-process
single-flight so overlapping launchd kicks are a no-op.

Never crashes the host process: every exception is logged and the run exits
0. A non-zero exit would make launchd throttle/reschedule erratically. The
"freshness/ops alerting" for this feed is 1525-scope, not this PR — the
runner only logs.

Invocation:
    python -m src.jobs.email_events_poller
"""

from __future__ import annotations

import asyncio
import fcntl
import logging
import os
import sys

sys.path.insert(
    0,
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("email_events_poller")

LOCK_PATH = "/tmp/sequence-service-email-events-poller.lock"


async def _run() -> None:
    import httpx
    from dotenv import load_dotenv
    from sqlalchemy.ext.asyncio import (
        async_sessionmaker,
        create_async_engine,
    )

    from src.services.email_events_poller import POLLER_FEED, poll_once

    load_dotenv(
        os.path.join(
            os.path.dirname(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            ),
            ".env",
        )
    )

    db_url = os.getenv(
        "DATABASE_URL",
        "postgresql+asyncpg://kevinward@localhost:5432/sequence_service",
    )
    base_url = os.environ.get("EMAIL_API_BASE_URL", "https://api.telnyx.com/v2").rstrip(
        "/"
    )
    api_key = os.environ.get("EMAIL_API_KEY")
    timeout = float(os.environ.get("EMAIL_API_TIMEOUT", "30.0"))

    if not api_key:
        logger.warning("EMAIL_API_KEY not set — skipping poll")
        return

    engine = create_async_engine(db_url, echo=False)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            summary = await poll_once(
                session_factory, client, base_url, api_key, feed=POLLER_FEED
            )
            logger.info(
                "Email events poll complete",
                pages=summary.pages,
                processed=summary.processed,
                already_processed=summary.already_processed,
                unmatched=summary.unmatched,
                skipped=summary.skipped,
                errors=summary.errors,
                cursor_advanced=summary.cursor_advanced,
            )
    finally:
        await engine.dispose()


def main() -> None:
    try:
        lock_fd = os.open(LOCK_PATH, os.O_CREAT | os.O_RDWR, 0o644)
    except OSError as e:
        logger.warning(
            "Could not open poller lock file — running without lock", error=str(e)
        )
        asyncio.run(_run())
        return

    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (OSError, IOError):
        logger.info("Another email events poller instance is running — exiting")
        os.close(lock_fd)
        return

    try:
        asyncio.run(_run())
    except Exception as e:
        logger.error(
            "Poller run failed — swallowing to protect host process",
            error=str(e),
        )
    finally:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
        except OSError:
            pass
        os.close(lock_fd)


if __name__ == "__main__":
    main()
