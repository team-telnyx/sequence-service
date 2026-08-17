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
            # stdlib logger — %-format args, NOT structlog-style kwargs
            # (review round 1: structlog kwargs on a stdlib logger raised
            # TypeError → exit 1 on a routine 401).
            logger.info(
                "Email events poll complete pages=%d processed=%d "
                "already_processed=%d unmatched=%d skipped=%d errors=%d "
                "cursor_advanced=%s",
                summary.pages,
                summary.processed,
                summary.already_processed,
                summary.unmatched,
                summary.skipped,
                summary.errors,
                summary.cursor_advanced,
            )
    finally:
        await engine.dispose()


def main() -> None:
    try:
        lock_fd = os.open(LOCK_PATH, os.O_CREAT | os.O_RDWR, 0o644)
    except OSError as e:
        # Fail closed (review round 1): the previous branch fell through to
        # _run() WITHOUT the lock, allowing overlapping instances. If the
        # lock file cannot be opened, log and exit 0 without polling.
        logger.warning(
            "Could not open poller lock file — exiting without polling: %s", e
        )
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
        # stdlib logger — %-format arg, NOT structlog-style `error=` kwarg
        # (review round 1: structlog kwarg on a stdlib logger raised
        # TypeError → exit 1 on a routine failure that escaped _run()).
        logger.error("Poller run failed — swallowing to protect host process: %s", e)
    finally:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
        except OSError:
            pass
        os.close(lock_fd)


if __name__ == "__main__":
    main()
