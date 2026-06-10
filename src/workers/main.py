"""ARQ worker configuration and task definitions."""

import asyncio
from arq import create_pool, cron
from arq.connections import RedisSettings
import structlog

from src.config import get_settings
from src.workers.reconcile import reconcile_scheduled_steps
from src.workers.sequence_step import process_sequence_step
from src.workers.signal_detection import detect_signals, detect_signals_all_mailboxes
from src.workers.webhook_delivery import deliver_webhook

settings = get_settings()
logger = structlog.get_logger()


async def startup(ctx: dict) -> None:
    """Worker startup - initialize connections."""
    logger.info("Worker starting up")
    # Add any startup logic here (db connections, etc.)


async def shutdown(ctx: dict) -> None:
    """Worker shutdown - cleanup."""
    logger.info("Worker shutting down")


class WorkerSettings:
    """ARQ worker settings."""
    
    functions = [
        process_sequence_step,
        detect_signals,
        detect_signals_all_mailboxes,
        deliver_webhook,
        reconcile_scheduled_steps,
    ]

    # Re-enqueue enrollment steps stranded in SCHEDULED by a lost arq job (M4).
    # Runs every 10 min; the reconciler's grace window (>10 min) ensures it never
    # races a step that is simply waiting on its defer.
    cron_jobs = [
        cron(reconcile_scheduled_steps, minute=set(range(0, 60, 10)), run_at_startup=False),
    ]

    on_startup = startup
    on_shutdown = shutdown
    
    redis_settings = RedisSettings.from_dsn(settings.redis_url)
    
    # Queue settings
    max_jobs = settings.worker_concurrency
    job_timeout = 300  # 5 minutes
    
    # Retry settings
    max_tries = 3
    retry_defer_time = 30  # Start with 30s delay


if __name__ == "__main__":
    # Run worker directly
    from arq import run_worker
    run_worker(WorkerSettings)
