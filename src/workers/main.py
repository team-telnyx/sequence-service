"""ARQ worker configuration and task definitions."""

import asyncio
from arq import create_pool
from arq.connections import RedisSettings
import structlog

from src.config import get_settings
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
