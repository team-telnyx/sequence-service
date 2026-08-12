"""ARQ worker configuration and task definitions."""

import logging
from arq import cron
from arq.connections import RedisSettings
import structlog

from src.config import get_settings
from src.workers.circuit_resume import resume_circuit_breaker_paused
from src.workers.daily_reset import reset_daily_send_counts
from src.workers.reconcile import reconcile_scheduled_steps
from src.workers.sequence_step import process_sequence_step
from src.workers.signal_detection import detect_signals, detect_signals_all_mailboxes
from src.workers.webhook_delivery import deliver_webhook

settings = get_settings()

# REVOPS-1425: structlog's default wrapper does NO level filtering, so every
# debug/info line from every task module reaches the launchd log file
# (scout-arq-worker.log grew ~250MB/month). Filter at settings.log_level
# (INFO default) — get_logger() proxies bind lazily, so configuring here
# still covers loggers already created at import time in the modules above.
structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(
        getattr(logging, settings.log_level.upper(), logging.INFO)
    ),
)
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
        resume_circuit_breaker_paused,
        reset_daily_send_counts,
    ]

    cron_jobs = [
        # Re-enqueue steps stranded in SCHEDULED by a lost arq job (M4). Every 10 min;
        # the >10 min grace window ensures it never races a step waiting on its defer.
        cron(
            reconcile_scheduled_steps,
            minute=set(range(0, 60, 10)),
            run_at_startup=False,
        ),
        # Un-pause circuit_breaker-paused enrollments once the mailbox bounce rate
        # cools below the resume threshold. Every 30 min (offset from the reconciler).
        cron(resume_circuit_breaker_paused, minute={5, 35}, run_at_startup=False),
        # Reset all mailbox sent_today counters at 00:05 UTC (REVOPS-1231).
        # Without this, the daily cap accumulates and compose goes silent.
        cron(reset_daily_send_counts, hour=0, minute=5, run_at_startup=False),
    ]

    on_startup = startup
    on_shutdown = shutdown

    redis_settings = RedisSettings.from_dsn(settings.redis_url)

    # Queue settings
    max_jobs = settings.worker_concurrency
    job_timeout = 300  # 5 minutes

    # Retry settings — read from the shared config (src/config.py
    # worker_max_tries) so the worker's retry ceiling and the handler's
    # last-attempt terminal check (src/workers/sequence_step.py) never drift.
    # See the r4 finding: drift here left exhausted-retry rows SCHEDULED for
    # the reconciler to resurrect.
    max_tries = settings.worker_max_tries
    retry_defer_time = 30  # Start with 30s delay


if __name__ == "__main__":
    # Run worker directly
    from arq import run_worker

    run_worker(WorkerSettings)
