"""Queue management for ARQ jobs."""

from arq import create_pool
from arq.connections import RedisSettings
from datetime import datetime, timedelta
from typing import Optional

import structlog

from src.config import get_settings

settings = get_settings()
logger = structlog.get_logger()

_pool = None


async def get_redis_pool():
    """Get or create ARQ Redis pool."""
    global _pool
    if _pool is None:
        _pool = await create_pool(RedisSettings.from_dsn(settings.redis_url))
    return _pool


async def queue_sequence_step(
    enrollment_step_id: str,
    tenant_id: str,
    scheduled_at: datetime,
    delay_seconds: Optional[int] = None,
) -> Optional[str]:
    """Queue a sequence step for processing with deterministic deduplication.

    The ``_job_id`` is derived from ``(enrollment_step_id, scheduled_at)`` so
    repeat enqueues for the SAME intended fire time collapse into one arq job
    (arq deduplicates by ``job_id`` only). Without this, every re-enqueue of a
    step — reconciler sweep, circuit_resume, retry paths — minted a fresh
    uuid4 and arq's dedup was completely off. Live measurement 2026-07-20:
    83,286 queued arq jobs against 2,927 live SCHEDULED steps (~27 duplicate
    jobs per step), plausibly driving the detect_signals TimeoutError/KeyError
    storm and legitimate sends slipping past-due.

    ``scheduled_at`` is in the key (NOT just ``enrollment_step_id``) so a
    genuine re-schedule after a prior job completed produces a NEW id. A bare
    ``f"step:{id}"`` would be refused by arq's result store (keep_result TTL)
    forever once a job ran, stranding any step that legitimately needed
    re-enqueueing after its previous job completed — the reconciler's rescue
    path. Including ``scheduled_at`` means the reconciler (which stamps
    ``scheduled_at=now`` before enqueueing) always produces a new id while
    duplicates for the same fire time collapse. This distinction is the whole
    point of the fix — do not simplify to a bare step id.

    arq's ``enqueue_job`` returns ``None`` when a job with this ``_job_id`` is
    already queued. On ``None`` we log at debug and return ``None`` (NOT the
    deterministic id) so callers that need to distinguish "newly enqueued"
    from "deduped" (the reconciler counts only newly-enqueued steps) can do
    so by checking for None. The two callers that capture the return for
    logging only (enrollments.create_enrollment, sequence_step._queue_next_step)
    are already None-safe — they pass the value into a logger or a dict.

    ``scheduled_at`` is REQUIRED (no default) — every caller already stamps
    the step's ``scheduled_at`` (reconciler, circuit_resume, _queue_next_step)
    or knows the intended fire time (create_enrollment passes
    ``enrollment.created_at``). Defaulting to ``datetime.utcnow()`` would
    re-introduce the bare-step-id trap under a different name: two legit
    re-enqueues in the same second would silently collapse.

    Args:
        enrollment_step_id: The enrollment step to process
        tenant_id: Tenant ID for the operation
        scheduled_at: The intended fire time for this step. Used as part of
            the dedup key so two enqueues for the same fire time collapse
            while a genuine re-schedule (different fire time) gets a new job.
        delay_seconds: Optional delay before processing

    Returns:
        The job id string on a successful enqueue, or ``None`` when arq
        deduped the enqueue (a job with this id was already queued). The
        debug log carries the deterministic id either way.
    """
    pool = await get_redis_pool()

    defer_by = timedelta(seconds=delay_seconds) if delay_seconds else None
    job_id = f"step:{enrollment_step_id}:{int(scheduled_at.timestamp())}"

    job = await pool.enqueue_job(
        'process_sequence_step',
        enrollment_step_id,
        tenant_id,
        _job_id=job_id,
        _defer_by=defer_by,
    )

    if job is None:
        logger.debug(
            "enqueue deduped — job with this id already queued",
            enrollment_step_id=enrollment_step_id,
            job_id=job_id,
        )
        return None
    return job.job_id


async def queue_signal_detection(
    mailbox_id: str,
    tenant_id: str,
) -> str:
    """Queue signal detection for a mailbox."""
    pool = await get_redis_pool()
    
    job = await pool.enqueue_job(
        'detect_signals',
        mailbox_id,
        tenant_id,
    )
    
    return job.job_id


async def queue_webhook_delivery(
    webhook_delivery_id: str,
    tenant_id: str,
) -> str:
    """Queue a webhook delivery."""
    pool = await get_redis_pool()
    
    job = await pool.enqueue_job(
        'deliver_webhook',
        webhook_delivery_id,
        tenant_id,
    )
    
    return job.job_id
