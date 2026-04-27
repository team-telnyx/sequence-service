"""Queue management for ARQ jobs."""

from arq import create_pool
from arq.connections import RedisSettings
from datetime import timedelta
from typing import Optional

from src.config import get_settings

settings = get_settings()

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
    delay_seconds: Optional[int] = None,
) -> str:
    """
    Queue a sequence step for processing.
    
    Args:
        enrollment_step_id: The enrollment step to process
        tenant_id: Tenant ID for the operation
        delay_seconds: Optional delay before processing
        
    Returns:
        Job ID
    """
    pool = await get_redis_pool()
    
    defer_by = timedelta(seconds=delay_seconds) if delay_seconds else None
    
    job = await pool.enqueue_job(
        'process_sequence_step',
        enrollment_step_id,
        tenant_id,
        _defer_by=defer_by,
    )
    
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
