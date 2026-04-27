"""Webhook service - creates and queues webhook deliveries."""

import json
import uuid
from datetime import datetime
from typing import Optional

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.models import (
    WebhookConfig,
    WebhookDelivery,
    Signal,
    SentEmail,
    SequenceEnrollment,
    SequenceEnrollmentStep,
)
from src.services.queue import queue_webhook_delivery

logger = structlog.get_logger()


async def create_signal_webhook(
    db: AsyncSession,
    signal: Signal,
    sent_email: SentEmail,
    enrollment: SequenceEnrollment,
) -> Optional[str]:
    """
    Create and queue a webhook delivery for a signal event.
    
    Returns the delivery ID if created, None if no webhook configured.
    """
    # Get tenant from enrollment's sequence
    from src.models.models import Sequence
    
    result = await db.execute(
        select(Sequence).where(Sequence.id == enrollment.sequence_id)
    )
    sequence = result.scalar_one_or_none()
    
    if not sequence:
        return None
    
    tenant_id = sequence.tenant_id
    
    # Find active webhook config for this tenant that handles this event
    result = await db.execute(
        select(WebhookConfig)
        .where(
            WebhookConfig.tenant_id == tenant_id,
            WebhookConfig.enabled == True,
        )
    )
    configs = result.scalars().all()
    
    delivery_ids = []
    
    for config in configs:
        # Check if config handles this event type
        events = json.loads(config.events) if config.events else []
        event_type = f"signal.{signal.type.value.lower()}"
        
        if events and event_type not in events and "signal.*" not in events:
            continue
        
        # Build payload
        payload = {
            "event": event_type,
            "timestamp": datetime.utcnow().isoformat(),
            "data": {
                "signal_id": signal.id,
                "signal_type": signal.type.value,
                "detected_at": signal.detected_at.isoformat(),
                "enrollment": {
                    "id": enrollment.id,
                    "contact_email": enrollment.contact_email,
                    "contact_name": enrollment.contact_name,
                    "status": enrollment.status.value,
                },
                "sent_email": {
                    "id": sent_email.id,
                    "message_id": sent_email.message_id,
                    "subject": sent_email.subject,
                    "to_email": sent_email.to_email,
                    "from_email": sent_email.from_email,
                    "sent_at": sent_email.sent_at.isoformat(),
                },
            },
        }
        
        if signal.raw_data:
            try:
                payload["data"]["raw"] = json.loads(signal.raw_data)
            except json.JSONDecodeError:
                payload["data"]["raw"] = signal.raw_data
        
        # Create delivery record
        delivery = WebhookDelivery(
            id=str(uuid.uuid4()),
            config_id=config.id,
            event_type=event_type,
            payload=json.dumps(payload),
            status="PENDING",
            attempts=0,
        )
        db.add(delivery)
        await db.flush()
        
        logger.info(
            "Created webhook delivery",
            delivery_id=delivery.id,
            event_type=event_type,
            config_url=config.url,
        )
        
        # Queue for delivery
        try:
            await queue_webhook_delivery(delivery.id, tenant_id)
            delivery_ids.append(delivery.id)
        except Exception as e:
            logger.error("Failed to queue webhook", error=str(e))
    
    await db.commit()
    
    return delivery_ids[0] if delivery_ids else None


async def create_enrollment_webhook(
    db: AsyncSession,
    enrollment: SequenceEnrollment,
    event: str,  # "created", "completed", "bounced", "paused"
) -> Optional[str]:
    """Create webhook for enrollment lifecycle events."""
    from src.models.models import Sequence
    
    result = await db.execute(
        select(Sequence).where(Sequence.id == enrollment.sequence_id)
    )
    sequence = result.scalar_one_or_none()
    
    if not sequence:
        return None
    
    tenant_id = sequence.tenant_id
    
    result = await db.execute(
        select(WebhookConfig)
        .where(
            WebhookConfig.tenant_id == tenant_id,
            WebhookConfig.enabled == True,
        )
    )
    configs = result.scalars().all()
    
    for config in configs:
        events = json.loads(config.events) if config.events else []
        event_type = f"enrollment.{event}"
        
        if events and event_type not in events and "enrollment.*" not in events:
            continue
        
        payload = {
            "event": event_type,
            "timestamp": datetime.utcnow().isoformat(),
            "data": {
                "enrollment": {
                    "id": enrollment.id,
                    "sequence_id": enrollment.sequence_id,
                    "contact_email": enrollment.contact_email,
                    "contact_name": enrollment.contact_name,
                    "status": enrollment.status.value,
                    "current_step": enrollment.current_step,
                },
                "sequence": {
                    "id": sequence.id,
                    "name": sequence.name,
                },
            },
        }
        
        delivery = WebhookDelivery(
            id=str(uuid.uuid4()),
            config_id=config.id,
            event_type=event_type,
            payload=json.dumps(payload),
            status="PENDING",
            attempts=0,
        )
        db.add(delivery)
        await db.flush()
        
        try:
            await queue_webhook_delivery(delivery.id, tenant_id)
        except Exception as e:
            logger.error("Failed to queue webhook", error=str(e))
    
    await db.commit()
    return None
