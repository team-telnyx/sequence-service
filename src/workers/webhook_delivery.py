"""Webhook delivery worker."""

import json
import hashlib
import hmac
from datetime import datetime

import httpx
import structlog
from sqlalchemy import select

from src.models.base import async_session
from src.models.models import WebhookDelivery, WebhookConfig

logger = structlog.get_logger()

MAX_RETRIES = 5


async def deliver_webhook(ctx: dict, delivery_id: str) -> dict:
    """
    Deliver a webhook to the configured endpoint.
    
    Retries with exponential backoff on failure.
    """
    logger.info("Delivering webhook", delivery_id=delivery_id)
    
    async with async_session() as db:
        # Load delivery with config
        result = await db.execute(
            select(WebhookDelivery)
            .where(WebhookDelivery.id == delivery_id)
        )
        delivery = result.scalar_one_or_none()
        
        if not delivery:
            logger.error("Webhook delivery not found", delivery_id=delivery_id)
            raise ValueError(f"Webhook delivery not found: {delivery_id}")
        
        # Load config
        config_result = await db.execute(
            select(WebhookConfig).where(WebhookConfig.id == delivery.config_id)
        )
        config = config_result.scalar_one_or_none()
        
        if not config or not config.enabled:
            logger.info("Webhook config disabled or not found", config_id=delivery.config_id)
            delivery.status = "SKIPPED"
            await db.commit()
            return {"skipped": True}
        
        # Prepare request
        payload = delivery.payload
        signature = hmac.new(
            config.secret.encode(),
            payload.encode(),
            hashlib.sha256,
        ).hexdigest()
        
        headers = {
            "Content-Type": "application/json",
            "X-Webhook-Signature": signature,
            "X-Webhook-Event": delivery.event_type,
        }
        
        # Attempt delivery
        delivery.attempts += 1
        delivery.last_attempt_at = datetime.utcnow()
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    config.url,
                    content=payload,
                    headers=headers,
                )
            
            delivery.response_status = response.status_code
            delivery.response_body = response.text[:1000]  # Truncate
            
            if response.is_success:
                delivery.status = "DELIVERED"
                logger.info(
                    "Webhook delivered successfully",
                    delivery_id=delivery_id,
                    status_code=response.status_code,
                )
            else:
                raise httpx.HTTPStatusError(
                    f"HTTP {response.status_code}",
                    request=response.request,
                    response=response,
                )
        
        except Exception as e:
            error_msg = str(e)
            logger.warning(
                "Webhook delivery failed",
                delivery_id=delivery_id,
                attempt=delivery.attempts,
                error=error_msg,
            )
            
            if delivery.attempts >= MAX_RETRIES:
                delivery.status = "FAILED"
                logger.error(
                    "Webhook delivery permanently failed",
                    delivery_id=delivery_id,
                    attempts=delivery.attempts,
                )
            else:
                delivery.status = "PENDING"
                # Re-raise to trigger ARQ retry
                await db.commit()
                raise
        
        await db.commit()
        
        return {
            "status": delivery.status,
            "attempts": delivery.attempts,
            "response_status": delivery.response_status,
        }
