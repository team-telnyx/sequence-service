"""Webhook configuration endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel, HttpUrl
from typing import Optional
import json
import uuid

from src.models.base import get_db
from src.models.models import WebhookConfig

router = APIRouter()


class WebhookCreate(BaseModel):
    url: HttpUrl
    secret: str
    events: list[str]


class WebhookUpdate(BaseModel):
    url: Optional[HttpUrl] = None
    secret: Optional[str] = None
    events: Optional[list[str]] = None
    enabled: Optional[bool] = None


class WebhookResponse(BaseModel):
    id: str
    url: str
    events: list[str]
    enabled: bool
    
    class Config:
        from_attributes = True
    
    @classmethod
    def from_model(cls, webhook: WebhookConfig) -> "WebhookResponse":
        return cls(
            id=webhook.id,
            url=webhook.url,
            events=json.loads(webhook.events),
            enabled=webhook.enabled,
        )


@router.get("/")
async def list_webhooks(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """List all webhook configurations for the tenant."""
    tenant_id = request.state.tenant_id
    
    result = await db.execute(
        select(WebhookConfig).where(WebhookConfig.tenant_id == tenant_id)
    )
    webhooks = result.scalars().all()
    
    return {"data": [WebhookResponse.from_model(w) for w in webhooks]}


@router.get("/{webhook_id}")
async def get_webhook(
    webhook_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Get a specific webhook configuration."""
    tenant_id = request.state.tenant_id
    
    result = await db.execute(
        select(WebhookConfig)
        .where(WebhookConfig.id == webhook_id, WebhookConfig.tenant_id == tenant_id)
    )
    webhook = result.scalar_one_or_none()
    
    if not webhook:
        raise HTTPException(status_code=404, detail="Webhook not found")
    
    return {"data": WebhookResponse.from_model(webhook)}


@router.post("/", status_code=201)
async def create_webhook(
    data: WebhookCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Create a new webhook configuration."""
    tenant_id = request.state.tenant_id
    
    webhook = WebhookConfig(
        id=str(uuid.uuid4()),
        tenant_id=tenant_id,
        url=str(data.url),
        secret=data.secret,
        events=json.dumps(data.events),
    )
    
    db.add(webhook)
    await db.commit()
    await db.refresh(webhook)
    
    return {"data": WebhookResponse.from_model(webhook)}


@router.put("/{webhook_id}")
async def update_webhook(
    webhook_id: str,
    data: WebhookUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Update a webhook configuration."""
    tenant_id = request.state.tenant_id
    
    result = await db.execute(
        select(WebhookConfig)
        .where(WebhookConfig.id == webhook_id, WebhookConfig.tenant_id == tenant_id)
    )
    webhook = result.scalar_one_or_none()
    
    if not webhook:
        raise HTTPException(status_code=404, detail="Webhook not found")
    
    if data.url is not None:
        webhook.url = str(data.url)
    if data.secret is not None:
        webhook.secret = data.secret
    if data.events is not None:
        webhook.events = json.dumps(data.events)
    if data.enabled is not None:
        webhook.enabled = data.enabled
    
    await db.commit()
    await db.refresh(webhook)
    
    return {"data": WebhookResponse.from_model(webhook)}


@router.delete("/{webhook_id}", status_code=204)
async def delete_webhook(
    webhook_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Delete a webhook configuration."""
    tenant_id = request.state.tenant_id
    
    result = await db.execute(
        select(WebhookConfig)
        .where(WebhookConfig.id == webhook_id, WebhookConfig.tenant_id == tenant_id)
    )
    webhook = result.scalar_one_or_none()
    
    if not webhook:
        raise HTTPException(status_code=404, detail="Webhook not found")
    
    await db.delete(webhook)
    await db.commit()
