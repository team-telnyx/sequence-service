"""Mailbox management endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel, EmailStr
from typing import Optional
import uuid

from src.models.base import get_db
from src.models.models import Mailbox, MailboxStatus

router = APIRouter()


class MailboxCreate(BaseModel):
    email: EmailStr
    display_name: Optional[str] = None
    daily_send_limit: int = 50
    weight: int = 1


class MailboxUpdate(BaseModel):
    display_name: Optional[str] = None
    status: Optional[MailboxStatus] = None
    daily_send_limit: Optional[int] = None
    weight: Optional[int] = None


class MailboxResponse(BaseModel):
    id: str
    email: str
    display_name: Optional[str]
    status: MailboxStatus
    weight: int
    daily_send_limit: int
    sent_today: int
    
    class Config:
        from_attributes = True


@router.get("/")
async def list_mailboxes(
    request: Request,
    db: AsyncSession = Depends(get_db),
    status: Optional[MailboxStatus] = None,
):
    """List all mailboxes for the tenant."""
    tenant_id = request.state.tenant_id
    
    query = select(Mailbox).where(Mailbox.tenant_id == tenant_id)
    if status:
        query = query.where(Mailbox.status == status)
    
    result = await db.execute(query)
    mailboxes = result.scalars().all()
    
    return {"data": [MailboxResponse.model_validate(m) for m in mailboxes]}


@router.get("/{mailbox_id}")
async def get_mailbox(
    mailbox_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Get a specific mailbox."""
    tenant_id = request.state.tenant_id
    
    result = await db.execute(
        select(Mailbox)
        .where(Mailbox.id == mailbox_id, Mailbox.tenant_id == tenant_id)
    )
    mailbox = result.scalar_one_or_none()
    
    if not mailbox:
        raise HTTPException(status_code=404, detail="Mailbox not found")
    
    return {"data": MailboxResponse.model_validate(mailbox)}


@router.post("/", status_code=201)
async def create_mailbox(
    data: MailboxCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Create a new mailbox."""
    tenant_id = request.state.tenant_id
    
    mailbox = Mailbox(
        id=str(uuid.uuid4()),
        tenant_id=tenant_id,
        email=data.email,
        display_name=data.display_name,
        daily_send_limit=data.daily_send_limit,
        weight=data.weight,
    )
    
    db.add(mailbox)
    await db.commit()
    await db.refresh(mailbox)
    
    return {"data": MailboxResponse.model_validate(mailbox)}


@router.put("/{mailbox_id}")
async def update_mailbox(
    mailbox_id: str,
    data: MailboxUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Update a mailbox."""
    tenant_id = request.state.tenant_id
    
    result = await db.execute(
        select(Mailbox)
        .where(Mailbox.id == mailbox_id, Mailbox.tenant_id == tenant_id)
    )
    mailbox = result.scalar_one_or_none()
    
    if not mailbox:
        raise HTTPException(status_code=404, detail="Mailbox not found")
    
    if data.display_name is not None:
        mailbox.display_name = data.display_name
    if data.status is not None:
        mailbox.status = data.status
    if data.daily_send_limit is not None:
        mailbox.daily_send_limit = data.daily_send_limit
    if data.weight is not None:
        mailbox.weight = data.weight
    
    await db.commit()
    await db.refresh(mailbox)
    
    return {"data": MailboxResponse.model_validate(mailbox)}


@router.delete("/{mailbox_id}", status_code=204)
async def delete_mailbox(
    mailbox_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Delete a mailbox."""
    tenant_id = request.state.tenant_id
    
    result = await db.execute(
        select(Mailbox)
        .where(Mailbox.id == mailbox_id, Mailbox.tenant_id == tenant_id)
    )
    mailbox = result.scalar_one_or_none()
    
    if not mailbox:
        raise HTTPException(status_code=404, detail="Mailbox not found")
    
    await db.delete(mailbox)
    await db.commit()


@router.post("/{mailbox_id}/reset-sent-today")
async def reset_sent_today(
    mailbox_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Reset the sent_today counter for a mailbox."""
    tenant_id = request.state.tenant_id
    
    result = await db.execute(
        select(Mailbox)
        .where(Mailbox.id == mailbox_id, Mailbox.tenant_id == tenant_id)
    )
    mailbox = result.scalar_one_or_none()
    
    if not mailbox:
        raise HTTPException(status_code=404, detail="Mailbox not found")
    
    mailbox.sent_today = 0
    await db.commit()
    
    return {"success": True}
