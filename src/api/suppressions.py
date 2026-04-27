"""Suppression list API — manage do-not-contact emails."""

import uuid
from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, EmailStr
from sqlalchemy import select, or_
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.models import Suppression, SuppressionReason
from src.models.base import get_db

router = APIRouter()


class SuppressionCreate(BaseModel):
    email: str
    reason: str = "UNSUBSCRIBE"  # UNSUBSCRIBE, BOUNCE, COMPLAINT, MANUAL
    source_enrollment_id: Optional[str] = None
    notes: Optional[str] = None


class SuppressionCheck(BaseModel):
    email: str


class SuppressionResponse(BaseModel):
    id: str
    email: str
    domain: Optional[str]
    reason: str
    source_enrollment_id: Optional[str]
    notes: Optional[str]
    created_at: datetime


@router.post("/", status_code=201)
async def add_suppression(
    body: SuppressionCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Add an email to the suppression list."""
    tenant_id: str = request.state.tenant_id
    
    # Extract domain from email
    domain = body.email.split("@")[1] if "@" in body.email else None
    
    # Check if already suppressed
    result = await db.execute(
        select(Suppression).where(
            Suppression.tenant_id == tenant_id,
            Suppression.email == body.email.lower(),
        )
    )
    existing = result.scalar_one_or_none()
    
    if existing:
        return {"data": _to_response(existing), "message": "Already suppressed"}
    
    # Validate reason
    try:
        reason = SuppressionReason(body.reason)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid reason: {body.reason}. Must be one of: {[r.value for r in SuppressionReason]}"
        )
    
    suppression = Suppression(
        id=str(uuid.uuid4()),
        tenant_id=tenant_id,
        email=body.email.lower(),
        domain=domain,
        reason=reason,
        source_enrollment_id=body.source_enrollment_id,
        notes=body.notes,
    )
    db.add(suppression)
    await db.commit()
    await db.refresh(suppression)
    
    return {"data": _to_response(suppression)}


@router.get("/check")
async def check_suppression(
    email: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Check if an email or its domain is suppressed."""
    tenant_id: str = request.state.tenant_id
    
    domain = email.split("@")[1] if "@" in email else None
    
    # Check exact email match OR domain-level suppression
    conditions = [Suppression.email == email.lower()]
    if domain:
        conditions.append(Suppression.domain == domain)
    
    result = await db.execute(
        select(Suppression).where(
            Suppression.tenant_id == tenant_id,
            or_(*conditions),
        )
    )
    matches = result.scalars().all()
    
    return {
        "suppressed": len(matches) > 0,
        "matches": [_to_response(s) for s in matches],
    }


@router.get("/")
async def list_suppressions(
    request: Request,
    db: AsyncSession = Depends(get_db),
    limit: int = 100,
    offset: int = 0,
):
    """List all suppressions for the tenant."""
    tenant_id: str = request.state.tenant_id
    
    result = await db.execute(
        select(Suppression)
        .where(Suppression.tenant_id == tenant_id)
        .order_by(Suppression.created_at.desc())
        .limit(limit)
        .offset(offset)
    )
    suppressions = result.scalars().all()
    
    return {"data": [_to_response(s) for s in suppressions]}


@router.delete("/{suppression_id}")
async def remove_suppression(
    suppression_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Remove an email from the suppression list."""
    tenant_id: str = request.state.tenant_id
    
    result = await db.execute(
        select(Suppression).where(
            Suppression.id == suppression_id,
            Suppression.tenant_id == tenant_id,
        )
    )
    suppression = result.scalar_one_or_none()
    
    if not suppression:
        raise HTTPException(status_code=404, detail="Suppression not found")
    
    await db.delete(suppression)
    await db.commit()
    
    return {"message": "Suppression removed"}


def _to_response(s: Suppression) -> dict:
    return {
        "id": s.id,
        "email": s.email,
        "domain": s.domain,
        "reason": s.reason.value,
        "source_enrollment_id": s.source_enrollment_id,
        "notes": s.notes,
        "created_at": s.created_at.isoformat(),
    }
