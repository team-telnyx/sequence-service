"""Enrollment management endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from pydantic import BaseModel, EmailStr
from typing import Optional
from datetime import datetime
import uuid
import structlog

from src.models.base import get_db
from src.models.models import (
    SequenceEnrollment, 
    SequenceEnrollmentStep,
    Sequence,
    SequenceStep,
    EnrollmentStatus,
    EnrollmentStepStatus,
)
from src.services.queue import queue_sequence_step
from src.services.mailbox_rotation import select_mailbox
from src.models.models import Mailbox, MailboxStatus
from src.config import validate_mailbox_for_tenant

logger = structlog.get_logger()

router = APIRouter()


class EnrollmentStepContent(BaseModel):
    step_number: int
    subject: str
    body: str


class EnrollmentCreate(BaseModel):
    sequence_id: str
    contact_email: EmailStr
    contact_name: Optional[str] = None
    timezone: Optional[str] = None  # IANA timezone, e.g. "Australia/Sydney"
    # Optional: region-routed sticky sender chosen by Scout's MailboxRouter.
    # When set + allowed + ACTIVE + has capacity, the enrollment sticks to THIS
    # mailbox instead of weighted-random rotation. Invalid/at-capacity/not-active
    # falls back to rotation (logged) — never a hard failure, so sends never stop.
    sender_email: Optional[EmailStr] = None
    # Optional: Scout-composed email content (overrides step template)
    email_subject: Optional[str] = None
    email_body: Optional[str] = None  # HTML content from Scout composition (legacy T1 only)
    composed_steps: Optional[list[EnrollmentStepContent]] = None  # All touches pre-composed


class EnrollmentUpdate(BaseModel):
    status: Optional[EnrollmentStatus] = None


class EnrollmentResponse(BaseModel):
    id: str
    sequence_id: str
    mailbox_id: str  # Sticky sender
    contact_email: str
    contact_name: Optional[str]
    status: EnrollmentStatus
    current_step: int
    
    class Config:
        from_attributes = True


@router.get("/")
async def list_enrollments(
    request: Request,
    db: AsyncSession = Depends(get_db),
    sequence_id: Optional[str] = None,
    status: Optional[EnrollmentStatus] = None,
    limit: int = 50,
    offset: int = 0,
):
    """List enrollments."""
    tenant_id = request.state.tenant_id
    
    query = (
        select(SequenceEnrollment)
        .join(Sequence)
        .where(Sequence.tenant_id == tenant_id)
    )
    
    if sequence_id:
        query = query.where(SequenceEnrollment.sequence_id == sequence_id)
    if status:
        query = query.where(SequenceEnrollment.status == status)
    
    query = query.limit(limit).offset(offset)
    
    result = await db.execute(query)
    enrollments = result.scalars().all()
    
    return {"data": [EnrollmentResponse.model_validate(e) for e in enrollments]}


@router.get("/{enrollment_id}")
async def get_enrollment(
    enrollment_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Get a specific enrollment."""
    tenant_id = request.state.tenant_id
    
    result = await db.execute(
        select(SequenceEnrollment)
        .join(Sequence)
        .where(
            SequenceEnrollment.id == enrollment_id,
            Sequence.tenant_id == tenant_id,
        )
        .options(selectinload(SequenceEnrollment.steps))
    )
    enrollment = result.scalar_one_or_none()
    
    if not enrollment:
        raise HTTPException(status_code=404, detail="Enrollment not found")
    
    return {"data": EnrollmentResponse.model_validate(enrollment)}


@router.post("/", status_code=201)
async def create_enrollment(
    data: EnrollmentCreate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Create a new enrollment."""
    tenant_id = request.state.tenant_id
    
    # Verify sequence exists and belongs to tenant
    result = await db.execute(
        select(Sequence)
        .where(Sequence.id == data.sequence_id, Sequence.tenant_id == tenant_id)
        .options(selectinload(Sequence.steps))
    )
    sequence = result.scalar_one_or_none()
    
    if not sequence:
        raise HTTPException(status_code=404, detail="Sequence not found")
    
    # Check for existing enrollment
    existing = await db.execute(
        select(SequenceEnrollment)
        .where(
            SequenceEnrollment.sequence_id == data.sequence_id,
            SequenceEnrollment.contact_email == data.contact_email,
        )
    )
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="Contact already enrolled in this sequence")
    
    # Select mailbox for sticky sender (assigned once, used for all steps).
    # If Scout passed a region-routed sender_email, honor it as the sticky
    # sender when it is allowlisted for the tenant, ACTIVE, and has capacity;
    # otherwise fall back to weighted-random rotation (never hard-fail, so a
    # stale/at-capacity hint can't stop sending).
    mailbox = None
    if data.sender_email:
        requested = str(data.sender_email).lower()
        allowed = False
        try:
            allowed = validate_mailbox_for_tenant(tenant_id, requested)
        except ValueError as exc:
            logger.warning(
                "sender_email not allowed for tenant — falling back to rotation",
                tenant_id=tenant_id, sender_email=requested, error=str(exc),
            )
        if allowed:
            result = await db.execute(
                select(Mailbox).where(
                    Mailbox.tenant_id == tenant_id,
                    Mailbox.email == requested,
                    Mailbox.status == MailboxStatus.ACTIVE,
                )
            )
            candidate = result.scalar_one_or_none()
            if candidate is None:
                logger.warning(
                    "sender_email has no ACTIVE mailbox row — falling back to rotation",
                    tenant_id=tenant_id, sender_email=requested,
                )
            elif (candidate.daily_send_limit - candidate.sent_today) < 1:
                logger.warning(
                    "sender_email mailbox at capacity — falling back to rotation",
                    tenant_id=tenant_id, sender_email=requested,
                    sent_today=candidate.sent_today, daily_send_limit=candidate.daily_send_limit,
                )
            else:
                mailbox = candidate
                logger.info(
                    "Honoring region-routed sender_email as sticky sender",
                    tenant_id=tenant_id, sender_email=requested, mailbox_id=mailbox.id,
                )

    if mailbox is None:
        mailbox = await select_mailbox(db, tenant_id)
    if not mailbox:
        raise HTTPException(status_code=503, detail="No available mailboxes for sending")
    
    # Create enrollment with assigned mailbox
    enrollment = SequenceEnrollment(
        id=str(uuid.uuid4()),
        sequence_id=data.sequence_id,
        mailbox_id=mailbox.id,  # Sticky sender
        contact_email=data.contact_email,
        contact_name=data.contact_name,
        timezone=data.timezone or 'America/New_York',
    )
    db.add(enrollment)
    
    # Build step content map from composed_steps or legacy single-step fields
    step_content_map = {}
    if data.composed_steps:
        step_content_map = {s.step_number: s for s in data.composed_steps}
    elif data.email_subject and data.email_body:
        step_content_map = {1: EnrollmentStepContent(step_number=1, subject=data.email_subject, body=data.email_body)}

    # Create enrollment steps for each sequence step (sorted by step_number)
    sorted_steps = sorted(sequence.steps, key=lambda s: s.step_number)
    first_enrollment_step_id = None

    for i, step in enumerate(sorted_steps):
        composed = step_content_map.get(step.step_number)
        enrollment_step = SequenceEnrollmentStep(
            id=str(uuid.uuid4()),
            enrollment_id=enrollment.id,
            step_id=step.id,
            status=EnrollmentStepStatus.SCHEDULED if i == 0 else EnrollmentStepStatus.PENDING,
            custom_subject=composed.subject if composed else None,
            custom_body=composed.body if composed else None,
        )
        db.add(enrollment_step)

        if i == 0:
            first_enrollment_step_id = enrollment_step.id
    
    await db.commit()
    await db.refresh(enrollment)
    
    # Queue first step for immediate processing
    if first_enrollment_step_id:
        try:
            job_id = await queue_sequence_step(
                enrollment_step_id=first_enrollment_step_id,
                tenant_id=tenant_id,
                delay_seconds=0,
            )
            logger.info(
                "Queued first sequence step",
                enrollment_id=enrollment.id,
                enrollment_step_id=first_enrollment_step_id,
                job_id=job_id,
            )
        except Exception as e:
            logger.error("Failed to queue first step", error=str(e))
            # Don't fail the enrollment - step can be retried
    
    return {"data": EnrollmentResponse.model_validate(enrollment)}


@router.put("/{enrollment_id}")
async def update_enrollment(
    enrollment_id: str,
    data: EnrollmentUpdate,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Update an enrollment."""
    tenant_id = request.state.tenant_id
    
    result = await db.execute(
        select(SequenceEnrollment)
        .join(Sequence)
        .where(
            SequenceEnrollment.id == enrollment_id,
            Sequence.tenant_id == tenant_id,
        )
    )
    enrollment = result.scalar_one_or_none()
    
    if not enrollment:
        raise HTTPException(status_code=404, detail="Enrollment not found")
    
    if data.status is not None:
        enrollment.status = data.status
    
    await db.commit()
    await db.refresh(enrollment)
    
    return {"data": EnrollmentResponse.model_validate(enrollment)}


@router.post("/{enrollment_id}/pause")
async def pause_enrollment(
    enrollment_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Pause an enrollment."""
    tenant_id = request.state.tenant_id
    
    result = await db.execute(
        select(SequenceEnrollment)
        .join(Sequence)
        .where(
            SequenceEnrollment.id == enrollment_id,
            Sequence.tenant_id == tenant_id,
        )
    )
    enrollment = result.scalar_one_or_none()
    
    if not enrollment:
        raise HTTPException(status_code=404, detail="Enrollment not found")
    
    enrollment.status = EnrollmentStatus.PAUSED
    await db.commit()
    
    return {"success": True}


@router.post("/{enrollment_id}/resume")
async def resume_enrollment(
    enrollment_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Resume a paused enrollment."""
    tenant_id = request.state.tenant_id
    
    result = await db.execute(
        select(SequenceEnrollment)
        .join(Sequence)
        .where(
            SequenceEnrollment.id == enrollment_id,
            Sequence.tenant_id == tenant_id,
        )
    )
    enrollment = result.scalar_one_or_none()
    
    if not enrollment:
        raise HTTPException(status_code=404, detail="Enrollment not found")
    
    enrollment.status = EnrollmentStatus.ACTIVE
    await db.commit()
    
    # TODO: Re-queue pending steps via ARQ
    
    return {"success": True}
