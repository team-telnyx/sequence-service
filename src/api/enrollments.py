"""Enrollment management endpoints."""

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from pydantic import BaseModel, EmailStr, Field, model_validator
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

# How long (seconds) a 429 at-capacity caller should wait before retrying. The
# daily mailbox cap resets at 00:05 UTC; an hour is a safe, conservative backoff
# that never bypasses the cap and avoids hammering the API mid-day.
CAPACITY_RETRY_AFTER_SECONDS = 3600

# pause_reasons that must NOT be auto/manually resumed without an explicit force:
# these mean the recipient took a terminal action (replied / bounced / opted out),
# so re-activating would re-email someone we must not contact (REVOPS-972 B1).
_PROTECTED_PAUSE_REASONS = frozenset({"reply", "bounce", "unsubscribe"})


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
    # External identity from the producer (Scout prospect id). Persisted on the
    # enrollment and echoed back so reply/bounce signals join to the originating
    # prospect deterministically (REVOPS-972 identity). `prospect_id` is accepted
    # as an alias for the same value.
    external_ref: Optional[str] = None
    prospect_id: Optional[str] = Field(default=None, exclude=True)

    @model_validator(mode="after")
    def _coalesce_external_ref(self):
        # prospect_id is a caller-friendly alias; external_ref wins if both given.
        if self.external_ref is None and self.prospect_id is not None:
            self.external_ref = self.prospect_id
        return self


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
    external_ref: Optional[str] = None
    
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
        # Distinguish a TRANSIENT at-capacity condition (every ACTIVE mailbox has
        # already hit its daily cap — resolves at the next daily reset) from a true
        # zero-active-mailbox INFRA failure (no ACTIVE mailbox row exists at all).
        # The contract Scout relies on: 429 = retry later (do NOT burn an enroll
        # attempt / OUT the account), 503 = infra (REVOPS-972 B2). The 75/day cap
        # is never bypassed — we only change the status code we report.
        active_count = (await db.execute(
            select(func.count())
            .select_from(Mailbox)
            .where(
                Mailbox.tenant_id == tenant_id,
                Mailbox.status == MailboxStatus.ACTIVE,
            )
        )).scalar_one()
        if active_count > 0:
            logger.warning(
                "All active mailboxes at capacity — returning 429",
                tenant_id=tenant_id, active_mailboxes=active_count,
            )
            raise HTTPException(
                status_code=429,
                detail="All mailboxes are at their daily send capacity; retry later",
                headers={"Retry-After": str(CAPACITY_RETRY_AFTER_SECONDS)},
            )
        logger.error(
            "No ACTIVE mailboxes for tenant — returning 503",
            tenant_id=tenant_id,
        )
        raise HTTPException(status_code=503, detail="No available mailboxes for sending")
    
    # Create enrollment with assigned mailbox
    enrollment = SequenceEnrollment(
        id=str(uuid.uuid4()),
        sequence_id=data.sequence_id,
        mailbox_id=mailbox.id,  # Sticky sender
        contact_email=data.contact_email,
        contact_name=data.contact_name,
        timezone=data.timezone or 'America/New_York',
        external_ref=data.external_ref,
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
            # First step is queued for immediate processing; record scheduled_at
            # so the reconciler can recover it if its arq job is lost (audit M4).
            scheduled_at=datetime.utcnow() if i == 0 else None,
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
    enrollment.pause_reason = "manual"
    await db.commit()
    
    return {"success": True}


@router.post("/{enrollment_id}/resume")
async def resume_enrollment(
    enrollment_id: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
    force: bool = Query(
        False,
        description=(
            "Resume even when the enrollment was paused for a TERMINAL recipient "
            "action (reply/bounce/unsubscribe). Without this, such a resume is "
            "refused to avoid re-emailing someone we must not contact."
        ),
    ),
):
    """Resume a paused enrollment.

    Mirrors circuit_resume._resume_mailbox: a valid resume CLEARS pause_reason,
    flips status to ACTIVE, and re-queues the next PENDING/SCHEDULED step so the
    sequence actually progresses (the old TODO left it stalled). REFUSES (409) to
    resume a reply/bounce/unsubscribe-paused enrollment unless `force=true`, so a
    manual resume can't reopen the B1 re-email hazard outside the cron's guard
    (REVOPS-972 B1).
    """
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

    if enrollment.pause_reason in _PROTECTED_PAUSE_REASONS and not force:
        logger.warning(
            "Refusing to resume enrollment paused for a terminal action",
            enrollment_id=enrollment_id, tenant_id=tenant_id,
            pause_reason=enrollment.pause_reason,
        )
        raise HTTPException(
            status_code=409,
            detail=(
                f"Enrollment paused for '{enrollment.pause_reason}'; resuming would "
                f"re-contact a recipient who took a terminal action. Pass force=true "
                f"to override."
            ),
        )

    enrollment.status = EnrollmentStatus.ACTIVE
    enrollment.pause_reason = None

    # Re-queue the next PENDING/SCHEDULED step (lowest step_number) so the sequence
    # actually advances after resume — mirroring circuit_resume._resume_mailbox.
    nxt = (await db.execute(
        select(SequenceEnrollmentStep)
        .join(SequenceStep, SequenceStep.id == SequenceEnrollmentStep.step_id)
        .where(
            SequenceEnrollmentStep.enrollment_id == enrollment.id,
            SequenceEnrollmentStep.status.in_(
                [EnrollmentStepStatus.PENDING, EnrollmentStepStatus.SCHEDULED]),
        )
        .order_by(SequenceStep.step_number)
        .limit(1)
    )).scalar_one_or_none()
    requeue_step_id = None
    if nxt is not None:
        nxt.status = EnrollmentStepStatus.SCHEDULED
        nxt.scheduled_at = datetime.utcnow()
        requeue_step_id = nxt.id

    await db.commit()

    # Enqueue AFTER commit so the worker sees the ACTIVE/SCHEDULED rows. A queue
    # failure must not undo the resume — the scheduled_at reconciler recovers it.
    if requeue_step_id is not None:
        try:
            await queue_sequence_step(
                enrollment_step_id=requeue_step_id, tenant_id=tenant_id, delay_seconds=None,
            )
        except Exception as exc:
            logger.error(
                "resume_enrollment: re-enqueue failed",
                enrollment_step_id=requeue_step_id, error=str(exc),
            )

    return {"success": True}
