"""Sequence step processing worker."""

import random
import structlog
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from src.config import get_settings
from src.models.base import async_session
from src.models.models import (
    SequenceEnrollmentStep,
    SequenceEnrollment,
    SentEmail,
    EnrollmentStepStatus,
    EnrollmentStatus,
)
from src.services.email_builder import build_tracked_email
from src.api.tracking import generate_unsubscribe_url
from src.services.gmail import GmailService, GmailError
from src.services.mailbox_rotation import select_mailbox, reserve_send
from src.services.queue import queue_sequence_step
from src.services.template import render_email
from src.services.circuit_breaker import check_circuit_breaker
from src.services.send_window import check_send_window
from src.services.suppression import check_suppressed

settings = get_settings()
logger = structlog.get_logger()


async def process_sequence_step(
    ctx: dict,
    enrollment_step_id: str,
    tenant_id: str,
) -> dict:
    """
    Process a single sequence step.
    
    1. Load enrollment step with all related data
    2. Select or use assigned mailbox
    3. Render email content
    4. Send email (or stub)
    5. Update status
    """
    logger.info("Processing sequence step", enrollment_step_id=enrollment_step_id)
    
    async with async_session() as db:
        # Load enrollment step
        result = await db.execute(
            select(SequenceEnrollmentStep)
            .where(SequenceEnrollmentStep.id == enrollment_step_id)
            .options(
                selectinload(SequenceEnrollmentStep.enrollment)
                .selectinload(SequenceEnrollment.sequence),
                selectinload(SequenceEnrollmentStep.step),
                selectinload(SequenceEnrollmentStep.mailbox),
            )
        )
        enrollment_step = result.scalar_one_or_none()
        
        if not enrollment_step:
            logger.error("Enrollment step not found", enrollment_step_id=enrollment_step_id)
            raise ValueError(f"Enrollment step not found: {enrollment_step_id}")
        
        enrollment = enrollment_step.enrollment
        sequence = enrollment.sequence
        step = enrollment_step.step
        
        # Check enrollment is still active
        if enrollment.status != EnrollmentStatus.ACTIVE:
            logger.info(
                "Skipping - enrollment not active",
                enrollment_id=enrollment.id,
                status=enrollment.status,
            )
            return {"skipped": True, "reason": "enrollment_not_active"}
        
        # Suppression check — never send to suppressed contacts
        is_suppressed = await check_suppressed(db, enrollment.contact_email, tenant_id)
        if is_suppressed:
            logger.info(
                "Skipping - contact is suppressed",
                enrollment_id=enrollment.id,
                contact_email=enrollment.contact_email,
            )
            enrollment.status = EnrollmentStatus.UNSUBSCRIBED
            enrollment_step.status = EnrollmentStepStatus.SKIPPED
            await db.commit()
            return {"skipped": True, "reason": "suppressed"}
        
        # Circuit breaker check — skip if mailbox bounce rate is too high
        tripped = await check_circuit_breaker(db, enrollment.mailbox_id, tenant_id)
        if tripped:
            logger.warning(
                "Circuit breaker tripped — skipping send",
                enrollment_id=enrollment.id,
                mailbox_id=enrollment.mailbox_id,
            )
            return {"skipped": True, "reason": "circuit_breaker"}
        
        # Send window check — re-queue if outside recipient's business hours
        window_delay = check_send_window(enrollment.timezone)
        if window_delay is not None:
            logger.info(
                "Outside send window — re-queuing",
                enrollment_step_id=enrollment_step_id,
                delay_seconds=window_delay,
            )
            await queue_sequence_step(
                enrollment_step_id=enrollment_step_id,
                tenant_id=tenant_id,
                delay_seconds=window_delay,
            )
            return {"skipped": True, "reason": "outside_send_window", "requeued_delay": window_delay}
        
        # Check step is ready to process (PENDING or SCHEDULED)
        if enrollment_step.status not in (EnrollmentStepStatus.PENDING, EnrollmentStepStatus.SCHEDULED):
            logger.info(
                "Skipping - step not ready",
                enrollment_step_id=enrollment_step_id,
                status=enrollment_step.status,
            )
            return {"skipped": True, "reason": "step_not_ready"}
        
        # Use enrollment's sticky mailbox (assigned at enrollment time)
        from src.models.models import Mailbox
        from src.config import validate_mailbox_for_tenant
        result = await db.execute(
            select(Mailbox).where(Mailbox.id == enrollment.mailbox_id)
        )
        mailbox = result.scalar_one_or_none()
        
        if not mailbox:
            logger.error("Enrollment mailbox not found", mailbox_id=enrollment.mailbox_id)
            raise RuntimeError(f"Enrollment mailbox not found: {enrollment.mailbox_id}")
        
        # HARDCODED ENFORCEMENT: Verify mailbox is allowed for this tenant
        try:
            validate_mailbox_for_tenant(tenant_id, mailbox.email)
        except ValueError as e:
            logger.error("Mailbox not allowed for tenant", error=str(e))
            raise RuntimeError(str(e))
        
        # Reserve send slot
        reserved = await reserve_send(db, mailbox.id)
        if not reserved:
            logger.warning("Failed to reserve send slot", mailbox_id=mailbox.id)
            raise RuntimeError("Failed to reserve send slot - mailbox at capacity")
        
        # Use Scout-composed content if available, otherwise render step template
        if enrollment_step.custom_subject and enrollment_step.custom_body:
            # Scout composed this email - use it directly
            subject = enrollment_step.custom_subject
            body = enrollment_step.custom_body
            logger.info("Using Scout-composed content", enrollment_step_id=enrollment_step_id)
        else:
            # Fall back to step template
            subject, body = render_email(
                step.subject,
                step.body,
                contact_name=enrollment.contact_name,
                contact_email=enrollment.contact_email,
            )
            logger.info("Using step template", enrollment_step_id=enrollment_step_id)
        
        import uuid
        from datetime import datetime
        
        # Create sent email record first (need ID for tracking)
        sent_email_id = str(uuid.uuid4())
        sent_email = SentEmail(
            id=sent_email_id,
            message_id=f"pending-{sent_email_id}",  # Placeholder until sent
            thread_id=None,
            mailbox_id=mailbox.id,
            enrollment_step_id=enrollment_step.id,
            subject=subject,
            body=body,
            to_email=enrollment.contact_email,
            to_name=enrollment.contact_name,
            from_email=mailbox.email,
            from_name=mailbox.display_name,
            sent_at=datetime.utcnow(),
        )
        db.add(sent_email)
        await db.flush()  # Get the ID assigned
        
        # Build tracked HTML email (with unsubscribe link + CAN-SPAM footer)
        # Note: step.body contains HTML content (with <p>, <br>, etc.)
        html_body, plain_body = build_tracked_email(
            body=body,
            sent_email_id=sent_email_id,
            is_html=True,  # step.body is HTML
            enrollment_id=enrollment.id,
        )
        
        # Build RFC 8058 List-Unsubscribe header
        unsub_url = generate_unsubscribe_url(settings.tracking_base_url, enrollment.id)
        list_unsubscribe = f"<{unsub_url}>, <mailto:unsubscribe@telnyx.com?subject=unsubscribe>"

        # Send email via Gmail API
        if settings.gmail_enabled:
            try:
                gmail = GmailService.get_inbox(mailbox.email)
                result = gmail.send_html_email(
                    to=enrollment.contact_email,
                    subject=subject,
                    html_body=html_body,
                    plain_text_fallback=plain_body,
                    sender_name=mailbox.display_name,
                    list_unsubscribe=list_unsubscribe,
                )
                gmail_message_id = result['message_id']
                gmail_thread_id = result['thread_id']
                
                # Update sent email with actual IDs
                sent_email.message_id = gmail_message_id
                sent_email.thread_id = gmail_thread_id
                
                logger.info(
                    "Email sent via Gmail (HTML with tracking)",
                    from_email=mailbox.email,
                    to_email=enrollment.contact_email,
                    message_id=gmail_message_id,
                )
            except GmailError as e:
                logger.error("Gmail send failed", error=str(e))
                raise RuntimeError(f"Gmail send failed: {e}")
        else:
            # Stub mode - generate fake message ID
            sent_email.message_id = f"stub-{uuid.uuid4()}"
            logger.info(
                "[STUB] Gmail disabled - skipping actual send",
                from_email=mailbox.email,
                to_email=enrollment.contact_email,
                subject=subject,
            )
        
        # Update step status
        enrollment_step.status = EnrollmentStepStatus.SENT
        enrollment_step.sent_at = datetime.utcnow()
        
        # Update enrollment current_step
        enrollment.current_step = step.step_number
        
        await db.commit()
        
        logger.info(
            "Sequence step processed successfully",
            enrollment_step_id=enrollment_step_id,
            message_id=sent_email.message_id,
        )
        
        # Queue next step if exists
        next_step_info = await _queue_next_step(
            db=db,
            enrollment=enrollment,
            current_step_number=step.step_number,
            tenant_id=tenant_id,
        )
        
        return {
            "success": True,
            "message_id": sent_email.message_id,
            "to_email": enrollment.contact_email,
            "next_step_queued": next_step_info,
        }


async def _queue_next_step(
    db,
    enrollment: SequenceEnrollment,
    current_step_number: int,
    tenant_id: str,
) -> dict | None:
    """
    Find and queue the next step in the sequence.
    
    Returns info about queued step, or None if no next step.
    """
    from src.models.models import SequenceStep
    
    # Find the next step in sequence
    result = await db.execute(
        select(SequenceStep)
        .where(
            SequenceStep.sequence_id == enrollment.sequence_id,
            SequenceStep.step_number > current_step_number,
        )
        .order_by(SequenceStep.step_number)
        .limit(1)
    )
    next_step = result.scalar_one_or_none()
    
    if not next_step:
        logger.info(
            "No more steps in sequence",
            enrollment_id=enrollment.id,
            current_step=current_step_number,
        )
        # Mark enrollment as completed
        enrollment.status = EnrollmentStatus.COMPLETED
        await db.commit()
        return None
    
    # Find the enrollment step for the next sequence step
    result = await db.execute(
        select(SequenceEnrollmentStep)
        .where(
            SequenceEnrollmentStep.enrollment_id == enrollment.id,
            SequenceEnrollmentStep.step_id == next_step.id,
        )
    )
    next_enrollment_step = result.scalar_one_or_none()
    
    if not next_enrollment_step:
        logger.error(
            "Enrollment step not found for next sequence step",
            enrollment_id=enrollment.id,
            step_id=next_step.id,
        )
        return None
    
    # Calculate delay in seconds (with optional jitter)
    delay_seconds = (next_step.delay_days * 24 * 3600) + (next_step.delay_hours * 3600)
    
    if settings.send_jitter_enabled and settings.send_jitter_minutes > 0:
        jitter = random.randint(
            -settings.send_jitter_minutes * 60,
            settings.send_jitter_minutes * 60,
        )
        delay_seconds = max(0, delay_seconds + jitter)
        logger.info("Applied send jitter", jitter_seconds=jitter, total_delay=delay_seconds)
    
    # Mark as scheduled
    next_enrollment_step.status = EnrollmentStepStatus.SCHEDULED
    await db.commit()
    
    # Queue the next step
    try:
        job_id = await queue_sequence_step(
            enrollment_step_id=next_enrollment_step.id,
            tenant_id=tenant_id,
            delay_seconds=delay_seconds if delay_seconds > 0 else None,
        )
        
        logger.info(
            "Queued next sequence step",
            enrollment_id=enrollment.id,
            enrollment_step_id=next_enrollment_step.id,
            step_number=next_step.step_number,
            delay_seconds=delay_seconds,
            job_id=job_id,
        )
        
        return {
            "enrollment_step_id": next_enrollment_step.id,
            "step_number": next_step.step_number,
            "delay_seconds": delay_seconds,
            "job_id": job_id,
        }
    except Exception as e:
        logger.error("Failed to queue next step", error=str(e))
        return None
