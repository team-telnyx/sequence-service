"""Signal detection worker - polls inboxes for replies, bounces, etc."""

import json
from datetime import datetime, timedelta
from typing import Optional
import structlog
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from src.config import get_settings
from src.models.base import async_session
from src.models.models import (
    Mailbox,
    SentEmail,
    Signal,
    SignalType,
    SequenceEnrollment,
    SequenceEnrollmentStep,
    EnrollmentStatus,
    EnrollmentStepStatus,
)
from src.services.gmail import GmailService, GmailError
from src.services.webhooks import create_signal_webhook

settings = get_settings()
logger = structlog.get_logger()


async def detect_signals(ctx: dict, mailbox_id: str, tenant_id: str) -> dict:
    """
    Poll a mailbox for engagement signals.
    
    Detects: replies, bounces, out-of-office, unsubscribes.
    
    1. Get recent inbox messages
    2. Match against our sent emails by thread_id
    3. Classify signal type
    4. Create Signal records
    5. Update enrollment status as needed
    """
    logger.info("Detecting signals", mailbox_id=mailbox_id)
    
    if not settings.gmail_enabled:
        logger.info("[STUB] Gmail disabled - skipping signal detection")
        return {"signals_detected": 0, "stub_mode": True}
    
    async with async_session() as db:
        # Get mailbox
        result = await db.execute(
            select(Mailbox).where(Mailbox.id == mailbox_id)
        )
        mailbox = result.scalar_one_or_none()
        
        if not mailbox:
            logger.error("Mailbox not found", mailbox_id=mailbox_id)
            return {"error": "mailbox_not_found"}
        
        # Get recent sent emails from this mailbox (last 7 days)
        cutoff = datetime.utcnow() - timedelta(days=7)
        result = await db.execute(
            select(SentEmail)
            .where(
                SentEmail.mailbox_id == mailbox_id,
                SentEmail.sent_at >= cutoff,
            )
            .options(
                selectinload(SentEmail.enrollment_step)
                .selectinload(SequenceEnrollmentStep.enrollment)
            )
        )
        sent_emails = result.scalars().all()
        
        if not sent_emails:
            logger.info("No recent sent emails to check", mailbox_id=mailbox_id)
            return {"signals_detected": 0, "no_sent_emails": True}
        
        # Build lookup by thread_id
        thread_to_sent = {se.thread_id: se for se in sent_emails if se.thread_id}
        thread_ids = list(thread_to_sent.keys())
        
        logger.info(
            "Checking threads for replies",
            mailbox=mailbox.email,
            thread_count=len(thread_ids),
        )
        
        # Poll Gmail for replies
        try:
            gmail = GmailService.get_inbox(mailbox.email)
            replies = gmail.get_replies_to_threads(thread_ids)
        except GmailError as e:
            logger.error("Gmail API error", error=str(e))
            return {"error": str(e)}
        
        signals_created = 0
        
        for reply in replies:
            thread_id = reply['thread_id']
            sent_email = thread_to_sent.get(thread_id)
            
            if not sent_email:
                continue
            
            # Check if we already recorded this signal
            existing = await db.execute(
                select(Signal)
                .where(
                    Signal.sent_email_id == sent_email.id,
                    Signal.raw_data.contains(reply['message_id']),
                )
            )
            if existing.scalar_one_or_none():
                logger.debug("Signal already recorded", message_id=reply['message_id'])
                continue
            
            # Classify signal type
            if reply.get('is_bounce'):
                signal_type = SignalType.BOUNCE
            elif reply.get('is_ooo'):
                signal_type = SignalType.OUT_OF_OFFICE
            else:
                signal_type = SignalType.REPLY
            
            # Create signal record
            signal = Signal(
                id=str(__import__('uuid').uuid4()),
                sent_email_id=sent_email.id,
                type=signal_type,
                detected_at=datetime.utcnow(),
                raw_data=json.dumps({
                    'gmail_message_id': reply['message_id'],
                    'from': reply['from'],
                    'subject': reply['subject'],
                    'snippet': reply['snippet'],
                    'date': reply['date'],
                }),
            )
            db.add(signal)
            signals_created += 1
            
            logger.info(
                "Signal detected",
                type=signal_type.value,
                from_addr=reply['from'],
                sent_email_id=sent_email.id,
            )
            
            # Update enrollment based on signal type
            enrollment = sent_email.enrollment_step.enrollment
            
            if signal_type == SignalType.REPLY:
                # Pause enrollment - they replied!
                if enrollment.status == EnrollmentStatus.ACTIVE:
                    enrollment.status = EnrollmentStatus.PAUSED
                    logger.info(
                        "Paused enrollment due to reply",
                        enrollment_id=enrollment.id,
                    )
            
            elif signal_type == SignalType.BOUNCE:
                # Mark as bounced - stop sending
                enrollment.status = EnrollmentStatus.BOUNCED
                
                # Cancel pending steps
                for step in await _get_pending_steps(db, enrollment.id):
                    step.status = EnrollmentStepStatus.SKIPPED
                
                logger.info(
                    "Marked enrollment as bounced",
                    enrollment_id=enrollment.id,
                )
            
            elif signal_type == SignalType.OUT_OF_OFFICE:
                # Just record it - don't change enrollment status
                logger.info(
                    "Out-of-office detected (no action)",
                    enrollment_id=enrollment.id,
                )
            
            # Trigger webhook for all signal types
            try:
                await create_signal_webhook(
                    db=db,
                    signal=signal,
                    sent_email=sent_email,
                    enrollment=enrollment,
                )
            except Exception as e:
                logger.error("Failed to create signal webhook", error=str(e))
        
        await db.commit()
        
        logger.info(
            "Signal detection complete",
            mailbox=mailbox.email,
            signals_created=signals_created,
        )
        
        return {
            "signals_detected": signals_created,
            "threads_checked": len(thread_ids),
            "replies_found": len(replies),
        }


async def _get_pending_steps(db, enrollment_id: str) -> list[SequenceEnrollmentStep]:
    """Get all pending/scheduled steps for an enrollment."""
    result = await db.execute(
        select(SequenceEnrollmentStep)
        .where(
            SequenceEnrollmentStep.enrollment_id == enrollment_id,
            SequenceEnrollmentStep.status.in_([
                EnrollmentStepStatus.PENDING,
                EnrollmentStepStatus.SCHEDULED,
            ]),
        )
    )
    return result.scalars().all()


async def detect_signals_all_mailboxes(ctx: dict, tenant_id: str) -> dict:
    """
    Run signal detection for all active mailboxes in a tenant.
    
    Convenience function for scheduled polling.
    """
    logger.info("Running signal detection for all mailboxes", tenant_id=tenant_id)
    
    async with async_session() as db:
        result = await db.execute(
            select(Mailbox)
            .where(Mailbox.tenant_id == tenant_id)
        )
        mailboxes = result.scalars().all()
    
    total_signals = 0
    results = []
    
    for mailbox in mailboxes:
        result = await detect_signals(ctx, mailbox.id, tenant_id)
        results.append({
            "mailbox_id": mailbox.id,
            "mailbox_email": mailbox.email,
            **result,
        })
        total_signals += result.get("signals_detected", 0)
    
    return {
        "total_signals": total_signals,
        "mailboxes_checked": len(mailboxes),
        "results": results,
    }
