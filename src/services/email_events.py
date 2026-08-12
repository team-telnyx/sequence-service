"""Email API webhook event processing (REVOPS-1552).

Idempotent handling of Telnyx Email API delivery events: delivered, bounce
(hard), complaint, one-click unsubscribe. Events are matched to sent emails
via the Telnyx message UUID stored on ``SentEmail.message_id`` at send time
by the Email API transport adapter (``sequence_step.py`` writes the Telnyx
UUID on 202 success). Bounce/complaint/unsubscribe write origin-split
suppression rows (``API_BOUNCE``/``API_COMPLAINT``/``API_UNSUBSCRIBE``);
manual/SFDC suppression rows are never modified — one-way sync IN from API
events only, never write back out.

Deduplication on Telnyx event id (Telnyx redelivers on non-2xx): a
``ProcessedEmailEvent`` marker is written ONLY after all processing succeeds
(same transaction), so a partial failure leaves no marker and the redelivery
retries cleanly. A redelivery hitting an existing marker is a no-op. The
marker table is opportunistically pruned (rows older than
``DEDUPE_RETENTION_DAYS`` deleted on each event) so growth is bounded
without a cron dependency.

The service operates on a normalized ``EmailEvent``, not raw Telnyx JSON —
the API layer (``src/api/email_events.py``) parses the raw payload and
constructs the ``EmailEvent``. This keeps the business logic independent of
the exact Telnyx webhook envelope structure.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

import structlog
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from src.models.models import (
    EnrollmentStatus,
    EnrollmentStepStatus,
    ProcessedEmailEvent,
    SequenceEnrollment,
    SequenceEnrollmentStep,
    SentEmail,
    Suppression,
    SuppressionReason,
)

logger = structlog.get_logger()

DEDUPE_RETENTION_DAYS = 30

EVENT_DELIVERED = "delivered"
EVENT_BOUNCE = "bounce"
EVENT_COMPLAINT = "complaint"
EVENT_UNSUBSCRIBE = "unsubscribe"

_REASON_MAP: dict[str, SuppressionReason] = {
    EVENT_BOUNCE: SuppressionReason.API_BOUNCE,
    EVENT_COMPLAINT: SuppressionReason.API_COMPLAINT,
    EVENT_UNSUBSCRIBE: SuppressionReason.API_UNSUBSCRIBE,
}


@dataclass(frozen=True)
class EmailEvent:
    """Normalized Telnyx Email API webhook event (transport-agnostic)."""

    event_id: str
    event_type: str
    message_id: str
    to_email: str
    occurred_at: Optional[str] = None


async def process_email_event(db: AsyncSession, event: EmailEvent) -> dict:
    """Process a single Telnyx Email API webhook event, idempotently.

    Returns a dict describing the outcome:
      - ``{"already_processed": True}`` — redelivery of a previously
        processed event (idempotent no-op).
      - ``{"unmatched": True, ...}`` — no ``SentEmail`` row matches the
        event's ``message_id`` (or the enrollment chain is incomplete).
        The marker is still written to prevent reprocessing on redelivery.
      - ``{"processed": True, ...}`` — the event was processed (suppression
        written and/or step/enrollment outcome updated).
    """
    existing = await db.execute(
        select(ProcessedEmailEvent.id).where(ProcessedEmailEvent.id == event.event_id)
    )
    if existing.scalar_one_or_none() is not None:
        logger.debug(
            "Email event already processed — redelivery no-op",
            event_id=event.event_id,
        )
        return {"already_processed": True, "event_id": event.event_id}

    result = await db.execute(
        select(SentEmail)
        .where(SentEmail.message_id == event.message_id)
        .options(
            selectinload(SentEmail.enrollment_step)
            .selectinload(SequenceEnrollmentStep.enrollment)
            .selectinload(SequenceEnrollment.sequence),
        )
    )
    sent_email = result.scalar_one_or_none()

    if sent_email is None:
        logger.warning(
            "Email event unmatched — no SentEmail for message_id",
            event_id=event.event_id,
            event_type=event.event_type,
            message_id=event.message_id,
        )
        _write_marker(db, event)
        await _prune_dedupe(db)
        await db.commit()
        return {"unmatched": True, "event_id": event.event_id}

    enrollment_step = sent_email.enrollment_step
    enrollment = enrollment_step.enrollment if enrollment_step else None
    sequence = enrollment.sequence if enrollment else None
    tenant_id = sequence.tenant_id if sequence else None

    if tenant_id is None or enrollment is None:
        logger.error(
            "Email event matched but enrollment chain incomplete",
            event_id=event.event_id,
            message_id=event.message_id,
            enrollment_step_id=enrollment_step.id if enrollment_step else None,
        )
        _write_marker(db, event)
        await _prune_dedupe(db)
        await db.commit()
        return {
            "unmatched": True,
            "event_id": event.event_id,
            "reason": "incomplete_chain",
        }

    contact_email = (enrollment.contact_email or event.to_email).lower()
    domain = contact_email.split("@")[1] if "@" in contact_email else None

    if event.event_type == EVENT_DELIVERED:
        logger.info(
            "Email delivered (Telnyx Email API)",
            event_id=event.event_id,
            message_id=event.message_id,
            to_email=contact_email,
        )
    elif event.event_type in _REASON_MAP:
        reason = _REASON_MAP[event.event_type]
        await _write_suppression(
            db,
            tenant_id=tenant_id,
            email=contact_email,
            domain=domain,
            reason=reason,
            enrollment_id=enrollment.id,
            event=event,
        )
        if event.event_type == EVENT_BOUNCE:
            enrollment_step.status = EnrollmentStepStatus.BOUNCED
            enrollment.status = EnrollmentStatus.BOUNCED
            logger.info(
                "Enrollment step marked BOUNCED (Email API bounce)",
                event_id=event.event_id,
                enrollment_step_id=enrollment_step.id,
                enrollment_id=enrollment.id,
            )
        else:
            enrollment.status = EnrollmentStatus.UNSUBSCRIBED
            logger.info(
                "Enrollment marked UNSUBSCRIBED (Email API event)",
                event_id=event.event_id,
                event_type=event.event_type,
                enrollment_id=enrollment.id,
            )
    else:
        logger.warning(
            "Unknown Email API event type — recording marker only",
            event_id=event.event_id,
            event_type=event.event_type,
        )

    _write_marker(db, event)
    await _prune_dedupe(db)
    await db.commit()

    return {
        "processed": True,
        "event_id": event.event_id,
        "event_type": event.event_type,
        "message_id": event.message_id,
    }


async def _write_suppression(
    db: AsyncSession,
    *,
    tenant_id: str,
    email: str,
    domain: Optional[str],
    reason: SuppressionReason,
    enrollment_id: str,
    event: EmailEvent,
) -> None:
    """Insert an origin-split suppression row (one-way IN, never modify).

    If the email is already suppressed (any reason — manual, reply, or a
    prior API event), skip the insert: the first suppression stands and is
    never modified. A race-induced unique violation is caught and treated
    as "already suppressed" (the unique constraint on (tenant_id, email)
    is the durable guard).
    """
    existing = await db.execute(
        select(Suppression.id).where(
            Suppression.tenant_id == tenant_id,
            Suppression.email == email,
        )
    )
    if existing.scalar_one_or_none() is not None:
        logger.info(
            "Suppression already exists — one-way sync, not modified",
            event_id=event.event_id,
            email=email,
        )
        return

    suppression = Suppression(
        id=str(uuid.uuid4()),
        tenant_id=tenant_id,
        email=email,
        domain=domain,
        reason=reason,
        source_enrollment_id=enrollment_id,
        notes=f"email_api_{event.event_type} event_id={event.event_id}",
    )
    db.add(suppression)
    await db.flush()
    logger.info(
        "Suppression written (Email API origin)",
        event_id=event.event_id,
        event_type=event.event_type,
        email=email,
        reason=reason.value,
    )


def _write_marker(db: AsyncSession, event: EmailEvent) -> None:
    """Record the dedupe marker for this event id."""
    db.add(
        ProcessedEmailEvent(
            id=event.event_id,
            event_type=event.event_type,
            processed_at=datetime.utcnow(),
        )
    )


async def _prune_dedupe(db: AsyncSession) -> None:
    """Delete dedupe markers older than DEDUPE_RETENTION_DAYS."""
    cutoff = datetime.utcnow() - timedelta(days=DEDUPE_RETENTION_DAYS)
    await db.execute(
        delete(ProcessedEmailEvent).where(ProcessedEmailEvent.processed_at < cutoff)
    )
