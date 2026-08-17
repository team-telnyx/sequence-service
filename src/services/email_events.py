"""Email API webhook event processing (REVOPS-1552).

Idempotent handling of Telnyx Email API delivery events: delivered, bounce
(hard), complaint, one-click unsubscribe. Events are matched to sent emails
via the Telnyx message UUID stored on ``SentEmail.message_id`` at send time
by the Email API transport adapter (``sequence_step.py`` writes the Telnyx
UUID on 202 success). Bounce/complaint/unsubscribe write origin-split
suppression rows (``API_BOUNCE``/``API_COMPLAINT``/``API_UNSUBSCRIBE``);
manual/SFDC suppression rows are never modified — one-way sync IN from API
events only, never write back out.

r3 idempotency design (PostgreSQL-native):

  Deduplication on Telnyx event id (Telnyx redelivers on non-2xx) uses
  ``INSERT ... ON CONFLICT DO NOTHING`` for BOTH the dedupe marker
  (``ProcessedEmailEvent``, PK on ``id``) AND the suppression insert
  (unique on ``(tenant_id, email)``). The marker is inserted FIRST in the
  transaction — rowcount=1 means this request is the winner and proceeds
  with processing; rowcount=0 means a concurrent request already
  committed the marker and this request returns 200 duplicate-ok
  WITHOUT reprocessing. The suppression insert is also idempotent
  (rowcount=0 → loser, no duplicate row, one-way sync preserved).

  This replaces the r2 broad ``except IntegrityError`` (which mislabeled
  EVERY integrity failure as a duplicate race — the reviewer's probe
  swallowed an FK violation: ``propagated=False, rows=0``). With ON
  CONFLICT DO NOTHING the duplicate-race case is handled structurally
  by the database, so there is no broad except to mislabel non-duplicate
  errors. Any ``IntegrityError`` that is NOT the expected duplicate
  constraint (e.g. FK, NOT NULL) propagates loudly to the caller — no
  200, no marker committed (the transaction rolls back).

  Dialect note: production runs on PostgreSQL; the test harness uses
  SQLite (in-memory, aiosqlite). Both support ``ON CONFLICT (cols) DO
  NOTHING`` (SQLite ≥3.24; Python 3.12 ships SQLite 3.4x+). The
  dialect-specific ``insert()`` is selected at execute time from the
  session's bind — ``sqlalchemy.dialects.postgresql.insert`` in
  production, ``sqlalchemy.dialects.sqlite.insert`` in tests. No
  exception-based fallback is needed; the only dialects supported are
  postgresql (production) and sqlite (tests).

  Atomicity: the marker is inserted FIRST in the same transaction as
  the side effects (suppression, SentEmail.delivered_at, enrollment
  status). A partial failure (exception) rolls back the marker AND the
  side effects, so the redelivery retries cleanly — the same atomicity
  guarantee as the r1/r2 "marker last" design, but with race-resilient
  winner/loser arbitration via the idempotent insert.

The service operates on a normalized ``EmailEvent``, not raw Telnyx JSON—
the API layer (``src/api/email_events.py``) parses the raw payload and
constructs the ``EmailEvent``. This keeps the business logic independent
of the exact Telnyx webhook envelope structure.
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

# Full Telnyx Email API delivery event set (SV2-044).
EVENT_SENT = "sent"
EVENT_DELIVERED = "delivered"
EVENT_BOUNCE = "bounce"
EVENT_OPENED = "opened"
EVENT_CLICKED = "clicked"
EVENT_COMPLAINT = "complaint"
EVENT_SUPPRESSED = "suppressed"
EVENT_UNSUBSCRIBE = "unsubscribe"

_REASON_MAP: dict[str, SuppressionReason] = {
    EVENT_BOUNCE: SuppressionReason.API_BOUNCE,
    EVENT_COMPLAINT: SuppressionReason.API_COMPLAINT,
    EVENT_UNSUBSCRIBE: SuppressionReason.API_UNSUBSCRIBE,
}


@dataclass(frozen=True)
class EmailEvent:
    """Normalized Telnyx Email API webhook event (transport-agnostic).

    contract_version: the delivery-event contract version (SV2-044).
    """

    event_id: str
    event_type: str
    message_id: str
    to_email: str
    occurred_at: Optional[str] = None
    contract_version: int = 1


async def process_email_event(db: AsyncSession, event: EmailEvent) -> dict:
    """Process a single Telnyx Email API webhook event, idempotently.

    Returns a dict describing the outcome:
      - ``{"already_processed": True}`` — redelivery of a previously
        processed event (idempotent no-op), OR this request lost the
        marker race to a concurrent request (``duplicate_race: True``).
      - ``{"unmatched": True, ...}`` — no ``SentEmail`` row matches the
        event's ``message_id`` (or the enrollment chain is incomplete).
        The marker is still written to prevent reprocessing on redelivery.
      - ``{"processed": True, ...}`` — the event was processed (suppression
        written and/or step/enrollment outcome updated).

    r3 race handling: the marker is inserted FIRST via
    ``INSERT ... ON CONFLICT (id) DO NOTHING``. If a concurrent request
    already committed the same marker, this request's insert returns
    rowcount=0 and this request returns ``already_processed`` WITHOUT
    reprocessing (no side effects, no duplicate suppression). This is
    the winner/loser arbitration point — the database is the source of
    truth, not a try/except.
    """
    # Fast path: marker already exists from a previously committed event.
    # This SELECT is an optimization to avoid the INSERT round-trip in the
    # common case of a redelivery arriving after the marker is committed.
    existing = await db.execute(
        select(ProcessedEmailEvent.id).where(ProcessedEmailEvent.id == event.event_id)
    )
    if existing.scalar_one_or_none() is not None:
        logger.debug(
            "Email event already processed — redelivery no-op",
            event_id=event.event_id,
        )
        return {"already_processed": True, "event_id": event.event_id}

    # r3: race-aware marker insert — atomic winner/loser arbitration.
    # INSERT ... ON CONFLICT (id) DO NOTHING returns rowcount=0 if a
    # concurrent request just committed the same marker; we lose and
    # return 200 duplicate-ok without reprocessing. This is the structural
    # fix for the r2 finding where the loser caught the suppression
    # unique-violation but then hit an UNCAUGHT duplicate-key on the marker
    # ORM autoflush at commit time.
    marker_inserted = await _idempotent_insert_marker(db, event)
    if marker_inserted == 0:
        logger.info(
            "Email event marker lost the race — concurrent request already processed",
            event_id=event.event_id,
        )
        return {
            "already_processed": True,
            "event_id": event.event_id,
            "duplicate_race": True,
        }

    # We won the marker race — proceed with processing. The marker is
    # in the current transaction; a partial failure (exception) rolls
    # back the marker AND any side effects, so the redelivery retries
    # cleanly (same atomicity guarantee as r1/r2 marker-last).
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
        await _prune_dedupe(db)
        await db.commit()
        return {
            "unmatched": True,
            "event_id": event.event_id,
            "reason": "incomplete_chain",
        }

    contact_email = (enrollment.contact_email or event.to_email).lower()

    if event.event_type == EVENT_DELIVERED:
        sent_email.delivered_at = datetime.utcnow()
        logger.info(
            "Email delivered (Telnyx Email API)",
            event_id=event.event_id,
            message_id=event.message_id,
            to_email=contact_email,
        )
    elif event.event_type == EVENT_SENT:
        logger.info(
            "Email sent confirmed (Telnyx Email API)",
            event_id=event.event_id,
            message_id=event.message_id,
            to_email=contact_email,
        )
    elif event.event_type in (EVENT_OPENED, EVENT_CLICKED):
        logger.info(
            "Email engagement event (Telnyx Email API)",
            event_id=event.event_id,
            event_type=event.event_type,
            message_id=event.message_id,
            to_email=contact_email,
        )
    elif event.event_type == EVENT_SUPPRESSED:
        await _write_suppression(
            db,
            tenant_id=tenant_id,
            email=contact_email,
            domain=None,
            reason=SuppressionReason.API_BOUNCE,
            enrollment_id=enrollment.id,
            event=event,
        )
        enrollment.status = EnrollmentStatus.BOUNCED
        logger.info(
            "Email suppressed by Telnyx (Email API)",
            event_id=event.event_id,
            message_id=event.message_id,
            to_email=contact_email,
        )
    elif event.event_type in _REASON_MAP:
        reason = _REASON_MAP[event.event_type]
        # API-event suppressions are EMAIL-scoped ONLY — domain=NULL prevents a
        # single bounce at a@x.com from suppressing every contact at x.com (F1).
        # The guard's domain semantics are NOT changed: manual domain rows
        # (with domain set) still block domain-wide via check_suppressed.
        await _write_suppression(
            db,
            tenant_id=tenant_id,
            email=contact_email,
            domain=None,
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

    await _prune_dedupe(db)
    await db.commit()

    return {
        "processed": True,
        "event_id": event.event_id,
        "event_type": event.event_type,
        "message_id": event.message_id,
    }


async def _suppression_exists(db: AsyncSession, tenant_id: str, email: str) -> bool:
    """Check if a suppression row already exists for (tenant_id, email)."""
    result = await db.execute(
        select(Suppression.id).where(
            Suppression.tenant_id == tenant_id,
            Suppression.email == email,
        )
    )
    return result.scalar_one_or_none() is not None


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
    """Insert an origin-split suppression row idempotently (r3).

    Uses ``INSERT ... ON CONFLICT (tenant_id, email) DO NOTHING``. The
    first suppression stands and is never modified (one-way sync). A
    concurrent insert that loses the race returns rowcount=0 and is a
    no-op — both concurrent requests return 200, exactly one row
    survives (the unique constraint is the durable guard).

    r3 change: the r2 broad ``except IntegrityError`` is REMOVED. The
    duplicate-race case is handled structurally by ON CONFLICT DO NOTHING
    (rowcount=0), so there is no exception path to mislabel. Any
    ``IntegrityError`` that is NOT the expected duplicate constraint
    (e.g. FK violation on ``tenant_id`` or ``source_enrollment_id``,
    NOT NULL violation) propagates loudly to the caller — no 200, no
    marker committed. This fixes the r2 Finding 2 (P1) where the broad
    except swallowed an FK violation (reviewer probe:
    ``propagated=False, rows=0``).
    """
    # Fast path: suppression already exists (committed by any prior
    # request or operator/SFDC). Skip the INSERT round-trip. The existing
    # row is never modified — one-way sync.
    if await _suppression_exists(db, tenant_id, email):
        logger.info(
            "Suppression already exists — one-way sync, not modified",
            event_id=event.event_id,
            email=email,
        )
        return

    # Idempotent INSERT — race-aware. Loser of a concurrent insert gets
    # rowcount=0; the existing row (from any origin) is never modified.
    # No try/except: a non-duplicate IntegrityError (FK, NOT NULL)
    # propagates loudly.
    rowcount = await _idempotent_insert_suppression(
        db,
        tenant_id=tenant_id,
        email=email,
        domain=domain,
        reason=reason,
        enrollment_id=enrollment_id,
        event=event,
    )
    if rowcount == 0:
        logger.info(
            "Suppression insert lost the race — already suppressed by a concurrent event",
            event_id=event.event_id,
            email=email,
        )
        return
    logger.info(
        "Suppression written (Email API origin)",
        event_id=event.event_id,
        event_type=event.event_type,
        email=email,
        reason=reason.value,
    )


async def _idempotent_insert_suppression(
    db: AsyncSession,
    *,
    tenant_id: str,
    email: str,
    domain: Optional[str],
    reason: SuppressionReason,
    enrollment_id: str,
    event: EmailEvent,
) -> int:
    """INSERT ... ON CONFLICT (tenant_id, email) DO NOTHING for Suppression.

    Returns 1 if inserted (this request is the suppression origin), 0 if
    a concurrent request already inserted (or a manual/SFDC row exists)
    — the existing row is never modified.

    Production: PostgreSQL ``sqlalchemy.dialects.postgresql.insert``.
    Test infra: SQLite ``sqlalchemy.dialects.sqlite.insert`` (≥3.24).
    The dialect is detected from the session's bind at execute time.
    """
    values = {
        "id": str(uuid.uuid4()),
        "tenant_id": tenant_id,
        "email": email,
        "domain": domain,
        "reason": reason,
        "source_enrollment_id": enrollment_id,
        "notes": f"email_api_{event.event_type} event_id={event.event_id}",
    }
    stmt = _build_idempotent_insert(
        db,
        Suppression,
        values,
        conflict_index_elements=["tenant_id", "email"],
    )
    result = await db.execute(stmt)
    return result.rowcount


async def _idempotent_insert_marker(db: AsyncSession, event: EmailEvent) -> int:
    """INSERT ... ON CONFLICT (id) DO NOTHING for ProcessedEmailEvent marker.

    Returns 1 if inserted (this request is the winner and proceeds with
    processing), 0 if a concurrent request already committed the same
    event_id marker (this request is the loser and returns 200
    already-processed without reprocessing).

    Production: PostgreSQL. Test infra: SQLite. Both support
    ON CONFLICT (id) DO NOTHING.
    """
    values = {
        "id": event.event_id,
        "event_type": event.event_type,
    }
    stmt = _build_idempotent_insert(
        db,
        ProcessedEmailEvent,
        values,
        conflict_index_elements=["id"],
    )
    result = await db.execute(stmt)
    return result.rowcount


def _build_idempotent_insert(
    db: AsyncSession,
    model,
    values: dict,
    *,
    conflict_index_elements: list[str],
):
    """Build a dialect-aware INSERT ... ON CONFLICT DO NOTHING statement.

    Production runs on PostgreSQL; the test harness uses SQLite. Both
    support ``ON CONFLICT (cols) DO NOTHING`` (SQLite ≥3.24; Python 3.12
    bundles SQLite 3.4x+). The dialect-specific ``insert()`` is selected
    at execute time from the session's bind:

      - ``postgresql`` → ``sqlalchemy.dialects.postgresql.insert`` (prod)
      - ``sqlite``     → ``sqlalchemy.dialects.sqlite.insert``     (tests)

    No exception-based fallback: the only supported dialects are
    postgresql (production) and sqlite (tests). Any other dialect raises
    ``RuntimeError`` — fail loud rather than silently degrading.
    """
    dialect_name = db.bind.dialect.name
    if dialect_name == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        return (
            pg_insert(model)
            .values(**values)
            .on_conflict_do_nothing(index_elements=conflict_index_elements)
        )
    if dialect_name == "sqlite":
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        return (
            sqlite_insert(model)
            .values(**values)
            .on_conflict_do_nothing(index_elements=conflict_index_elements)
        )
    raise RuntimeError(
        f"Unsupported dialect for idempotent insert: {dialect_name} "
        "(production expects postgresql, test infra expects sqlite)"
    )


async def _prune_dedupe(db: AsyncSession) -> None:
    """Delete dedupe markers older than DEDUPE_RETENTION_DAYS."""
    cutoff = datetime.utcnow() - timedelta(days=DEDUPE_RETENTION_DAYS)
    await db.execute(delete(ProcessedEmailEvent).where(ProcessedEmailEvent.processed_at < cutoff))
