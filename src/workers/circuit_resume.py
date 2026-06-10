"""Auto-resume enrollments paused by the circuit breaker once the mailbox recovers.

check_circuit_breaker pauses ALL of a mailbox's active enrollments when its bounce
rate crosses the threshold (default 10%/24h) — but nothing ever un-pauses them, so
they sit PAUSED forever (586 stranded 2026-06-05..09). This cron resumes
circuit_breaker-paused enrollments once the mailbox's bounce rate has cooled
comfortably below a resume threshold (hysteresis — so a still-elevated mailbox
doesn't resume and immediately re-trip), and re-queues each enrollment's next step.
"""
from datetime import datetime

import structlog
from sqlalchemy import distinct, select

from src.config import get_settings
from src.models.base import async_session
from src.models.models import (
    EnrollmentStatus,
    EnrollmentStepStatus,
    Sequence,
    SequenceEnrollment,
    SequenceEnrollmentStep,
    SequenceStep,
)
from src.services.circuit_breaker import mailbox_bounce_rate
from src.services.queue import queue_sequence_step

settings = get_settings()
logger = structlog.get_logger()


async def _resume_mailbox(db, mailbox_id: str) -> int:
    """Resume a mailbox's circuit_breaker-paused enrollments + re-queue next step."""
    enrollments = (await db.execute(
        select(SequenceEnrollment).where(
            SequenceEnrollment.mailbox_id == mailbox_id,
            SequenceEnrollment.status == EnrollmentStatus.PAUSED,
            SequenceEnrollment.pause_reason == "circuit_breaker",
        )
    )).scalars().all()

    resumed = 0
    requeue = []  # (step_id, tenant_id) — enqueue AFTER commit so the job sees ACTIVE
    for e in enrollments:
        e.status = EnrollmentStatus.ACTIVE
        e.pause_reason = None
        nxt = (await db.execute(
            select(SequenceEnrollmentStep)
            .join(SequenceStep, SequenceStep.id == SequenceEnrollmentStep.step_id)
            .where(
                SequenceEnrollmentStep.enrollment_id == e.id,
                SequenceEnrollmentStep.status.in_(
                    [EnrollmentStepStatus.PENDING, EnrollmentStepStatus.SCHEDULED]),
            )
            .order_by(SequenceStep.step_number)
            .limit(1)
        )).scalar_one_or_none()
        if nxt is not None:
            nxt.status = EnrollmentStepStatus.SCHEDULED
            nxt.scheduled_at = datetime.utcnow()
            tenant_id = (await db.execute(
                select(Sequence.tenant_id).where(Sequence.id == e.sequence_id)
            )).scalar_one_or_none()
            if tenant_id:
                requeue.append((nxt.id, tenant_id))
        resumed += 1

    await db.commit()

    for step_id, tenant_id in requeue:
        try:
            await queue_sequence_step(enrollment_step_id=step_id, tenant_id=tenant_id, delay_seconds=None)
        except Exception as exc:
            logger.error("circuit_resume: re-enqueue failed", enrollment_step_id=step_id, error=str(exc))

    if resumed:
        logger.info("circuit_resume: resumed mailbox", mailbox_id=mailbox_id, resumed=resumed)
    return resumed


async def resume_circuit_breaker_paused(ctx: dict) -> dict:
    """Cron: resume circuit_breaker-paused enrollments on recovered mailboxes."""
    resume_threshold = getattr(settings, "circuit_breaker_resume_threshold", 0.06)
    resumed = checked = skipped_hot = 0

    async with async_session() as db:
        mailboxes = (await db.execute(
            select(distinct(SequenceEnrollment.mailbox_id)).where(
                SequenceEnrollment.status == EnrollmentStatus.PAUSED,
                SequenceEnrollment.pause_reason == "circuit_breaker",
            )
        )).scalars().all()

        for mailbox_id in mailboxes:
            if not mailbox_id:
                continue
            checked += 1
            rate = await mailbox_bounce_rate(db, mailbox_id)
            # rate None = no recent sends (safe to resume); otherwise must be below
            # the resume threshold (hysteresis under the trip line).
            if rate is not None and rate >= resume_threshold:
                skipped_hot += 1
                logger.info("circuit_resume: mailbox still elevated, staying paused",
                            mailbox_id=mailbox_id, bounce_rate=rate, resume_threshold=resume_threshold)
                continue
            resumed += await _resume_mailbox(db, mailbox_id)

    logger.info("resume_circuit_breaker_paused complete",
                resumed=resumed, mailboxes_checked=checked, skipped_hot=skipped_hot)
    return {"resumed": resumed, "mailboxes_checked": checked, "skipped_hot": skipped_hot}
