"""Versioned enrollment contract endpoint (SV2-044, docs/10 §Sequence enrollment contract).

POST /v1/enrollments — authenticated (X-API-Key), idempotent enrollment creation.
Implements the docs/10 contract with contract_version=1.

Idempotency semantics:
  same key + same digest → status="existing" + ORIGINAL enrollment (no side effect)
  same key + different digest → 409 conflict (stable reason_code)
  first time → status="reserved", enrollment_id, capacity_date (local mailbox date)

A timeout is reconciled by idempotency-key lookup BEFORE any retry: the caller
re-sends the same key+digest; if a completed record exists, it returns "existing";
if a pending record exists, it returns "existing" with status pending (retry later).
No double enrollment/send is possible under repeated or concurrent requests.
"""

from __future__ import annotations

import hashlib
import uuid
from datetime import datetime

import structlog
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import get_settings
from src.contracts import (
    ENROLLMENT_CONTRACT_VERSION,
    EnrollmentRequest,
    EnrollmentResponse,
    EnrollmentStatusContract,
)
from src.models.base import get_db
from src.models.models import (
    IdempotencyRecord,
    Sequence,
    SequenceEnrollment,
    SequenceEnrollmentStep,
    SequenceStatus,
    EnrollmentStepStatus,
)
from src.services.mailbox_rotation import select_mailbox

logger = structlog.get_logger()
settings = get_settings()
router = APIRouter()

IDEMPOTENCY_SCOPE = "enrollment"
CAPACITY_RETRY_AFTER_SECONDS = 3600


def _canonical_request_sha256(req: EnrollmentRequest) -> str:
    """SHA-256 of the canonical JSON representation of the request."""
    raw = req.model_dump_json().encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _local_mailbox_date() -> str:
    """Local mailbox date in ISO format (YYYY-MM-DD)."""
    return datetime.utcnow().strftime("%Y-%m-%d")


async def _insert_idempotency_record(
    db: AsyncSession,
    key: str,
    request_sha256: str,
) -> int:
    """INSERT ... ON CONFLICT (scope, idempotency_key) DO NOTHING.

    Returns 1 if inserted (we're the winner), 0 if a record already exists.
    """
    dialect_name = db.bind.dialect.name
    if dialect_name == "postgresql":
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        stmt = (
            pg_insert(IdempotencyRecord)
            .values(
                scope=IDEMPOTENCY_SCOPE,
                idempotency_key=key,
                request_sha256=request_sha256,
                status="pending",
            )
            .on_conflict_do_nothing(index_elements=["scope", "idempotency_key"])
        )
    elif dialect_name == "sqlite":
        from sqlalchemy.dialects.sqlite import insert as sqlite_insert

        stmt = (
            sqlite_insert(IdempotencyRecord)
            .values(
                scope=IDEMPOTENCY_SCOPE,
                idempotency_key=key,
                request_sha256=request_sha256,
                status="pending",
            )
            .on_conflict_do_nothing(index_elements=["scope", "idempotency_key"])
        )
    else:
        raise RuntimeError(f"Unsupported dialect: {dialect_name}")

    result = await db.execute(stmt)
    return result.rowcount


@router.post("/enrollments", status_code=201)
async def create_versioned_enrollment(
    req: EnrollmentRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Create a versioned enrollment with idempotency (docs/10 contract)."""
    if req.contract_version != ENROLLMENT_CONTRACT_VERSION:
        return JSONResponse(
            status_code=400,
            content={
                "detail": f"Unsupported contract_version {req.contract_version}; "
                f"expected {ENROLLMENT_CONTRACT_VERSION}"
            },
        )

    request_sha256 = _canonical_request_sha256(req)
    key = req.idempotency_key

    # Idempotency arbitration: INSERT ON CONFLICT DO NOTHING.
    inserted = await _insert_idempotency_record(db, key, request_sha256)

    if inserted == 0:
        # A record exists — read it.
        existing = await db.execute(
            select(IdempotencyRecord).where(
                IdempotencyRecord.scope == IDEMPOTENCY_SCOPE,
                IdempotencyRecord.idempotency_key == key,
            )
        )
        record = existing.scalar_one_or_none()
        if record is None:
            # Race: the record was deleted between insert-conflict and select.
            # Retry the request by failing loudly.
            raise HTTPException(status_code=500, detail="Idempotency record vanished")

        if record.request_sha256 != request_sha256:
            return JSONResponse(
                status_code=409,
                content={
                    "contract_version": ENROLLMENT_CONTRACT_VERSION,
                    "status": "rejected",
                    "idempotency_key": key,
                    "reason_code": "sequence.idempotency_key_conflict",
                },
            )

        # Same key + same digest → return existing.
        result = record.result
        if record.status == "pending" or result is None:
            # Prior attempt still in progress (crashed/timed out) — return
            # existing with no enrollment_id yet; caller retries to get completed.
            return JSONResponse(
                status_code=200,
                content=EnrollmentResponse(
                    contract_version=ENROLLMENT_CONTRACT_VERSION,
                    status=EnrollmentStatusContract.EXISTING,
                    enrollment_id=None,
                    idempotency_key=key,
                    capacity_date=None,
                    reason_code="sequence.idempotency_pending",
                ).model_dump(),
            )

        return JSONResponse(
            status_code=200,
            content=EnrollmentResponse(
                contract_version=ENROLLMENT_CONTRACT_VERSION,
                status=EnrollmentStatusContract.EXISTING,
                enrollment_id=result.get("enrollment_id"),
                idempotency_key=key,
                capacity_date=result.get("capacity_date"),
                reason_code=None,
            ).model_dump(),
        )

    # We won the insert — proceed with enrollment creation.
    tenant_id = "tenant-scout"  # Scout-only deployment

    # Create a synthetic Sequence from the request's steps.
    sequence = Sequence(
        id=str(uuid.uuid4()),
        tenant_id=tenant_id,
        name=f"scout-v2-{req.cohort_id}",
        status=SequenceStatus.ACTIVE,
    )
    db.add(sequence)
    await db.flush()

    # Create SequenceSteps from the request, tracking IDs by step_number.
    from src.models.models import SequenceStep

    step_id_by_number: dict[int, str] = {}
    for s in req.steps:
        step_db_id = str(uuid.uuid4())
        step_id_by_number[s.step_number] = step_db_id
        step = SequenceStep(
            id=step_db_id,
            sequence_id=sequence.id,
            step_number=s.step_number,
            subject=s.subject,
            body=s.body,
        )
        db.add(step)
    await db.flush()

    # Select mailbox (reserve capacity atomically).
    mailbox = await select_mailbox(db, tenant_id)
    if not mailbox:
        # No capacity — delete the pending idempotency record so a retry proceeds.
        record_to_delete = await db.get(IdempotencyRecord, (IDEMPOTENCY_SCOPE, key))
        if record_to_delete is not None:
            await db.delete(record_to_delete)
        await db.commit()
        return JSONResponse(
            status_code=429,
            content={
                "contract_version": ENROLLMENT_CONTRACT_VERSION,
                "status": "rejected",
                "idempotency_key": key,
                "reason_code": "policy.capacity.exhausted",
            },
            headers={"Retry-After": str(CAPACITY_RETRY_AFTER_SECONDS)},
        )

    # Create enrollment + steps.
    enrollment = SequenceEnrollment(
        id=str(uuid.uuid4()),
        sequence_id=sequence.id,
        mailbox_id=mailbox.id,
        contact_email=f"{req.contact_id}@scout-v2.local",
        contact_name=None,
        timezone="America/New_York",
        external_ref=req.contact_id,
    )
    db.add(enrollment)
    await db.flush()

    sorted_steps = sorted(req.steps, key=lambda s: s.step_number)
    for i, s in enumerate(sorted_steps):
        enrollment_step = SequenceEnrollmentStep(
            id=str(uuid.uuid4()),
            enrollment_id=enrollment.id,
            step_id=step_id_by_number[s.step_number],
            status=EnrollmentStepStatus.SCHEDULED if i == 0 else EnrollmentStepStatus.PENDING,
            custom_subject=s.subject,
            custom_body=s.body,
        )
        db.add(enrollment_step)

    capacity_date = _local_mailbox_date()

    # Update idempotency record to completed with the result.
    record = await db.get(IdempotencyRecord, (IDEMPOTENCY_SCOPE, key))
    if record is not None:
        record.status = "completed"
        record.result = {
            "enrollment_id": enrollment.id,
            "capacity_date": capacity_date,
        }
        record.completed_at = datetime.utcnow()

    await db.commit()

    return JSONResponse(
        status_code=201,
        content=EnrollmentResponse(
            contract_version=ENROLLMENT_CONTRACT_VERSION,
            status=EnrollmentStatusContract.RESERVED,
            enrollment_id=enrollment.id,
            idempotency_key=key,
            capacity_date=capacity_date,
            reason_code=None,
        ).model_dump(),
    )


# SV2-044 r4 (FAIL 2): GET-lookup endpoint for timeout reconciliation.
# The v2 enrollment client GETs this BEFORE any retry-POST — a read, not a
# POST. If the lookup shows an existing enrollment (200), the client returns
# it (no POST). If 404 (no prior attempt landed), the client retries the
# POST. The r3 client re-POSTed on timeout (labelled a "lookup" but actually
# a second POST) — LOOKUP_BEFORE_RETRY=False. r4 is the real reconciliation.
@router.get("/enrollments/by-idempotency-key/{key}")
async def lookup_enrollment_by_idempotency_key(
    key: str,
    db: AsyncSession = Depends(get_db),
):
    """Lookup an enrollment by idempotency key (SV2-044 r4, FAIL 2).

    A READ endpoint for timeout reconciliation: the v2 client GETs this
    BEFORE retrying a POST (``LOOKUP_BEFORE_RETRY=True``).

    Returns:
      - 200 with ``EnrollmentResponse(status=EXISTING, enrollment_id,
        capacity_date)`` if the idempotency record is found and completed
        (the prior attempt landed and finished).
      - 200 with ``EnrollmentResponse(status=EXISTING, enrollment_id=None,
        reason_code="sequence.idempotency_pending")`` if the record is
        found but pending (the prior attempt crashed/timed out before
        completing — the client retries to get the completed id).
      - 404 if no record exists for the key (the prior attempt did not
        land — the client retries the POST via ``enroll``).
    """
    record_q = await db.execute(
        select(IdempotencyRecord).where(
            IdempotencyRecord.scope == IDEMPOTENCY_SCOPE,
            IdempotencyRecord.idempotency_key == key,
        )
    )
    rec = record_q.scalar_one_or_none()
    if rec is None:
        return JSONResponse(
            status_code=404,
            content={
                "contract_version": ENROLLMENT_CONTRACT_VERSION,
                "idempotency_key": key,
                "detail": "no enrollment found for this idempotency key",
            },
        )

    result = rec.result
    if rec.status == "pending" or result is None:
        # Prior attempt still in progress (crashed/timed out) — return
        # existing with no enrollment_id yet; caller retries to get completed.
        return JSONResponse(
            status_code=200,
            content=EnrollmentResponse(
                contract_version=ENROLLMENT_CONTRACT_VERSION,
                status=EnrollmentStatusContract.EXISTING,
                enrollment_id=None,
                idempotency_key=key,
                capacity_date=None,
                reason_code="sequence.idempotency_pending",
            ).model_dump(),
        )

    return JSONResponse(
        status_code=200,
        content=EnrollmentResponse(
            contract_version=ENROLLMENT_CONTRACT_VERSION,
            status=EnrollmentStatusContract.EXISTING,
            enrollment_id=result.get("enrollment_id"),
            idempotency_key=key,
            capacity_date=result.get("capacity_date"),
            reason_code=None,
        ).model_dump(),
    )
