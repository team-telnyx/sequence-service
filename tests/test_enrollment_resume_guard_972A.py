"""REVOPS-972 Workstream A — enrollment API hardening.

Covers:
  B1 (resume guard): POST /resume must REFUSE to resume an enrollment whose
     pause_reason is in {reply, bounce, unsubscribe} unless an explicit `force`
     flag is passed. A valid resume (circuit_breaker, manual, or forced) must
     CLEAR pause_reason, flip to ACTIVE, and RE-QUEUE the next pending/scheduled
     step (mirroring circuit_resume._resume_mailbox).
  B2 (capacity contract): when select_mailbox returns None because every
     allowlisted mailbox is AT CAPACITY, the create API returns HTTP 429 with a
     Retry-After header; 503 is reserved for true zero-active-mailbox infra.
  Identity: create accepts external_ref/prospect_id and echoes it on the response.
"""

import uuid

import pytest
from sqlalchemy import select

from src.models.models import (
    SequenceEnrollment,
    SequenceEnrollmentStep,
    EnrollmentStatus,
    EnrollmentStepStatus,
    Mailbox,
    MailboxStatus,
)


def _hdr(api_key):
    return {"X-API-Key": api_key}


async def _make_paused_enrollment(session_factory, seeded, pause_reason):
    """Create a PAUSED enrollment on seq-1 with two steps; next step PENDING."""
    eid = str(uuid.uuid4())
    async with session_factory() as db:
        db.add(SequenceEnrollment(
            id=eid,
            sequence_id=seeded["sequence_id"],
            mailbox_id=seeded["active_mailbox_id"],
            contact_email=f"{uuid.uuid4().hex}@example.com",
            status=EnrollmentStatus.PAUSED,
            pause_reason=pause_reason,
            current_step=1,
        ))
        db.add(SequenceEnrollmentStep(
            id=str(uuid.uuid4()), enrollment_id=eid, step_id="step-1",
            status=EnrollmentStepStatus.SENT,
        ))
        db.add(SequenceEnrollmentStep(
            id=str(uuid.uuid4()), enrollment_id=eid, step_id="step-2",
            status=EnrollmentStepStatus.PENDING,
        ))
        await db.commit()
    return eid


async def _fetch(session_factory, eid):
    async with session_factory() as db:
        return (await db.execute(
            select(SequenceEnrollment).where(SequenceEnrollment.id == eid)
        )).scalar_one()


# --------------------------------------------------------------------------- #
# B1: resume guard
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["reply", "bounce", "unsubscribe"])
async def test_resume_refuses_reply_paused_without_force(
    client, seeded, session_factory, reason
):
    eid = await _make_paused_enrollment(session_factory, seeded, reason)
    resp = await client.post(f"/api/enrollments/{eid}/resume", headers=_hdr(seeded["api_key"]))
    assert resp.status_code == 409, resp.text

    row = await _fetch(session_factory, eid)
    assert row.status == EnrollmentStatus.PAUSED  # untouched
    assert row.pause_reason == reason


@pytest.mark.asyncio
async def test_resume_reply_paused_with_force_clears_and_requeues(
    client, seeded, session_factory
):
    eid = await _make_paused_enrollment(session_factory, seeded, "reply")
    resp = await client.post(
        f"/api/enrollments/{eid}/resume?force=true", headers=_hdr(seeded["api_key"])
    )
    assert resp.status_code == 200, resp.text

    row = await _fetch(session_factory, eid)
    assert row.status == EnrollmentStatus.ACTIVE
    assert row.pause_reason is None

    # next pending step re-queued -> SCHEDULED with scheduled_at set
    async with session_factory() as db:
        nxt = (await db.execute(
            select(SequenceEnrollmentStep).where(
                SequenceEnrollmentStep.enrollment_id == eid,
                SequenceEnrollmentStep.step_id == "step-2",
            )
        )).scalar_one()
    assert nxt.status == EnrollmentStepStatus.SCHEDULED
    assert nxt.scheduled_at is not None

    # the queue call fired (mocked in conftest)
    import src.api.enrollments as em
    assert em.queue_sequence_step.await_count >= 1


@pytest.mark.asyncio
async def test_resume_circuit_breaker_clears_and_requeues_without_force(
    client, seeded, session_factory
):
    eid = await _make_paused_enrollment(session_factory, seeded, "circuit_breaker")
    resp = await client.post(f"/api/enrollments/{eid}/resume", headers=_hdr(seeded["api_key"]))
    assert resp.status_code == 200, resp.text

    row = await _fetch(session_factory, eid)
    assert row.status == EnrollmentStatus.ACTIVE
    assert row.pause_reason is None


@pytest.mark.asyncio
async def test_resume_manual_paused_clears_and_requeues_without_force(
    client, seeded, session_factory
):
    eid = await _make_paused_enrollment(session_factory, seeded, "manual")
    resp = await client.post(f"/api/enrollments/{eid}/resume", headers=_hdr(seeded["api_key"]))
    assert resp.status_code == 200, resp.text
    row = await _fetch(session_factory, eid)
    assert row.status == EnrollmentStatus.ACTIVE
    assert row.pause_reason is None


# --------------------------------------------------------------------------- #
# B2: capacity contract — 429 at-capacity, 503 zero-active infra
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_create_returns_429_when_all_mailboxes_at_capacity(
    client, seeded, session_factory
):
    # Drive every ACTIVE allowlisted mailbox to capacity, leave at least one
    # ACTIVE row (so it's at-capacity, NOT zero-active infra).
    async with session_factory() as db:
        rows = (await db.execute(
            select(Mailbox).where(Mailbox.status == MailboxStatus.ACTIVE)
        )).scalars().all()
        for m in rows:
            m.sent_today = m.daily_send_limit
        await db.commit()

    resp = await client.post(
        "/api/enrollments/",
        json={"sequence_id": seeded["sequence_id"], "contact_email": "cap@example.com"},
        headers=_hdr(seeded["api_key"]),
    )
    assert resp.status_code == 429, resp.text
    assert "retry-after" in {k.lower() for k in resp.headers.keys()}


@pytest.mark.asyncio
async def test_create_returns_503_when_no_active_mailboxes(
    client, seeded, session_factory
):
    # Disable every mailbox -> true zero-active infra failure -> 503.
    async with session_factory() as db:
        rows = (await db.execute(select(Mailbox))).scalars().all()
        for m in rows:
            m.status = MailboxStatus.DISABLED
        await db.commit()

    resp = await client.post(
        "/api/enrollments/",
        json={"sequence_id": seeded["sequence_id"], "contact_email": "infra@example.com"},
        headers=_hdr(seeded["api_key"]),
    )
    assert resp.status_code == 503, resp.text


# --------------------------------------------------------------------------- #
# Identity: external_ref / prospect_id echoed on response
# --------------------------------------------------------------------------- #

@pytest.mark.asyncio
async def test_create_echoes_external_ref(client, seeded, session_factory):
    resp = await client.post(
        "/api/enrollments/",
        json={
            "sequence_id": seeded["sequence_id"],
            "contact_email": "ident@example.com",
            "external_ref": "prospect-abc-123",
        },
        headers=_hdr(seeded["api_key"]),
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()["data"]
    assert body["external_ref"] == "prospect-abc-123"

    async with session_factory() as db:
        row = (await db.execute(
            select(SequenceEnrollment).where(SequenceEnrollment.id == body["id"])
        )).scalar_one()
    assert row.external_ref == "prospect-abc-123"


@pytest.mark.asyncio
async def test_create_accepts_prospect_id_alias(client, seeded):
    """`prospect_id` is accepted as an alias for external_ref."""
    resp = await client.post(
        "/api/enrollments/",
        json={
            "sequence_id": seeded["sequence_id"],
            "contact_email": "alias@example.com",
            "prospect_id": "p-999",
        },
        headers=_hdr(seeded["api_key"]),
    )
    assert resp.status_code == 201, resp.text
    assert resp.json()["data"]["external_ref"] == "p-999"


@pytest.mark.asyncio
async def test_create_external_ref_optional(client, seeded):
    resp = await client.post(
        "/api/enrollments/",
        json={"sequence_id": seeded["sequence_id"], "contact_email": "noref@example.com"},
        headers=_hdr(seeded["api_key"]),
    )
    assert resp.status_code == 201, resp.text
    assert resp.json()["data"]["external_ref"] is None
