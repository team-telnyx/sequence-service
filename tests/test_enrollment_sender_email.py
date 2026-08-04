"""Tests for Task 4.1: honoring `sender_email` in the enrollment API.

Behavior under test (src/api/enrollments.py create_enrollment):
  - sender_email valid+ACTIVE+capacity -> sticky to THAT mailbox (no rotation)
  - sender_email not in tenant allowlist -> log warning, fall back to rotation,
    still create enrollment (no 4xx)
  - sender_email allowed but no ACTIVE row / at capacity -> fall back, still create
  - sender_email None -> unchanged rotation behavior

REVOPS-1499 — `sender_policy` strict mode:
  - strict + available sender -> enrolled on the requested mailbox
  - strict + at_capacity -> 409 reason=at_capacity, NO enrollment row created
  - strict + inactive (no ACTIVE row) -> 409 reason=inactive
  - strict + not_allowed (ValueError path) -> 409 reason=not_allowed
  - strict without sender_email -> 422
  - unknown policy value -> 422
  - null policy -> 422 (regression pin: null must NOT silently rotate)
  - omitted policy -> rotation fallback still occurs (regression pin)
"""

import pytest
from sqlalchemy import select, func

from src.models.models import SequenceEnrollment


async def _create(client, api_key, payload):
    return await client.post(
        "/api/enrollments/", json=payload, headers={"X-API-Key": api_key}
    )


async def _fetch_enrollment(session_factory, enrollment_id):
    async with session_factory() as db:
        res = await db.execute(
            select(SequenceEnrollment).where(SequenceEnrollment.id == enrollment_id)
        )
        return res.scalar_one()


@pytest.mark.asyncio
async def test_valid_active_sender_is_sticky(client, seeded, session_factory):
    """A valid ACTIVE allowlisted mailbox with capacity is used verbatim."""
    resp = await _create(client, seeded["api_key"], {
        "sequence_id": seeded["sequence_id"],
        "contact_email": "lead1@example.com",
        "sender_email": seeded["active_mailbox_email"],
    })
    assert resp.status_code == 201, resp.text
    body = resp.json()["data"]
    assert body["mailbox_id"] == seeded["active_mailbox_id"]

    row = await _fetch_enrollment(session_factory, body["id"])
    assert row.mailbox_id == seeded["active_mailbox_id"]


@pytest.mark.asyncio
async def test_not_allowed_sender_falls_back_and_warns(client, seeded, session_factory, caplog):
    """An email not in the tenant allowlist (ValueError path) falls back to
    rotation, logs a warning, and still creates the enrollment."""
    resp = await _create(client, seeded["api_key"], {
        "sequence_id": seeded["sequence_id"],
        "contact_email": "lead2@example.com",
        "sender_email": seeded["not_allowed_email"],
    })
    assert resp.status_code == 201, resp.text
    body = resp.json()["data"]

    # Fell back to a rotation pick -> one of the eligible scout mailboxes,
    # never the disallowed/full/paused ones.
    assert body["mailbox_id"] in {seeded["active_mailbox_id"], "mb-other"}

    row = await _fetch_enrollment(session_factory, body["id"])
    assert row.mailbox_id in {seeded["active_mailbox_id"], "mb-other"}


@pytest.mark.asyncio
async def test_allowed_but_at_capacity_falls_back(client, seeded, session_factory):
    """An allowlisted email whose mailbox is at capacity falls back to rotation
    and still creates the enrollment (never sticks to the full mailbox)."""
    resp = await _create(client, seeded["api_key"], {
        "sequence_id": seeded["sequence_id"],
        "contact_email": "lead3@example.com",
        "sender_email": seeded["full_mailbox_email"],
    })
    assert resp.status_code == 201, resp.text
    body = resp.json()["data"]
    assert body["mailbox_id"] != seeded["full_mailbox_id"]
    assert body["mailbox_id"] in {seeded["active_mailbox_id"], "mb-other"}


@pytest.mark.asyncio
async def test_allowed_but_not_active_falls_back(client, seeded, session_factory):
    """An allowlisted email whose only row is PAUSED (no ACTIVE row) falls back."""
    resp = await _create(client, seeded["api_key"], {
        "sequence_id": seeded["sequence_id"],
        "contact_email": "lead4@example.com",
        "sender_email": seeded["paused_mailbox_email"],
    })
    assert resp.status_code == 201, resp.text
    body = resp.json()["data"]
    # PAUSED mailbox has no ACTIVE row -> not stuck to it; rotation eligible only.
    assert body["mailbox_id"] in {seeded["active_mailbox_id"], "mb-other"}


@pytest.mark.asyncio
async def test_no_sender_email_uses_rotation(client, seeded, session_factory):
    """sender_email omitted -> unchanged rotation behavior, enrollment created."""
    resp = await _create(client, seeded["api_key"], {
        "sequence_id": seeded["sequence_id"],
        "contact_email": "lead5@example.com",
    })
    assert resp.status_code == 201, resp.text
    body = resp.json()["data"]
    # Rotation only picks ACTIVE + capacity scout mailboxes.
    assert body["mailbox_id"] in {seeded["active_mailbox_id"], "mb-other"}


# ─────────────────────────────────────────────────────────────────
# REVOPS-1499: strict sender policy — defer instead of rotating
# ─────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_strict_policy_with_available_sender_enrolls(client, seeded, session_factory):
    """strict + allowed + ACTIVE + capacity -> enrolled on the requested mailbox."""
    resp = await _create(client, seeded["api_key"], {
        "sequence_id": seeded["sequence_id"],
        "contact_email": "strict1@example.com",
        "sender_email": seeded["active_mailbox_email"],
        "sender_policy": "strict",
    })
    assert resp.status_code == 201, resp.text
    body = resp.json()["data"]
    assert body["mailbox_id"] == seeded["active_mailbox_id"]

    row = await _fetch_enrollment(session_factory, body["id"])
    assert row.mailbox_id == seeded["active_mailbox_id"]


@pytest.mark.asyncio
async def test_strict_policy_at_capacity_returns_409_and_no_row(client, seeded, session_factory):
    """strict + at-capacity mailbox -> 409 reason=at_capacity, NO enrollment row created."""
    contact = "strict2@example.com"
    async with session_factory() as db:
        before = (await db.execute(
            select(func.count()).select_from(SequenceEnrollment)
            .where(SequenceEnrollment.contact_email == contact)
        )).scalar_one()
    assert before == 0

    resp = await _create(client, seeded["api_key"], {
        "sequence_id": seeded["sequence_id"],
        "contact_email": contact,
        "sender_email": seeded["full_mailbox_email"],
        "sender_policy": "strict",
    })
    assert resp.status_code == 409, resp.text
    body = resp.json()
    assert body["error"] == "sender_unavailable"
    assert body["reason"] == "at_capacity"
    assert body["sender_email"] == seeded["full_mailbox_email"]

    # No enrollment row was created — strict must defer, not rotate-and-enroll.
    async with session_factory() as db:
        after = (await db.execute(
            select(func.count()).select_from(SequenceEnrollment)
            .where(SequenceEnrollment.contact_email == contact)
        )).scalar_one()
    assert after == 0


@pytest.mark.asyncio
async def test_strict_policy_inactive_returns_409(client, seeded, session_factory):
    """strict + no ACTIVE mailbox row (PAUSED) -> 409 reason=inactive."""
    resp = await _create(client, seeded["api_key"], {
        "sequence_id": seeded["sequence_id"],
        "contact_email": "strict3@example.com",
        "sender_email": seeded["paused_mailbox_email"],
        "sender_policy": "strict",
    })
    assert resp.status_code == 409, resp.text
    body = resp.json()
    assert body["error"] == "sender_unavailable"
    assert body["reason"] == "inactive"
    assert body["sender_email"] == seeded["paused_mailbox_email"]


@pytest.mark.asyncio
async def test_strict_policy_not_allowed_returns_409(client, seeded, session_factory):
    """strict + sender not in tenant allowlist -> 409 reason=not_allowed."""
    resp = await _create(client, seeded["api_key"], {
        "sequence_id": seeded["sequence_id"],
        "contact_email": "strict4@example.com",
        "sender_email": seeded["not_allowed_email"],
        "sender_policy": "strict",
    })
    assert resp.status_code == 409, resp.text
    body = resp.json()
    assert body["error"] == "sender_unavailable"
    assert body["reason"] == "not_allowed"
    assert body["sender_email"] == seeded["not_allowed_email"]


@pytest.mark.asyncio
async def test_strict_policy_without_sender_email_returns_422(client, seeded, session_factory):
    """strict without sender_email -> 422 (strict is meaningless without a sender)."""
    resp = await _create(client, seeded["api_key"], {
        "sequence_id": seeded["sequence_id"],
        "contact_email": "strict5@example.com",
        "sender_policy": "strict",
    })
    assert resp.status_code == 422, resp.text


@pytest.mark.asyncio
async def test_unknown_sender_policy_returns_422(client, seeded, session_factory):
    """Unknown sender_policy value -> 422."""
    resp = await _create(client, seeded["api_key"], {
        "sequence_id": seeded["sequence_id"],
        "contact_email": "strict6@example.com",
        "sender_policy": "bogus",
    })
    assert resp.status_code == 422, resp.text


@pytest.mark.asyncio
async def test_null_sender_policy_returns_422(client, seeded, session_factory):
    """Regression pin: sender_policy=null -> 422. Null must NOT be accepted as
    "use the default" and silently rotate — the caller explicitly passed a
    value, and that value is invalid for a non-optional field (REVOPS-1499).
    """
    resp = await _create(client, seeded["api_key"], {
        "sequence_id": seeded["sequence_id"],
        "contact_email": "strict6b@example.com",
        "sender_policy": None,
    })
    assert resp.status_code == 422, resp.text


@pytest.mark.asyncio
async def test_omitted_policy_rotation_fallback_regression(client, seeded, session_factory):
    """Regression pin: omitted sender_policy -> rotation fallback still occurs
    (a not-allowed sender_email with no policy still falls back to rotation,
    bit-identical to today's behavior)."""
    resp = await _create(client, seeded["api_key"], {
        "sequence_id": seeded["sequence_id"],
        "contact_email": "strict7@example.com",
        "sender_email": seeded["not_allowed_email"],
        # no sender_policy -> defaults to "rotate"
    })
    assert resp.status_code == 201, resp.text
    body = resp.json()["data"]
    assert body["mailbox_id"] in {seeded["active_mailbox_id"], "mb-other"}
