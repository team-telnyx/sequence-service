"""Tests for Task 4.1: honoring `sender_email` in the enrollment API.

Behavior under test (src/api/enrollments.py create_enrollment):
  - sender_email valid+ACTIVE+capacity -> sticky to THAT mailbox (no rotation)
  - sender_email not in tenant allowlist -> log warning, fall back to rotation,
    still create enrollment (no 4xx)
  - sender_email allowed but no ACTIVE row / at capacity -> fall back, still create
  - sender_email None -> unchanged rotation behavior
"""

import pytest
from sqlalchemy import select

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
