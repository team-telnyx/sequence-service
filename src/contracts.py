"""Versioned contract types shared between seq-svc and scout-v2 (SV2-044).

These mirror the docs/10 contract specifications exactly. The scout-v2
side defines equivalent types in packages/contracts/sequence.py; both
sides must agree (enforced by contract-compatibility tests).
"""

from __future__ import annotations

import enum
from typing import Optional

from pydantic import BaseModel, Field


ENROLLMENT_CONTRACT_VERSION = 1
DELIVERY_EVENT_CONTRACT_VERSION = 1


class EnrollmentStatusContract(str, enum.Enum):
    """docs/10 enrollment response status."""

    RESERVED = "reserved"
    EXISTING = "existing"
    REJECTED = "rejected"


class EnrollmentStepRequest(BaseModel):
    step_number: int = Field(ge=1)
    delay_seconds: int = Field(ge=0, default=0)
    subject: str = Field(max_length=500)
    body: str = Field(max_length=50000)


class EnrollmentRequest(BaseModel):
    """docs/10 §Sequence enrollment contract request."""

    contract_version: int = Field(default=ENROLLMENT_CONTRACT_VERSION)
    idempotency_key: str = Field(min_length=1, max_length=500)
    correlation_id: str
    tenant_id: str
    account_id: str
    contact_id: str
    cohort_id: str
    mailbox_policy: str
    policy_version: str
    content_version: str
    steps: list[EnrollmentStepRequest]


class EnrollmentResponse(BaseModel):
    """docs/10 §Sequence enrollment contract response."""

    contract_version: int = ENROLLMENT_CONTRACT_VERSION
    status: EnrollmentStatusContract
    enrollment_id: Optional[str] = None
    idempotency_key: str
    capacity_date: Optional[str] = None
    reason_code: Optional[str] = None


class DeliveryEventStatus(str, enum.Enum):
    """Full Telnyx Email API delivery event set (SV2-044)."""

    SENT = "sent"
    DELIVERED = "delivered"
    BOUNCED = "bounced"
    OPENED = "opened"
    CLICKED = "clicked"
    SUPPRESSED = "suppressed"
    UNSUBSCRIBED = "unsubscribed"


class DeliveryEventContract(BaseModel):
    """Versioned delivery-event contract for Email API webhook events."""

    contract_version: int = DELIVERY_EVENT_CONTRACT_VERSION
    event_id: str
    status: DeliveryEventStatus
    message_id: str
    to_email: str
    occurred_at: Optional[str] = None
