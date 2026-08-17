"""Versioned contract types shared between seq-svc and scout-v2 (SV2-044).

These mirror the docs/10 contract specifications exactly. The scout-v2
side defines equivalent types in packages/contracts/sequence.py; both
sides must agree (enforced by contract-compatibility tests).

SV2-044 r3: ``ReplyIntent`` and ``REPLY_INTENT_CONTRACT_VERSION`` live HERE
(in the contract module the compat test imports), not only in models.py. A
copy living only in models.py was a silent contract break — the compat test
could not bite on a divergent server enum because it never imported the
contract module's version of the enum. r3 moves the canonical definition
here (models.py keeps a re-export for back-compat with existing callers).
"""

from __future__ import annotations

import enum
from typing import Optional

from pydantic import BaseModel, Field


ENROLLMENT_CONTRACT_VERSION = 1
DELIVERY_EVENT_CONTRACT_VERSION = 1
REPLY_INTENT_CONTRACT_VERSION = 1


class EnrollmentStatusContract(str, enum.Enum):
    """docs/10 enrollment response status."""

    RESERVED = "reserved"
    EXISTING = "existing"
    REJECTED = "rejected"


class DeliveryEventStatus(str, enum.Enum):
    """Full Telnyx Email API delivery event set (SV2-044)."""

    SENT = "sent"
    DELIVERED = "delivered"
    BOUNCED = "bounced"
    OPENED = "opened"
    CLICKED = "clicked"
    SUPPRESSED = "suppressed"
    UNSUBSCRIBED = "unsubscribed"


class ReplyIntent(str, enum.Enum):
    """Versioned reply-intent taxonomy (SV2-044, contract_version=1).

    Mirrors ``packages.contracts.sequence.ReplyIntent`` on the v2 side
    EXACTLY — both sides must agree (enforced by
    tests/contract/test_sequence_contract_compat.py on the v2 side and
    tests/test_sv2_044_contract.py on the server side). A divergent server
    enum is a contract break the compat test must bite on.

    Bounce is NOT a ReplyIntent — it is structurally detected (Gmail
    is_bounce flag or Email-API bounce webhook event) and handled as a
    separate SignalType. v1's poller conflated bounce with REPLY
    (154/168 of the live backlog were mailer-daemon bounces recorded as
    REPLY); the r3 ingest path classifies bounce DISTINCTLY via the
    is_bounce structural flag BEFORE any ReplyIntent classification.
    Referral routing is out of scope (REVOPS-1458).
    """

    POSITIVE_INTEREST = "positive_interest"
    POSITIVE_MEETING = "positive_meeting"
    NEGATIVE_NOT_INTERESTED = "negative_not_interested"
    NEGATIVE_WRONG_PERSON = "negative_wrong_person"
    OUT_OF_OFFICE = "out_of_office"
    AUTORESPONDER = "autoresponder"
    UNSUBSCRIBE_REQUEST = "unsubscribe_request"
    UNKNOWN = "unknown"


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


class DeliveryEventContract(BaseModel):
    """Versioned delivery-event contract for Email API webhook events."""

    contract_version: int = DELIVERY_EVENT_CONTRACT_VERSION
    event_id: str
    status: DeliveryEventStatus
    message_id: str
    to_email: str
    occurred_at: Optional[str] = None
