"""SQLAlchemy models for Sequence Service.

Mirrors the Prisma schema from the TypeScript version.
"""

import enum
from datetime import datetime
from typing import Optional
from sqlalchemy import (
    CheckConstraint,
    ForeignKey,
    String,
    Text,
    Integer,
    Float,
    Boolean,
    Enum,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.models.base import Base


class MailboxStatus(str, enum.Enum):
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    WARMING = "WARMING"
    DISABLED = "DISABLED"


class SequenceStatus(str, enum.Enum):
    DRAFT = "DRAFT"
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    ARCHIVED = "ARCHIVED"


class EnrollmentStatus(str, enum.Enum):
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    COMPLETED = "COMPLETED"
    BOUNCED = "BOUNCED"
    UNSUBSCRIBED = "UNSUBSCRIBED"


class EnrollmentStepStatus(str, enum.Enum):
    PENDING = "PENDING"
    SCHEDULED = "SCHEDULED"
    SENT = "SENT"
    SKIPPED = "SKIPPED"
    BOUNCED = "BOUNCED"


class SignalType(str, enum.Enum):
    REPLY = "REPLY"
    OPEN = "OPEN"
    CLICK = "CLICK"
    BOUNCE = "BOUNCE"
    UNSUBSCRIBE = "UNSUBSCRIBE"
    OUT_OF_OFFICE = "OUT_OF_OFFICE"


class Tenant(Base):
    """Multi-tenant organization."""
    
    __tablename__ = "tenants"
    
    name: Mapped[str] = mapped_column(String(255))
    api_key: Mapped[str] = mapped_column(String(255), unique=True)
    
    # Relationships
    mailboxes: Mapped[list["Mailbox"]] = relationship(back_populates="tenant")
    sequences: Mapped[list["Sequence"]] = relationship(back_populates="tenant")
    webhook_configs: Mapped[list["WebhookConfig"]] = relationship(back_populates="tenant")


class Mailbox(Base):
    """Gmail mailbox for sending."""
    
    __tablename__ = "mailboxes"
    
    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenants.id"))
    email: Mapped[str] = mapped_column(String(255))
    display_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    status: Mapped[MailboxStatus] = mapped_column(Enum(MailboxStatus), default=MailboxStatus.ACTIVE)
    weight: Mapped[int] = mapped_column(Integer, default=1)
    daily_send_limit: Mapped[int] = mapped_column(Integer, default=50)
    sent_today: Mapped[int] = mapped_column(Integer, default=0)
    
    # Relationships
    tenant: Mapped["Tenant"] = relationship(back_populates="mailboxes")
    sent_emails: Mapped[list["SentEmail"]] = relationship(back_populates="mailbox")
    enrollment_steps: Mapped[list["SequenceEnrollmentStep"]] = relationship(back_populates="mailbox")
    enrollments: Mapped[list["SequenceEnrollment"]] = relationship(back_populates="mailbox")
    
    __table_args__ = (
        UniqueConstraint("tenant_id", "email", name="uq_mailbox_tenant_email"),
    )


class Sequence(Base):
    """Email sequence definition."""
    
    __tablename__ = "sequences"
    
    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenants.id"))
    name: Mapped[str] = mapped_column(String(255))
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    status: Mapped[SequenceStatus] = mapped_column(Enum(SequenceStatus), default=SequenceStatus.DRAFT)
    
    # Relationships
    tenant: Mapped["Tenant"] = relationship(back_populates="sequences")
    steps: Mapped[list["SequenceStep"]] = relationship(back_populates="sequence", order_by="SequenceStep.step_number")
    enrollments: Mapped[list["SequenceEnrollment"]] = relationship(back_populates="sequence")


class SequenceStep(Base):
    """Individual step in a sequence."""
    
    __tablename__ = "sequence_steps"
    
    sequence_id: Mapped[str] = mapped_column(ForeignKey("sequences.id"))
    step_number: Mapped[int] = mapped_column(Integer)
    subject: Mapped[str] = mapped_column(String(500))
    body: Mapped[str] = mapped_column(Text)
    delay_days: Mapped[int] = mapped_column(Integer, default=0)
    delay_hours: Mapped[int] = mapped_column(Integer, default=0)
    
    # Relationships
    sequence: Mapped["Sequence"] = relationship(back_populates="steps")
    enrollment_steps: Mapped[list["SequenceEnrollmentStep"]] = relationship(back_populates="step")
    
    __table_args__ = (
        UniqueConstraint("sequence_id", "step_number", name="uq_sequence_step_number"),
    )


class SequenceEnrollment(Base):
    """Contact enrolled in a sequence."""
    
    __tablename__ = "sequence_enrollments"
    
    sequence_id: Mapped[str] = mapped_column(ForeignKey("sequences.id"))
    mailbox_id: Mapped[str] = mapped_column(ForeignKey("mailboxes.id"))  # Sticky sender - assigned at enrollment
    contact_email: Mapped[str] = mapped_column(String(255))
    contact_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    timezone: Mapped[str] = mapped_column(String(63), default="America/New_York", server_default="America/New_York")
    status: Mapped[EnrollmentStatus] = mapped_column(Enum(EnrollmentStatus), default=EnrollmentStatus.ACTIVE)
    current_step: Mapped[int] = mapped_column(Integer, default=1)
    # pause_reason: WHY an enrollment is PAUSED, constrained to a known set by a
    # CHECK so a reply/bounce/unsubscribe pause is always identifiable and the
    # circuit-resume worker / manual-resume endpoint can guard on it instead of
    # treating an unconstrained string as resumable (REVOPS-972 B1/H5). The
    # migration (orchestrator-owned) applies the CHECK to the live DB; this ORM
    # mirrors that intent so create_all test DBs and the live schema agree.
    # Nullable on purpose: an ACTIVE/resumed enrollment has NO pause reason
    # (circuit_resume + the manual-resume endpoint both clear it to NULL). The
    # CHECK constrains the *value* to the known set when present, so a typo/legacy
    # reason can never slip in, while still allowing NULL for not-paused rows. The
    # `default`/`server_default='manual'` only applies on INSERT when the caller
    # omits it (an operator pause), never overriding an explicit reason. This is
    # the durable B1 schema guard (REVOPS-972 H5) without breaking the resume path.
    pause_reason: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True, default=None,
    )
    # External identity from the producer (Scout prospect id). Nullable so legacy
    # rows and non-Scout callers are unaffected; lets reply/bounce signals join back
    # to the originating prospect deterministically instead of email-only matching
    # (REVOPS-972 identity). DB column applied by migration 001.
    external_ref: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)
    
    # Relationships
    sequence: Mapped["Sequence"] = relationship(back_populates="enrollments")
    mailbox: Mapped["Mailbox"] = relationship(back_populates="enrollments")
    steps: Mapped[list["SequenceEnrollmentStep"]] = relationship(back_populates="enrollment")
    
    __table_args__ = (
        UniqueConstraint("sequence_id", "contact_email", name="uq_enrollment_sequence_contact"),
        CheckConstraint(
            "pause_reason IS NULL OR pause_reason IN "
            "('circuit_breaker', 'reply', 'unsubscribe', 'bounce', 'manual')",
            name="ck_enrollment_pause_reason",
        ),
    )


class SequenceEnrollmentStep(Base):
    """Tracking for each step of an enrollment."""
    
    __tablename__ = "sequence_enrollment_steps"
    
    enrollment_id: Mapped[str] = mapped_column(ForeignKey("sequence_enrollments.id"))
    step_id: Mapped[str] = mapped_column(ForeignKey("sequence_steps.id"))
    mailbox_id: Mapped[Optional[str]] = mapped_column(ForeignKey("mailboxes.id"), nullable=True)
    status: Mapped[EnrollmentStepStatus] = mapped_column(Enum(EnrollmentStepStatus), default=EnrollmentStepStatus.PENDING)
    scheduled_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    sent_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    
    # Scout-composed content (overrides step template if present)
    custom_subject: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)
    custom_body: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Relationships
    enrollment: Mapped["SequenceEnrollment"] = relationship(back_populates="steps")
    step: Mapped["SequenceStep"] = relationship(back_populates="enrollment_steps")
    mailbox: Mapped[Optional["Mailbox"]] = relationship(back_populates="enrollment_steps")
    sent_emails: Mapped[list["SentEmail"]] = relationship(back_populates="enrollment_step")


class SentEmail(Base):
    """Record of sent emails."""
    
    __tablename__ = "sent_emails"
    
    message_id: Mapped[str] = mapped_column(String(255), unique=True)
    thread_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    mailbox_id: Mapped[str] = mapped_column(ForeignKey("mailboxes.id"))
    enrollment_step_id: Mapped[str] = mapped_column(ForeignKey("sequence_enrollment_steps.id"))
    subject: Mapped[str] = mapped_column(String(500))
    body: Mapped[str] = mapped_column(Text)
    to_email: Mapped[str] = mapped_column(String(255))
    to_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    from_email: Mapped[str] = mapped_column(String(255))
    from_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    sent_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)
    
    # Relationships
    mailbox: Mapped["Mailbox"] = relationship(back_populates="sent_emails")
    enrollment_step: Mapped["SequenceEnrollmentStep"] = relationship(back_populates="sent_emails")
    signals: Mapped[list["Signal"]] = relationship(back_populates="sent_email")


class Signal(Base):
    """Engagement signals detected from inbox."""
    
    __tablename__ = "signals"
    
    sent_email_id: Mapped[str] = mapped_column(ForeignKey("sent_emails.id"))
    type: Mapped[SignalType] = mapped_column(Enum(SignalType))
    detected_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)
    raw_data: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Relationships
    sent_email: Mapped["SentEmail"] = relationship(back_populates="signals")


class SuppressionReason(str, enum.Enum):
    UNSUBSCRIBE = "UNSUBSCRIBE"
    BOUNCE = "BOUNCE"
    COMPLAINT = "COMPLAINT"
    MANUAL = "MANUAL"


class Suppression(Base):
    """Email suppression list — do not contact."""
    
    __tablename__ = "suppressions"
    
    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenants.id"))
    email: Mapped[str] = mapped_column(String(255), index=True)
    domain: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)
    reason: Mapped[SuppressionReason] = mapped_column(Enum(SuppressionReason, name="suppression_reason", create_type=False))
    source_enrollment_id: Mapped[Optional[str]] = mapped_column(ForeignKey("sequence_enrollments.id"), nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Relationships
    tenant: Mapped["Tenant"] = relationship()
    
    __table_args__ = (
        UniqueConstraint("tenant_id", "email", name="uq_suppression_tenant_email"),
    )


class WebhookConfig(Base):
    """Webhook configuration for consumers."""
    
    __tablename__ = "webhook_configs"
    
    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenants.id"))
    url: Mapped[str] = mapped_column(String(500))
    secret: Mapped[str] = mapped_column(String(255))
    events: Mapped[str] = mapped_column(Text)  # JSON array of event types
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    
    # Relationships
    tenant: Mapped["Tenant"] = relationship(back_populates="webhook_configs")
    deliveries: Mapped[list["WebhookDelivery"]] = relationship(back_populates="config")


class WebhookDelivery(Base):
    """Webhook delivery attempt tracking."""
    
    __tablename__ = "webhook_deliveries"
    
    config_id: Mapped[str] = mapped_column(ForeignKey("webhook_configs.id"))
    event_type: Mapped[str] = mapped_column(String(100))
    payload: Mapped[str] = mapped_column(Text)  # JSON payload
    status: Mapped[str] = mapped_column(String(50), default="PENDING")
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_attempt_at: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    response_status: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    response_body: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    
    # Relationships
    config: Mapped["WebhookConfig"] = relationship(back_populates="deliveries")
