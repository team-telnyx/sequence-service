-- SV2-044 r5 (FAIL 2): baseline migration — the pre-migration base schema.
--
-- seq-svc historically built tables via startup Base.metadata.create_all with
-- no maintained migration history. Migrations 001-007 are ADD-ONS that assume
-- the base schema already exists. This baseline reproduces the schema
-- create_all would produce FOR TABLES/COLUMNS/CONSTRAINTS NOT COVERED by
-- 001-007, so the full chain 000->007 reproduces the COMPLETE schema with NO
-- create_all.
--
-- What 001-007 own (NOT in this baseline — they add it):
--   001: uq_mailbox_email, uq_suppression_email, ck_enrollment_pause_reason,
--       external_ref column + index
--   002: transport column + ck_mailbox_transport CHECK
--   003: 'FAILED' value on enrollmentstepstatus enum
--   004: 'API_BOUNCE','API_COMPLAINT','API_UNSUBSCRIBE' on suppression_reason
--   005: delivered_at column on sent_emails
--   006: idempotency_records table (entire)
--   007: reply_intent enum type + reply_intent column on signals
--
-- Enum values: create_all emits the enum MEMBER NAMES (uppercase) by default.
-- reply_intent uses values_callable (lowercase .value) and is created by 007.
-- suppression_reason uses create_type=False — this baseline creates it with
-- the 4 base values; 004 adds the 3 API_* values.
-- enrollmentstepstatus is created with 5 base values; 003 adds 'FAILED'.
--
-- Generated from the ORM metadata (src/models/models.py) via the PG dialect
-- DDL compiler, then hand-stripped of migration-owned parts. A drift assertion
-- in tests/conftest.py compares the final 000->007 schema to the ORM metadata
-- so any future ORM/migration divergence fails loudly.

-- ── Base enum types (create_type=True, NOT reply_intent) ──────────────────

CREATE TYPE mailboxstatus AS ENUM ('ACTIVE', 'PAUSED', 'WARMING', 'DISABLED');
CREATE TYPE sequencestatus AS ENUM ('DRAFT', 'ACTIVE', 'PAUSED', 'ARCHIVED');
CREATE TYPE enrollmentstatus AS ENUM ('ACTIVE', 'PAUSED', 'COMPLETED', 'BOUNCED', 'UNSUBSCRIBED');
-- 5 base values; migration 003 adds 'FAILED'.
CREATE TYPE enrollmentstepstatus AS ENUM ('PENDING', 'SCHEDULED', 'SENT', 'SKIPPED', 'BOUNCED');
CREATE TYPE signaltype AS ENUM ('REPLY', 'OPEN', 'CLICK', 'BOUNCE', 'UNSUBSCRIBE', 'OUT_OF_OFFICE');
-- 4 base values; migration 004 adds API_BOUNCE, API_COMPLAINT, API_UNSUBSCRIBE.
-- create_type=False in the ORM — this baseline owns the initial creation.
CREATE TYPE suppression_reason AS ENUM ('UNSUBSCRIBE', 'BOUNCE', 'COMPLAINT', 'MANUAL');
-- reply_intent is NOT created here — migration 007 owns it.

-- ── Base tables (NOT idempotency_records — migration 006 owns it) ──────────

CREATE TABLE tenants (
    name VARCHAR(255) NOT NULL,
    api_key VARCHAR(255) NOT NULL,
    id VARCHAR NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    CONSTRAINT pk_tenants PRIMARY KEY (id),
    CONSTRAINT uq_tenants_api_key UNIQUE (api_key)
);

CREATE TABLE mailboxes (
    tenant_id VARCHAR NOT NULL,
    email VARCHAR(255) NOT NULL,
    display_name VARCHAR(255),
    status mailboxstatus NOT NULL,
    weight INTEGER NOT NULL,
    daily_send_limit INTEGER NOT NULL,
    sent_today INTEGER NOT NULL,
    -- transport column + ck_mailbox_transport: migration 002 owns these.
    id VARCHAR NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    CONSTRAINT pk_mailboxes PRIMARY KEY (id),
    CONSTRAINT uq_mailbox_tenant_email UNIQUE (tenant_id, email),
    CONSTRAINT fk_mailboxes_tenant_id_tenants FOREIGN KEY(tenant_id) REFERENCES tenants (id)
);
-- uq_mailbox_email: migration 001 owns this.

CREATE TABLE sequences (
    tenant_id VARCHAR NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    status sequencestatus NOT NULL,
    id VARCHAR NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    CONSTRAINT pk_sequences PRIMARY KEY (id),
    CONSTRAINT fk_sequences_tenant_id_tenants FOREIGN KEY(tenant_id) REFERENCES tenants (id)
);

CREATE TABLE sequence_steps (
    sequence_id VARCHAR NOT NULL,
    step_number INTEGER NOT NULL,
    subject VARCHAR(500) NOT NULL,
    body TEXT NOT NULL,
    delay_days INTEGER NOT NULL,
    delay_hours INTEGER NOT NULL,
    id VARCHAR NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    CONSTRAINT pk_sequence_steps PRIMARY KEY (id),
    CONSTRAINT uq_sequence_step_number UNIQUE (sequence_id, step_number),
    CONSTRAINT fk_sequence_steps_sequence_id_sequences FOREIGN KEY(sequence_id) REFERENCES sequences (id)
);

CREATE TABLE sequence_enrollments (
    sequence_id VARCHAR NOT NULL,
    mailbox_id VARCHAR NOT NULL,
    contact_email VARCHAR(255) NOT NULL,
    contact_name VARCHAR(255),
    timezone VARCHAR(63) DEFAULT 'America/New_York' NOT NULL,
    status enrollmentstatus NOT NULL,
    current_step INTEGER NOT NULL,
    pause_reason VARCHAR(255),
    -- external_ref column: migration 001 owns it.
    -- ck_enrollment_pause_reason CHECK: migration 001 owns it.
    id VARCHAR NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    CONSTRAINT pk_sequence_enrollments PRIMARY KEY (id),
    CONSTRAINT uq_enrollment_sequence_contact UNIQUE (sequence_id, contact_email),
    CONSTRAINT fk_sequence_enrollments_sequence_id_sequences FOREIGN KEY(sequence_id) REFERENCES sequences (id),
    CONSTRAINT fk_sequence_enrollments_mailbox_id_mailboxes FOREIGN KEY(mailbox_id) REFERENCES mailboxes (id)
);

CREATE TABLE sequence_enrollment_steps (
    enrollment_id VARCHAR NOT NULL,
    step_id VARCHAR NOT NULL,
    mailbox_id VARCHAR,
    status enrollmentstepstatus NOT NULL,
    scheduled_at TIMESTAMP WITHOUT TIME ZONE,
    sent_at TIMESTAMP WITHOUT TIME ZONE,
    custom_subject VARCHAR(500),
    custom_body TEXT,
    id VARCHAR NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    CONSTRAINT pk_sequence_enrollment_steps PRIMARY KEY (id),
    CONSTRAINT fk_sequence_enrollment_steps_enrollment_id_sequence_enrollments FOREIGN KEY(enrollment_id) REFERENCES sequence_enrollments (id),
    CONSTRAINT fk_sequence_enrollment_steps_step_id_sequence_steps FOREIGN KEY(step_id) REFERENCES sequence_steps (id),
    CONSTRAINT fk_sequence_enrollment_steps_mailbox_id_mailboxes FOREIGN KEY(mailbox_id) REFERENCES mailboxes (id)
);

CREATE TABLE sent_emails (
    message_id VARCHAR(255) NOT NULL,
    thread_id VARCHAR(255),
    mailbox_id VARCHAR NOT NULL,
    enrollment_step_id VARCHAR NOT NULL,
    subject VARCHAR(500) NOT NULL,
    body TEXT NOT NULL,
    to_email VARCHAR(255) NOT NULL,
    to_name VARCHAR(255),
    from_email VARCHAR(255) NOT NULL,
    from_name VARCHAR(255),
    sent_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    -- delivered_at column: migration 005 owns it.
    id VARCHAR NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    CONSTRAINT pk_sent_emails PRIMARY KEY (id),
    CONSTRAINT uq_sent_emails_message_id UNIQUE (message_id),
    CONSTRAINT fk_sent_emails_mailbox_id_mailboxes FOREIGN KEY(mailbox_id) REFERENCES mailboxes (id),
    CONSTRAINT fk_sent_emails_enrollment_step_id_sequence_enrollment_steps FOREIGN KEY(enrollment_step_id) REFERENCES sequence_enrollment_steps (id)
);

CREATE TABLE signals (
    sent_email_id VARCHAR NOT NULL,
    type signaltype NOT NULL,
    -- reply_intent column: migration 007 owns it.
    detected_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    raw_data TEXT,
    id VARCHAR NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    CONSTRAINT pk_signals PRIMARY KEY (id),
    CONSTRAINT fk_signals_sent_email_id_sent_emails FOREIGN KEY(sent_email_id) REFERENCES sent_emails (id)
);

CREATE TABLE suppressions (
    tenant_id VARCHAR NOT NULL,
    email VARCHAR(255) NOT NULL,
    domain VARCHAR(255),
    reason suppression_reason NOT NULL,
    source_enrollment_id VARCHAR,
    notes TEXT,
    id VARCHAR NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    CONSTRAINT pk_suppressions PRIMARY KEY (id),
    CONSTRAINT uq_suppression_tenant_email UNIQUE (tenant_id, email),
    CONSTRAINT fk_suppressions_tenant_id_tenants FOREIGN KEY(tenant_id) REFERENCES tenants (id),
    CONSTRAINT fk_suppressions_source_enrollment_id_sequence_enrollments FOREIGN KEY(source_enrollment_id) REFERENCES sequence_enrollments (id)
);
-- uq_suppression_email: migration 001 owns this.

CREATE TABLE webhook_configs (
    tenant_id VARCHAR NOT NULL,
    url VARCHAR(500) NOT NULL,
    secret VARCHAR(255) NOT NULL,
    events TEXT NOT NULL,
    enabled BOOLEAN NOT NULL,
    id VARCHAR NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    CONSTRAINT pk_webhook_configs PRIMARY KEY (id),
    CONSTRAINT fk_webhook_configs_tenant_id_tenants FOREIGN KEY(tenant_id) REFERENCES tenants (id)
);

CREATE TABLE webhook_deliveries (
    config_id VARCHAR NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload TEXT NOT NULL,
    status VARCHAR(50) NOT NULL,
    attempts INTEGER NOT NULL,
    last_attempt_at TIMESTAMP WITHOUT TIME ZONE,
    response_status INTEGER,
    response_body TEXT,
    id VARCHAR NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    CONSTRAINT pk_webhook_deliveries PRIMARY KEY (id),
    CONSTRAINT fk_webhook_deliveries_config_id_webhook_configs FOREIGN KEY(config_id) REFERENCES webhook_configs (id)
);

CREATE TABLE processed_email_events (
    event_type VARCHAR(100) NOT NULL,
    processed_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    id VARCHAR NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    CONSTRAINT pk_processed_email_events PRIMARY KEY (id)
);

-- Create indexes for ORM index=True columns (NOT external_ref — migration 001 owns it).
CREATE INDEX ix_suppressions_email ON suppressions (email);
CREATE INDEX ix_suppressions_domain ON suppressions (domain);
