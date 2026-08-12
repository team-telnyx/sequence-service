-- REVOPS-1552 migration 004 — origin-split suppression reasons for Email API
-- webhook events.
--
-- Adds three new values to the suppressionreason enum: API_BOUNCE,
-- API_COMPLAINT, API_UNSUBSCRIBE. These identify suppressions originating
-- from Telnyx Email API webhook events (bounce/complaint/one-click-unsubscribe),
-- distinct from the existing BOUNCE/COMPLAINT/UNSUBSCRIBE (reply-classification
-- driven via poll_replies) and MANUAL (operator/SFDC) values. The origin split
-- is one-way: API events write IN to the suppressions table; manual/SFDC rows
-- are never modified or written back out.
--
-- The ORM (src/models/models.py SuppressionReason) mirrors these values so
-- create_all test DBs and the live schema agree. Idempotent: IF NOT EXISTS
-- means re-running is a no-op.
--
-- NOTE: ALTER TYPE ... ADD VALUE IF NOT EXISTS can run inside a transaction
-- block on Postgres 12+ (the repo runs 15). Kept in BEGIN/COMMIT to match
-- the style of 003.

BEGIN;

-- GUARD: the enum type exists (created by the base schema via create_all).
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_type WHERE typname = 'suppressionreason'
  ) THEN
    RAISE EXCEPTION
      'enum type suppressionreason does not exist — run base schema first';
  END IF;
END $$;

ALTER TYPE suppressionreason ADD VALUE IF NOT EXISTS 'API_BOUNCE';
ALTER TYPE suppressionreason ADD VALUE IF NOT EXISTS 'API_COMPLAINT';
ALTER TYPE suppressionreason ADD VALUE IF NOT EXISTS 'API_UNSUBSCRIBE';

COMMIT;
