-- REVOPS-1552 migration 005 — durable delivered_at marker on SentEmail.
--
-- Adds a nullable delivered_at column to sent_emails. Set by the Email API
-- webhook receiver (src/services/email_events.py) when a 'delivered' event
-- is processed. Nullable because every existing row predates the column
-- (backfill is not meaningful — we only mark deliveries going forward).
--
-- The ORM (src/models/models.py SentEmail.delivered_at) mirrors this column
-- so create_all test DBs and the live schema agree. Idempotent: IF NOT EXISTS
-- means re-running is a no-op. Style matches 002 (ADD COLUMN IF NOT EXISTS,
-- GUARD, one transaction).
--
-- Delivered events do NOT touch suppression or enrollment status — this
-- column is the only durable side-effect of a delivered webhook (F4).

BEGIN;

-- GUARD: sent_emails table exists (defensive — matches 002/003 style).
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables WHERE table_name = 'sent_emails'
  ) THEN
    RAISE EXCEPTION
      'sent_emails table does not exist — run base schema first';
  END IF;
END $$;

ALTER TABLE sent_emails
  ADD COLUMN IF NOT EXISTS delivered_at TIMESTAMP WITHOUT TIME ZONE;

COMMIT;
