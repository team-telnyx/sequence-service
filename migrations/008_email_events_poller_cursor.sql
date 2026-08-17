-- REVOPS-1525 migration 008 — cursor persistence for the Email API events pull-poller.
--
-- The Ed25519 webhook receiver (PR #30) is live but Telnyx cannot reach the
-- host (no public ingress). This pivot PULLS delivery events from
-- GET /v2/email/events on a ~2-minute interval and replays them through the
-- SAME process_email_event the receiver uses. To resume where the last poll
-- left off, the poller persists the last-processed page_cursor here.
--
-- One row per feed (PK = feed name, currently just "email_events"). Stores
-- the last-processed page_cursor from the API's meta.page_cursor. On
-- empty/missing cursor, the poller starts from the first page; the
-- processed_email_events dedupe markers make the backfill safe (re-pulled
-- events are no-ops).
--
-- The cursor advances ONLY after every event on a page is processed
-- successfully — a failed page leaves the cursor untouched so the next run
-- re-fetches and retries.
--
-- The ORM (src/models/models.py EmailEventsPollerCursor) mirrors this table
-- so create_all test DBs and the live schema agree. Idempotent: IF NOT EXISTS
-- means re-running is a no-op. Style matches 005 (ADD COLUMN/TABLE IF NOT
-- EXISTS, GUARD, one transaction).

BEGIN;

-- GUARD: the base schema exists (defensive — matches 004/005 style).
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables WHERE table_name = 'tenants'
  ) THEN
    RAISE EXCEPTION
      'base schema does not exist — run baseline migration first';
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS email_events_poller_cursor (
  id VARCHAR PRIMARY KEY,
  last_cursor VARCHAR(2000),
  created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
  updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
);

COMMIT;
