-- REVOPS-1552 migration 002 — per-mailbox transport selection.
--
-- Adds the `transport` column to mailboxes: 'gmail' (default, byte-identical
-- to today) or 'email_api' (Telnyx Email API). NOT NULL with DEFAULT 'gmail'
-- so every existing mailbox backfills to the Gmail path with NO behavior
-- change — cutover happens per-mailbox via an explicit UPDATE, not via deploy.
-- CHECK-constrained to the two known values so a typo can never silently fall
-- through to the default branch.
--
-- Style matches 001_scout_only_collapse.sql: idempotent where practical,
-- wrapped in one transaction.
--
-- The ORM (src/models/models.py Mailbox.transport) mirrors this column so
-- create_all test DBs and the live schema agree; the CHECK constraint is
-- declared in both places (model __table_args__ + this migration) so a
-- create_all test DB enforces the same invariant as the live DB.

BEGIN;

-- GUARD: mailboxes table exists (defensive — 001 assumed it).
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.tables WHERE table_name = 'mailboxes'
  ) THEN
    RAISE EXCEPTION 'mailboxes table does not exist — run base schema first';
  END IF;
END $$;

-- transport: 'gmail' (default) | 'email_api'. NOT NULL DEFAULT 'gmail'
-- backfills every existing row to the Gmail path (byte-identical behavior).
-- Postgres 11+ adds a NOT NULL column with a DEFAULT instantly without a
-- full table rewrite, so this is safe on the live DB.
ALTER TABLE mailboxes
  ADD COLUMN IF NOT EXISTS transport VARCHAR(50) NOT NULL DEFAULT 'gmail';

-- CHECK-constrain to known values so a typo can't silently hit the default
-- branch. Drop-then-add so re-running the migration is idempotent.
ALTER TABLE mailboxes DROP CONSTRAINT IF EXISTS ck_mailbox_transport;
ALTER TABLE mailboxes ADD CONSTRAINT ck_mailbox_transport
  CHECK (transport IN ('gmail', 'email_api'));

COMMIT;
