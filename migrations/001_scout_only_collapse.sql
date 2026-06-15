-- REVOPS-972 migration 001 — Scout-only schema hardening (DATA-collapse scope).
-- Runs AFTER the data steps (archive/repoint/migrate-suppressions/delete/backfill).
-- SCOPE (per decision 2026-06-15): add UNIQUE(email) + pause_reason CHECK + external_ref.
-- tenant_id columns + tenants table are RETAINED (now single-valued 'tenant-scout');
-- dropping them + single-secret auth is a separate validated follow-up (needs ORM rewrite).
-- Idempotent where practical. Wrapped in one transaction.

BEGIN;

-- GUARD 1: no non-scout rows remain (delete step must have run).
DO $$
BEGIN
  IF (SELECT count(*) FROM mailboxes    WHERE tenant_id <> 'tenant-scout') > 0
  OR (SELECT count(*) FROM sequences    WHERE tenant_id <> 'tenant-scout') > 0
  OR (SELECT count(*) FROM suppressions WHERE tenant_id <> 'tenant-scout') > 0
  OR (SELECT count(*) FROM tenants      WHERE id        <> 'tenant-scout') > 0
  THEN RAISE EXCEPTION 'non-scout data still present — run 04_delete_nonscout.sql first';
  END IF;
END $$;

-- GUARD 2: no duplicate emails (UNIQUE(email) would abort otherwise).
DO $$
BEGIN
  IF (SELECT count(*) FROM (SELECT email FROM mailboxes GROUP BY email HAVING count(*)>1) d) > 0
  THEN RAISE EXCEPTION 'duplicate mailbox emails remain'; END IF;
  IF (SELECT count(*) FROM (SELECT email FROM suppressions GROUP BY email HAVING count(*)>1) d) > 0
  THEN RAISE EXCEPTION 'duplicate suppression emails remain'; END IF;
END $$;

-- UNIQUE(email): the durable guard against the cross-tenant double-send leak.
-- Composite uq_*_tenant_email is retained (matches the unchanged ORM, now redundant
-- but harmless since email-unique implies tenant+email-unique).
ALTER TABLE mailboxes    ADD CONSTRAINT uq_mailbox_email     UNIQUE (email);
ALTER TABLE suppressions ADD CONSTRAINT uq_suppression_email UNIQUE (email);

-- pause_reason CHECK (matches WS-A ORM: nullable + enum; NOT NULL would break the
-- resume-to-NULL path in circuit_resume.py and the manual-resume endpoint).
ALTER TABLE sequence_enrollments DROP CONSTRAINT IF EXISTS ck_enrollment_pause_reason;
ALTER TABLE sequence_enrollments ADD CONSTRAINT ck_enrollment_pause_reason
  CHECK (pause_reason IS NULL OR pause_reason IN
         ('circuit_breaker','reply','unsubscribe','bounce','manual'));

-- external_ref (Scout prospect identity write-back; matches WS-A ORM).
ALTER TABLE sequence_enrollments ADD COLUMN IF NOT EXISTS external_ref VARCHAR(255);
CREATE INDEX IF NOT EXISTS ix_sequence_enrollments_external_ref
  ON sequence_enrollments (external_ref);

COMMIT;
