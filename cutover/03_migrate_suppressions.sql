-- REVOPS-972 cutover step 3 — PRESERVE Quinn opt-outs (CAN-SPAM, mandatory).
-- Copy tenant-quinn suppressions into tenant-scout before deleting tenant-quinn.
-- id has no DB default -> generate a deterministic 'migq-' id. source_enrollment_id
-- is nulled (it may point at a tenant-quinn enrollment being deleted). Dupes on
-- (tenant_id,email) are skipped (2 already exist under tenant-scout; 139 are new).

INSERT INTO suppressions
  (id, tenant_id, email, domain, reason, source_enrollment_id, notes, created_at, updated_at)
SELECT
  'migq-' || q.id,
  'tenant-scout',
  q.email,
  q.domain,
  q.reason,
  NULL,
  COALESCE(q.notes, '') || ' [migrated from tenant-quinn ' || q.id || ' @REVOPS-972]',
  q.created_at,
  now()
FROM suppressions q
WHERE q.tenant_id = 'tenant-quinn'
ON CONFLICT (tenant_id, email) DO NOTHING;
