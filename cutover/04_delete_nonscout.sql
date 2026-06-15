-- REVOPS-972 cutover step 4 — DELETE all non-Scout data in FK order.
-- Non-scout = tenant-quinn + test-tenant. Run AFTER archive (1), repoint (2),
-- suppression migration (3). Children before parents.

-- 1. signals (-> sent_emails)
DELETE FROM signals g USING sent_emails se, sequence_enrollment_steps st,
  sequence_enrollments e, sequences s
WHERE g.sent_email_id = se.id AND se.enrollment_step_id = st.id
  AND st.enrollment_id = e.id AND e.sequence_id = s.id
  AND s.tenant_id IN ('tenant-quinn','test-tenant');

-- 2. sent_emails (-> mailboxes, enrollment_steps) — scoped by owning sequence
DELETE FROM sent_emails se USING sequence_enrollment_steps st,
  sequence_enrollments e, sequences s
WHERE se.enrollment_step_id = st.id AND st.enrollment_id = e.id
  AND e.sequence_id = s.id AND s.tenant_id IN ('tenant-quinn','test-tenant');

-- 3. sequence_enrollment_steps (-> enrollments)
DELETE FROM sequence_enrollment_steps st USING sequence_enrollments e, sequences s
WHERE st.enrollment_id = e.id AND e.sequence_id = s.id
  AND s.tenant_id IN ('tenant-quinn','test-tenant');

-- 4. suppressions (-> enrollments, tenants) — delete before enrollments
DELETE FROM suppressions WHERE tenant_id IN ('tenant-quinn','test-tenant');

-- 4b. Null any SURVIVING (scout) suppression that sources from a non-scout
-- enrollment about to be deleted (a second cross-tenant leak: scout suppression
-- created off a tenant-quinn enrollment). Otherwise the FK blocks step 5.
UPDATE suppressions sup SET source_enrollment_id = NULL
FROM sequence_enrollments e
JOIN sequences s ON s.id = e.sequence_id
WHERE sup.source_enrollment_id = e.id
  AND s.tenant_id IN ('tenant-quinn','test-tenant');

-- 5. sequence_enrollments (-> sequences, mailboxes)
DELETE FROM sequence_enrollments e USING sequences s
WHERE e.sequence_id = s.id AND s.tenant_id IN ('tenant-quinn','test-tenant');

-- 6. sequence_steps (-> sequences)
DELETE FROM sequence_steps st USING sequences s
WHERE st.sequence_id = s.id AND s.tenant_id IN ('tenant-quinn','test-tenant');

-- 7. sequences (-> tenants)
DELETE FROM sequences WHERE tenant_id IN ('tenant-quinn','test-tenant');

-- 8. webhook_deliveries (-> webhook_configs)
DELETE FROM webhook_deliveries d USING webhook_configs c
WHERE d.config_id = c.id AND c.tenant_id IN ('tenant-quinn','test-tenant');

-- 9. webhook_configs (-> tenants)
DELETE FROM webhook_configs WHERE tenant_id IN ('tenant-quinn','test-tenant');

-- 10. mailboxes (-> tenants) — incl. the duplicate quinn.c-j rows (now de-referenced)
DELETE FROM mailboxes WHERE tenant_id IN ('tenant-quinn','test-tenant');

-- 11. tenants rows (keep the table; only tenant-scout remains)
DELETE FROM tenants WHERE id IN ('tenant-quinn','test-tenant');
