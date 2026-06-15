-- REVOPS-972 cutover step 1 — ARCHIVE all non-Scout-owned data (recoverable).
-- The full pg_dump snapshot is the authoritative rollback; archive_quinn is the
-- in-DB recoverable copy honoring the "archive then remove" decision.
-- Non-scout = tenant-quinn + test-tenant. Idempotent (drops+recreates the schema).

DROP SCHEMA IF EXISTS archive_quinn CASCADE;
CREATE SCHEMA archive_quinn;

CREATE TABLE archive_quinn.tenants AS
  SELECT * FROM tenants WHERE id IN ('tenant-quinn','test-tenant');
CREATE TABLE archive_quinn.mailboxes AS
  SELECT * FROM mailboxes WHERE tenant_id IN ('tenant-quinn','test-tenant');
CREATE TABLE archive_quinn.sequences AS
  SELECT * FROM sequences WHERE tenant_id IN ('tenant-quinn','test-tenant');
CREATE TABLE archive_quinn.sequence_steps AS
  SELECT st.* FROM sequence_steps st
  JOIN sequences s ON s.id = st.sequence_id
  WHERE s.tenant_id IN ('tenant-quinn','test-tenant');
CREATE TABLE archive_quinn.sequence_enrollments AS
  SELECT e.* FROM sequence_enrollments e
  JOIN sequences s ON s.id = e.sequence_id
  WHERE s.tenant_id IN ('tenant-quinn','test-tenant');
CREATE TABLE archive_quinn.sequence_enrollment_steps AS
  SELECT st.* FROM sequence_enrollment_steps st
  JOIN sequence_enrollments e ON e.id = st.enrollment_id
  JOIN sequences s ON s.id = e.sequence_id
  WHERE s.tenant_id IN ('tenant-quinn','test-tenant');
CREATE TABLE archive_quinn.sent_emails AS
  SELECT se.* FROM sent_emails se
  JOIN sequence_enrollment_steps st ON st.id = se.enrollment_step_id
  JOIN sequence_enrollments e ON e.id = st.enrollment_id
  JOIN sequences s ON s.id = e.sequence_id
  WHERE s.tenant_id IN ('tenant-quinn','test-tenant');
CREATE TABLE archive_quinn.signals AS
  SELECT g.* FROM signals g
  JOIN sent_emails se ON se.id = g.sent_email_id
  JOIN sequence_enrollment_steps st ON st.id = se.enrollment_step_id
  JOIN sequence_enrollments e ON e.id = st.enrollment_id
  JOIN sequences s ON s.id = e.sequence_id
  WHERE s.tenant_id IN ('tenant-quinn','test-tenant');
CREATE TABLE archive_quinn.suppressions AS
  SELECT * FROM suppressions WHERE tenant_id IN ('tenant-quinn','test-tenant');
CREATE TABLE archive_quinn.webhook_configs AS
  SELECT * FROM webhook_configs WHERE tenant_id IN ('tenant-quinn','test-tenant');
CREATE TABLE archive_quinn.webhook_deliveries AS
  SELECT d.* FROM webhook_deliveries d
  JOIN webhook_configs c ON c.id = d.config_id
  WHERE c.tenant_id IN ('tenant-quinn','test-tenant');
