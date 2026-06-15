-- REVOPS-972 cutover step 2 — REPOINT the cross-tenant leak.
-- 3 scout enrollments + 3 scout sent_emails reference tenant-quinn mailbox ROWS
-- (the 2026-06-12 leak). Repoint any SCOUT-owned row off a non-scout mailbox onto
-- the scout mailbox with the same email, so the non-scout mailbox rows can be
-- deleted without dangling FKs. Scoped strictly to scout-owned rows (non-scout
-- rows are deleted in step 4 anyway).

-- enrollments
UPDATE sequence_enrollments e
SET mailbox_id = sc.id
FROM mailboxes q, mailboxes sc, sequences s
WHERE e.mailbox_id = q.id
  AND q.tenant_id <> 'tenant-scout'
  AND sc.email = q.email AND sc.tenant_id = 'tenant-scout'
  AND s.id = e.sequence_id AND s.tenant_id = 'tenant-scout';

-- enrollment steps
UPDATE sequence_enrollment_steps st
SET mailbox_id = sc.id
FROM mailboxes q, mailboxes sc, sequence_enrollments e, sequences s
WHERE st.mailbox_id = q.id
  AND q.tenant_id <> 'tenant-scout'
  AND sc.email = q.email AND sc.tenant_id = 'tenant-scout'
  AND e.id = st.enrollment_id AND s.id = e.sequence_id AND s.tenant_id = 'tenant-scout';

-- sent emails
UPDATE sent_emails se
SET mailbox_id = sc.id
FROM mailboxes q, mailboxes sc, sequence_enrollment_steps st, sequence_enrollments e, sequences s
WHERE se.mailbox_id = q.id
  AND q.tenant_id <> 'tenant-scout'
  AND sc.email = q.email AND sc.tenant_id = 'tenant-scout'
  AND st.id = se.enrollment_step_id AND e.id = st.enrollment_id
  AND s.id = e.sequence_id AND s.tenant_id = 'tenant-scout';
