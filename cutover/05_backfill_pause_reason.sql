-- REVOPS-972 cutover step 5 — BACKFILL pause_reason='reply' on the reply-paused
-- enrollments that signal_detection left NULL (audit C1 / B1). After step 4 the
-- 11 tenant-quinn rows are gone, leaving 100 tenant-scout rows. Only stamp rows
-- that actually carry a REPLY signal (guard against mislabeling a legacy NULL).

UPDATE sequence_enrollments e
SET pause_reason = 'reply', updated_at = now()
WHERE e.status = 'PAUSED'
  AND e.pause_reason IS NULL
  AND EXISTS (
    SELECT 1 FROM signals g
    JOIN sent_emails se ON se.id = g.sent_email_id
    JOIN sequence_enrollment_steps st ON st.id = se.enrollment_step_id
    WHERE st.enrollment_id = e.id AND g.type::text ILIKE '%repl%'
  );
