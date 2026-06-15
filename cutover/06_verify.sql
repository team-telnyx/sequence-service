-- REVOPS-972 cutover step 6 — VERIFY. Raises EXCEPTION on any failed assertion
-- (run with ON_ERROR_STOP=1). Reports key counts via NOTICE.

DO $$
DECLARE n int; mx int;
BEGIN
  -- 1. no non-scout rows remain
  SELECT count(*) INTO n FROM (
    SELECT 1 FROM mailboxes    WHERE tenant_id<>'tenant-scout'
    UNION ALL SELECT 1 FROM sequences    WHERE tenant_id<>'tenant-scout'
    UNION ALL SELECT 1 FROM suppressions WHERE tenant_id<>'tenant-scout'
    UNION ALL SELECT 1 FROM tenants      WHERE id<>'tenant-scout') q;
  IF n>0 THEN RAISE EXCEPTION 'FAIL: % non-scout rows remain', n; END IF;

  -- 2. no duplicate emails
  IF (SELECT count(*) FROM (SELECT email FROM mailboxes GROUP BY email HAVING count(*)>1) d)>0
    THEN RAISE EXCEPTION 'FAIL: duplicate mailbox emails'; END IF;
  IF (SELECT count(*) FROM (SELECT email FROM suppressions GROUP BY email HAVING count(*)>1) d)>0
    THEN RAISE EXCEPTION 'FAIL: duplicate suppression emails'; END IF;

  -- 3. UNIQUE(email) constraints present
  IF (SELECT count(*) FROM pg_constraint WHERE conname IN ('uq_mailbox_email','uq_suppression_email'))<>2
    THEN RAISE EXCEPTION 'FAIL: UNIQUE(email) constraints missing'; END IF;

  -- 4. pause_reason CHECK present
  IF (SELECT count(*) FROM pg_constraint WHERE conname='ck_enrollment_pause_reason')<>1
    THEN RAISE EXCEPTION 'FAIL: ck_enrollment_pause_reason missing'; END IF;

  -- 5. external_ref column present
  IF (SELECT count(*) FROM information_schema.columns
      WHERE table_name='sequence_enrollments' AND column_name='external_ref')<>1
    THEN RAISE EXCEPTION 'FAIL: external_ref column missing'; END IF;

  -- 6. backfill complete: no PAUSED+NULL with a reply signal
  SELECT count(*) INTO n FROM sequence_enrollments e
   WHERE e.status='PAUSED' AND e.pause_reason IS NULL
     AND EXISTS (SELECT 1 FROM signals g JOIN sent_emails se ON se.id=g.sent_email_id
                 JOIN sequence_enrollment_steps st ON st.id=se.enrollment_step_id
                 WHERE st.enrollment_id=e.id AND g.type::text ILIKE '%repl%');
  IF n>0 THEN RAISE EXCEPTION 'FAIL: % reply-paused rows still NULL', n; END IF;

  -- 7. HARD RULE: no mailbox cap exceeds 75 (never raised/bypassed)
  SELECT max(daily_send_limit) INTO mx FROM mailboxes;
  IF mx>75 THEN RAISE EXCEPTION 'FAIL: a mailbox daily_send_limit=% exceeds 75', mx; END IF;

  -- 8. no orphaned mailbox references (leak repointed cleanly)
  SELECT count(*) INTO n FROM sequence_enrollments e
   LEFT JOIN mailboxes m ON m.id=e.mailbox_id WHERE e.mailbox_id IS NOT NULL AND m.id IS NULL;
  IF n>0 THEN RAISE EXCEPTION 'FAIL: % enrollments reference a missing mailbox', n; END IF;

  RAISE NOTICE 'ALL VERIFY ASSERTIONS PASSED';
END $$;

\echo '--- post-cutover state ---'
SELECT 'mailboxes' t, count(*), max(daily_send_limit) max_cap FROM mailboxes
UNION ALL SELECT 'suppressions(scout)', count(*), NULL FROM suppressions
UNION ALL SELECT 'tenants', count(*), NULL FROM tenants
UNION ALL SELECT 'paused_reply', count(*), NULL FROM sequence_enrollments WHERE pause_reason='reply'
UNION ALL SELECT 'archive_quinn.mailboxes', count(*), NULL FROM archive_quinn.mailboxes
UNION ALL SELECT 'archive_quinn.suppressions', count(*), NULL FROM archive_quinn.suppressions;
