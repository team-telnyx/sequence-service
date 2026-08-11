-- REVOPS-1552 migration 003 — durable terminal FAILED status for exhausted
-- retries.
--
-- r4 finding: when ARQ retries are exhausted the handler converted the final
-- attempt to a durable terminal failure (row -> FAILED) instead of raising
-- ArqRetry, so the reconciler can no longer resurrect a dead job. The new
-- status value must exist in the Postgres enum before the ORM can write it.
--
-- The ORM (src/models/models.py EnrollmentStepStatus) mirrors this value so
-- create_all test DBs and the live schema agree. Idempotent: IF NOT EXISTS
-- means re-running is a no-op.
--
-- NOTE: ALTER TYPE ... ADD VALUE IF NOT EXISTS can run inside a transaction
-- block on Postgres 12+ (the repo runs 15). Kept in BEGIN/COMMIT to match
-- the style of 001/002. The non-IF-NOT-EXISTS form CANNOT run in a
-- transaction — always use IF NOT EXISTS here.

BEGIN;

-- GUARD: the enum type exists (created by the base schema via create_all).
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_type WHERE typname = 'enrollmentstepstatus'
  ) THEN
    RAISE EXCEPTION
      'enum type enrollmentstepstatus does not exist — run base schema first';
  END IF;
END $$;

-- Add the FAILED value. SQLAlchemy's Enum(EnrollmentStepStatus) names the
-- Postgres type after the enum class lowercased: enrollmentstepstatus.
ALTER TYPE enrollmentstepstatus ADD VALUE IF NOT EXISTS 'FAILED';

COMMIT;
