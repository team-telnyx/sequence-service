-- SV2-044: Versioned enrollment contract + idempotency records + reply intent.
-- Matches docs/10 §idempotency_records schema AND the ORM (src/models/models.py
-- IdempotencyRecord) exactly. The r2 migration omitted the inherited ``id``
-- and ``updated_at`` columns, so a real PostgreSQL deploy that applied ONLY
-- the migrations (no Base.metadata.create_all) raised
-- ``UndefinedColumnError: column "id" does not exist`` on the first ORM insert.
-- r3 fixes the drift at the source: the migration is the schema-of-record for
-- production, so it must agree with the ORM column-for-column.
--
-- SV2-044 r6 (FAIL 1): the r3 migration used TEXT for scope/idempotency_key/
-- request_sha256/status/id while the ORM declares VARCHAR(n) with explicit
-- lengths. PostgreSQL accepts over-length values in TEXT silently, so the
-- drift masked a real production risk (a value longer than the ORM's declared
-- limit would be stored fine but rejected on ORM read-back). r6 aligns the
-- migration column types to the ORM exactly: VARCHAR(100/500/64/50/36). The
-- result column stays JSONB (the production type) — the ORM is aligned to
-- JSONB in models.py so both sides agree.

CREATE TABLE IF NOT EXISTS idempotency_records (
    id              VARCHAR(36)  NOT NULL DEFAULT gen_random_uuid()::varchar(36),
    scope           VARCHAR(100) NOT NULL,
    idempotency_key VARCHAR(500) NOT NULL,
    request_sha256  VARCHAR(64)  NOT NULL,
    status          VARCHAR(50)  NOT NULL,
    result          JSONB,
    created_at      TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    updated_at      TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    completed_at    TIMESTAMP,
    PRIMARY KEY (scope, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_idempotency_request_sha256
    ON idempotency_records (scope, idempotency_key, request_sha256);
