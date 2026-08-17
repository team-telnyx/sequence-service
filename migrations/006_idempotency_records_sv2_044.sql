-- SV2-044: Versioned enrollment contract + idempotency records + reply intent.
-- Matches docs/10 §idempotency_records schema AND the ORM (src/models/models.py
-- IdempotencyRecord) exactly. The r2 migration omitted the inherited ``id``
-- and ``updated_at`` columns, so a real PostgreSQL deploy that applied ONLY
-- the migrations (no Base.metadata.create_all) raised
-- ``UndefinedColumnError: column "id" does not exist`` on the first ORM insert.
-- r3 fixes the drift at the source: the migration is the schema-of-record for
-- production, so it must agree with the ORM column-for-column.

CREATE TABLE IF NOT EXISTS idempotency_records (
    id              TEXT      NOT NULL DEFAULT gen_random_uuid()::text,
    scope           TEXT      NOT NULL,
    idempotency_key TEXT      NOT NULL,
    request_sha256  TEXT      NOT NULL,
    status          TEXT      NOT NULL,
    result          JSONB,
    created_at      TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    updated_at      TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    completed_at    TIMESTAMP,
    PRIMARY KEY (scope, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_idempotency_request_sha256
    ON idempotency_records (scope, idempotency_key, request_sha256);
