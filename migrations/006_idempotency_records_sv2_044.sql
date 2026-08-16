-- SV2-044: Versioned enrollment contract + idempotency records + reply intent.
-- Matches docs/10 §idempotency_records schema.

CREATE TABLE IF NOT EXISTS idempotency_records (
    scope          TEXT      NOT NULL,
    idempotency_key TEXT     NOT NULL,
    request_sha256 TEXT      NOT NULL,
    status         TEXT      NOT NULL,
    result         JSONB,
    created_at     TIMESTAMP NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    completed_at   TIMESTAMP,
    PRIMARY KEY (scope, idempotency_key)
);

-- Index for digest lookup on conflict (same key, different digest → 409).
CREATE INDEX IF NOT EXISTS idx_idempotency_request_sha256
    ON idempotency_records (scope, idempotency_key, request_sha256);
