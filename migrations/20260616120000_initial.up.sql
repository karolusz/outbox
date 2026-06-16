CREATE SCHEMA IF NOT EXISTS outbox;

CREATE TABLE outbox.messages (
    id                BIGSERIAL    PRIMARY KEY,
    event_id          UUID         NOT NULL DEFAULT uuidv7(),
    address           TEXT         NOT NULL,
    data              BYTEA        NOT NULL,
    headers           JSONB        NOT NULL DEFAULT '{}',
    ordering_key      TEXT         NOT NULL DEFAULT '',
    retry_count       INT          NOT NULL DEFAULT 0,
    retry_limit       INT          NOT NULL DEFAULT 5,
    created_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    last_attempted_at TIMESTAMPTZ
);

-- Partial index supporting the relay's claim query, which filters by
-- retry_count < retry_limit and (last_attempted_at IS NULL OR is older
-- than the leeway window). Indexing the pending subset keeps scans cheap
-- as published rows are deleted out of the table.
CREATE INDEX IF NOT EXISTS messages_pending_idx
    ON outbox.messages (id)
    WHERE retry_count < retry_limit;
