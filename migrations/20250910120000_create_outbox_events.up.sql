CREATE TABLE outbox_events (
    id BIGSERIAL PRIMARY KEY,
    data BYTEA NOT NULL,
    attributes JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    topic TEXT NOT NULL,
    retry_count INT NOT NULL DEFAULT 0,
    retry_limit INT NOT NULL DEFAULT 5,
    ordering_key TEXT NOT NULL,
    event_type TEXT NOT NULL,
    last_attempted_at TIMESTAMPTZ DEFAULT NULL,
    -- Producer-side dedup against transactional retries: identical (topic,
    -- event_type, ordering_key) triples collapse to a single row. Useful for
    -- emission patterns where each (aggregate, state-transition) pair maps
    -- to exactly one event. Services that legitimately emit multiple events
    -- with the same key can drop this constraint via a service-local
    -- migration.
    CONSTRAINT uq_outbox_topic_event_ordering UNIQUE (topic, event_type, ordering_key)
);
