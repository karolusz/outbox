# Outbox table schema

The schema is the contract between any producer (Go, Python, TS, anything that talks to Postgres) and the relay. This document describes what's there, who populates what, and the minimum Postgres version.

**Minimum Postgres version: 18.** The schema uses the built-in `uuidv7()` function (introduced in PG 18) as the default for `event_id`. Adopters on older PG must either upgrade or fork the migration to substitute a different UUID default.

## Migration

The library ships the schema as a single SQL file: [`migrations/20260616120000_initial.up.sql`](../../migrations/20260616120000_initial.up.sql). Apply it once with any migration runner of your choice — goose, golang-migrate, dbmate, sqitch, hand-rolled `psql`, anything that runs the SQL.

```sql
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

CREATE INDEX IF NOT EXISTS messages_pending_idx
    ON outbox.messages (id)
    WHERE retry_count < retry_limit;
```

The library's default behavior assumes the table lives at `outbox.messages`. The relay can be reconfigured to use a different schema name (e.g. via `relay.WithDBSchema` in Go or `--schema` flag on `cmd/outbox-relay`), but the table name `messages` is fixed.

## Column reference

| Column | Type | Populated by | Notes |
|---|---|---|---|
| `id` | `BIGSERIAL` | DB | Row primary key. Monotonically increasing, sequence-backed. Do NOT treat as a stable event identifier — use `event_id` for that. |
| `event_id` | `UUID NOT NULL` | producer or DB default | UUIDv7 recommended. Time-ordered ID for broker-side dedup and correlation. If left NULL or DEFAULT at insert, the DB generates one via `uuidv7()`. **Recommended: producer supplies UUIDv7 client-side** so the ID can be logged/traced before the INSERT commits. |
| `address` | `TEXT NOT NULL` | producer | The logical address the relay resolves via the address book to a (publisher, broker target) pair. E.g. `"payments.completed.v1"`. |
| `data` | `BYTEA NOT NULL` | producer | The payload. Opaque to the relay — encode CloudEvents, protobuf, JSON, or raw bytes as you prefer. See "Wire format" below for CloudEvents binding guidance. |
| `headers` | `JSONB NOT NULL DEFAULT '{}'` | producer | Key/value metadata that travels with the message to the broker (Pub/Sub attributes, Kafka headers, etc.). |
| `ordering_key` | `TEXT NOT NULL DEFAULT ''` | producer | Broker-level ordering hint. Maps to Pub/Sub `OrderingKey`, Kafka partition key, NATS subject hash, etc. Empty string means no hint. |
| `retry_count` | `INT NOT NULL DEFAULT 0` | relay | Incremented on each failed publish attempt. The relay filters rows out of polling once `retry_count >= retry_limit`. Producers SHOULD NOT touch this. |
| `retry_limit` | `INT NOT NULL DEFAULT 5` | producer | Per-row cap on retry attempts. Override per event if some events tolerate more retries than others. |
| `created_at` | `TIMESTAMPTZ NOT NULL DEFAULT NOW()` | DB | Set by the database at INSERT time. Producers leave this alone. |
| `last_attempted_at` | `TIMESTAMPTZ` | relay | Set when the relay attempts a publish. Used for backoff (the relay throttles re-attempts within a leeway window). NULL means never attempted yet. |

## Permissions

Two roles is the recommended setup: the application user (which the producer runs as) and the relay user (which the relay runs as). Template:

```sql
-- See migrations/grants.sql.template for the canonical version.
GRANT USAGE ON SCHEMA outbox TO {{app_role}};
GRANT INSERT ON outbox.messages TO {{app_role}};
GRANT USAGE, SELECT ON SEQUENCE outbox.messages_id_seq TO {{app_role}};

GRANT USAGE ON SCHEMA outbox TO {{relay_role}};
GRANT SELECT, UPDATE, DELETE ON outbox.messages TO {{relay_role}};
GRANT USAGE, SELECT ON SEQUENCE outbox.messages_id_seq TO {{relay_role}};
```

Adopters running the app and relay under the same DB role can collapse the two blocks into one.

The relay never needs access to the adopter's domain tables. Broker credentials are mounted into the relay's container only — the application pod does not see them.

## Cross-schema transactions

The transactional outbox pattern requires the INSERT into `outbox.messages` to commit atomically with the producer's domain writes. Postgres transactions are database-scoped, not schema-scoped — a single transaction can touch tables in any number of schemas the user has permissions on. So:

```sql
BEGIN;
  UPDATE public.payments SET status = 'completed' WHERE id = $1;
  INSERT INTO outbox.messages (event_id, address, data, ...) VALUES (...);
COMMIT;
-- Both statements either land or neither does.
```

This works identically to a single-schema setup. The dedicated `outbox` schema is a packaging/permissions improvement, not a semantic change.

## Wire format

The `data` column is opaque. Adopters choose the wire format. We recommend (but do not require) CloudEvents — see [`address-book.md`](./address-book.md) for the two binding modes (structured and binary) and how they map to our `data` and `headers` columns.

## Adopter-side extensions

The relay's SELECT uses an explicit column list, so adopters MAY add columns to `outbox.messages` for their own purposes (tenancy, correlation IDs, custom indexes). The relay simply ignores any column it doesn't know.

Adding NOT NULL columns without defaults will break the relay's INSERT path (it doesn't supply them). For optional adopter-side columns, give them defaults or make them nullable.

## What's NOT in the schema

These are deliberate v1 design choices, documented to avoid confusion:

- **No `event_type` column.** Adopters who want event-type metadata in the broker put it in `headers` (where it flows through). v0 had a dedicated column but the relay never used it.
- **No `state` enum / `published_at` column.** The relay deletes rows immediately on broker ack rather than marking them. This may change in a future version if audit visibility is a real adopter need; today it isn't.
- **No UNIQUE constraint on `event_id`.** Producers MAY supply the same `event_id` twice (e.g. across application retries) and both rows are inserted. Consumer-side dedup against `event_id` is the responsibility of the downstream consumer, not the DB.
- **No FK to producer domain tables.** The lib's schema doesn't know what tables exist in the adopter's domain.
