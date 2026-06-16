# outbox

A transactional outbox for Postgres. Run the relay as a sidecar to any service that talks to Postgres — Python, TypeScript, Go, anything.

**Status**: v0.x. API and schema are stabilising for v1; expect breaking changes between v0.x releases.

## What it does

Persists events in the same transaction as your domain writes, then a separate relay process publishes them at-least-once to a broker. Consumers must be idempotent.

The pattern: [Transactional Outbox](https://microservices.io/patterns/data/transactional-outbox.html).

The library ships two things:

1. **The relay** — a Go binary that polls the outbox table and publishes rows to a broker via a pluggable publisher (GCP Pub/Sub today; adopters can author their own). This is the headline artifact. Run it as a sidecar to your service.
2. **A Go producer SDK** (`outbox.Send`, etc.) — a convenience layer for services written in Go. Polyglot adopters don't need it; the producer-side integration is a 20-line raw SQL helper in any language.

Polyglot adopters interact with two things and only two things:

- **The table schema** ([`docs/protocol/schema.md`](./docs/protocol/schema.md)) — what columns exist, what the relay sets, what the producer sets.
- **The address book YAML** ([`docs/protocol/address-book.md`](./docs/protocol/address-book.md)) — how the relay maps logical addresses to broker targets.

## Requirements

- **Postgres 18+**. The schema uses the built-in `uuidv7()` function. Older PG versions need a fork of the migration.
- **Go 1.26+** (for building the relay binary or using the Go SDK).

## Quick start: any service, any language

### 1. Apply the schema migration

[`migrations/20260616120000_initial.up.sql`](./migrations/20260616120000_initial.up.sql) — apply it once to your Postgres with any migration runner. Creates the `outbox` schema and the `outbox.messages` table.

### 2. Write to the outbox table inside your domain transaction

Reference INSERT: [`docs/protocol/insert.sql`](./docs/protocol/insert.sql). Six bind parameters: `event_id`, `address`, `data`, `headers`, `ordering_key`, `retry_limit`.

#### Python (psycopg)

Working example: [`docs/protocol/examples/python/insert.py`](./docs/protocol/examples/python/insert.py). The integration is:

```python
INSERT_SQL = """
INSERT INTO outbox.messages (event_id, address, data, headers, ordering_key, retry_limit)
VALUES (%s, %s, %s, %s, %s, %s)
"""

with conn.transaction():
    # Your domain write inside the same tx:
    conn.execute("UPDATE payments SET status = 'completed' WHERE id = %s", (payment_id,))

    # Outbox row:
    conn.execute(INSERT_SQL, (
        str(uuid7()),                                      # event_id (UUIDv7)
        "payments.completed.v1",                           # address
        payload_bytes,                                     # data
        json.dumps({"content-type": "application/json"}),  # headers
        payment_id,                                        # ordering_key
        5,                                                 # retry_limit
    ))
```

#### TypeScript (node-postgres)

Working example: [`docs/protocol/examples/typescript/insert.ts`](./docs/protocol/examples/typescript/insert.ts). Same shape, TS syntax.

#### Go (with the SDK)

```go
import (
    "github.com/karolusz/outbox"
    "github.com/karolusz/outbox/outboxsqlx"
    "github.com/karolusz/outbox/publisher"
)

err := outboxsqlx.Send(ctx, tx, outbox.Message{
    Address:     "payments.completed.v1",
    Data:        payload,
    Headers:     publisher.JSONBMap{"content-type": "application/json"},
    OrderingKey: paymentID.String(),
    RetryLimit:  5,
})
```

The SDK fills in `EventID` with a fresh UUIDv7 if you don't supply one.

### 3. Run the relay as a sidecar

The lib ships a reference binary: [`cmd/outbox-relay`](./cmd/outbox-relay).

```sh
# Docker:
docker build -t outbox-relay:dev .
docker run --rm \
  -e DB_CONNECTION_STRING="postgres://outbox:outbox@db:5432/outbox" \
  -v $(pwd)/addressbook.yaml:/etc/outbox/addressbook.yaml \
  outbox-relay:dev

# Or built directly:
go install github.com/karolusz/outbox/cmd/outbox-relay@latest
DB_CONNECTION_STRING=postgres://... outbox-relay --addressbook=addressbook.yaml
```

Flags:

- `--addressbook` — path to the YAML address book (default `/etc/outbox/addressbook.yaml`).
- `--db-env` — env var name holding the connection string (default `DB_CONNECTION_STRING`).
- `--schema` — Postgres schema (default `outbox`).
- `--log-level` — `trace|debug|info|warn|error` (default `info`).

The binary handles `SIGINT` and `SIGTERM` cleanly: stops accepting new claims, finishes in-flight publishes, exits.

### 4. Write your address book

Minimal example:

```yaml
version: 1

publishers:
  - name: pubsub-prod
    plugin: gcppubsub
    config:
      project: my-gcp-project

addresses:
  - name: payments.completed.v1
    publisher: pubsub-prod
    target: payments-prod-topic
```

Full spec: [`docs/protocol/address-book.md`](./docs/protocol/address-book.md).

## Custom plugins

A polyglot adopter who needs a broker we don't ship (Kafka, NATS, internal bus) writes a small Go module implementing `publisher.Publisher`, then builds their own relay binary that blank-imports it:

```go
package main

import (
    _ "github.com/karolusz/outbox/publisher/gcppubsub"
    _ "company.com/internal/outbox-kafka-plugin"  // custom plugin

    // ... rest of the standard relay main
)
```

See [`cmd/outbox-relay/main.go`](./cmd/outbox-relay/main.go) for the reference main to fork.

## Go SDK package layout

For Go adopters who use the SDK, the library is intentionally split so producers pull in only the dependencies they actually use:

| Package | Use it for | Third-party deps it adds |
|---|---|---|
| `github.com/karolusz/outbox` | Producer API (`Send`, `SendBatch`), `Message`, `AddressBook` value type | none — stdlib only |
| `github.com/karolusz/outbox/publisher` | `Publisher` interface, `Message`, plugin registry, `NewEventID()` | none — stdlib only |
| `github.com/karolusz/outbox/yamlconfig` | YAML loaders | `gopkg.in/yaml.v3` |
| `github.com/karolusz/outbox/relay` | Relay engine | `sqlx`, `zerolog`, `lib/pq` (transitive) |
| `github.com/karolusz/outbox/outboxsqlx` | `*sqlx.Tx` adapter | `sqlx` |
| `github.com/karolusz/outbox/publisher/gcppubsub` | GCP Pub/Sub plugin | GCP SDK |
| `github.com/karolusz/outbox/publisher/fake` | In-memory plugin for tests | none |

A producer that does `import "github.com/karolusz/outbox"` for `Send` inherits **zero third-party deps**. Verified by:

```sh
make check-producer-deps
```

## Address validation in producers (optional)

Producers (Go or otherwise) that want to reject unknown addresses at API boundaries — before the row even hits Postgres — load the address book in validate-only mode. The Go SDK has a helper:

```go
import "github.com/karolusz/outbox/yamlconfig"

book, _ := yamlconfig.LoadAddressBookValidateOnly("outbox.addressbook.yaml")
if err := book.Validate(addr); err != nil {
    return fmt.Errorf("unknown outbox address: %w", err)
}
```

Polyglot adopters parse the YAML themselves with their language's YAML library and check the address against the list. The format is small.

## What v0.x is and isn't

- `Publisher` interface is `Publish(ctx, target, *Message) + Close(ctx)`. No permanent-error discrimination yet — every error is retried until `retry_limit`.
- Polling-only. No `LISTEN/NOTIFY`, no CDC.
- Single-replica relay assumed. No leader election yet.
- Rows are deleted immediately on broker ack (no "published" state retention). May change in v0.4+ if audit visibility is a real adopter need.

Expect breaking changes between v0.x releases. v1.0.0 lands when the schema and YAML format have been validated by adopters in production.

## Running tests

The integration tests require a running Postgres with the lib's schema applied. The lib ships a `docker-compose.yml` and `Makefile`:

```sh
make test-up       # start a local PG 18 with the schema applied
make test          # run the full suite
make test-down     # stop the container (keeps the volume; fast restart later)
make test-clean    # stop and wipe the volume (fresh schema next time)
```

The compose stack uses port `5434` so it does not collide with other Postgres instances on the host. Override `DB_CONNECTION_STRING` for non-default setups.

Unit-only subset (no DB needed):

```sh
make test-unit
```

## License

MIT. See [LICENSE](./LICENSE).
