# outbox

A transactional outbox for Go services on Postgres.

**Status**: v0 — early extraction from a production fintech codebase. API will change.

## What it does

Persists events in the same transaction as your domain writes, then a separate relay process publishes them to a broker (currently GCP Pub/Sub). Delivery is at-least-once; consumers must be idempotent.

The pattern: [Transactional Outbox](https://microservices.io/patterns/data/transactional-outbox.html).

## Layout

```
outbox/              Core package. Producer API (SendMessage), relay engine (workers, polling, panic recovery), schema knowledge.
publisher/
  gcppubsub/         GCP Pub/Sub publisher plugin.
  fake/              In-memory publisher for tests.
migrations/          Schema migrations (goose-compatible).
test/outbox/         Integration tests (require a real Postgres).
internal/testutils/  Test helpers (mock DB, real-DB test schema spinup, goroutine leak detection).
```

## Status of v0

This is the lift from an old project. The interfaces here are minimal and **expected to break** as the project finds its shape:

- `Publisher` interface is `Publish(ctx, *Message) error`. No permanent-error discrimination, no batching, no `Close`. Adequate for one production publisher (Pub/Sub) plus a fake. Will be revisited when a second real broker plugin is added.
- The schema is the one originally shipped (`outbox_events` table). Renames and structural changes are deferred.
- Polling-only. No `LISTEN/NOTIFY`, no CDC.
- Single-replica relay. No leader election.

## Quick example

```go
import (
    "github.com/karolusz/outbox"
    "github.com/karolusz/outbox/outboxsqlx"
    "github.com/karolusz/outbox/publisher/gcppubsub"
)

// In your service (sqlx adopter; use a different sub-package for pgx etc.):
//   1. write events transactionally
err := outboxsqlx.Send(ctx, tx, outbox.Message{
    Data:        payload,
    Destination: "payments.events",
    OrderingKey: paymentID.String(),
    EventType:   "PaymentSucceeded",
    RetryLimit:  5,
})

// In your relay sidecar:
pub, _ := gcppubsub.New(ctx, "my-gcp-project")
relay := outbox.NewOutboxRelay(db, &logger, pub, nil)
<-relay.Start(ctx, nil)
```

Using a driver we don't ship a sub-package for? Implement the small
`outbox.TxWriter` interface against your driver's tx type and pass it
to `outbox.Send` directly.

## Running tests

The integration tests require a running Postgres with the lib's schema applied. A `docker-compose.yml` and `Makefile` provide a one-command setup.

```sh
make test-up       # start a local Postgres with the v0 schema applied
make test          # run the full suite against it
make test-down     # stop the container (keeps the volume; fast restart later)
make test-clean    # stop and wipe the volume (fresh schema next time)
```

The compose stack uses port `5434` so it does not collide with other Postgres instances on the host. The connection string defaults to `postgres://outbox:outbox@localhost:5434/outbox?sslmode=disable`; override it by exporting `DB_CONNECTION_STRING` before invoking `make test` (or `go test` directly).

Unit-only subset (no DB needed):

```sh
make test-unit
```

## License

MIT. See [LICENSE](./LICENSE).
