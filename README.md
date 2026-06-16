# outbox

A transactional outbox for Go services on Postgres.

**Status**: v0.x — extraction from a production fintech codebase. API will continue to change before v1.

## What it does

Persists events in the same transaction as your domain writes, then a separate relay process publishes them at-least-once to a broker. Consumers must be idempotent.

The pattern: [Transactional Outbox](https://microservices.io/patterns/data/transactional-outbox.html).

## Packages

The library is intentionally split so producers pull in only the dependencies they actually use.

| Package | Use it for | Third-party deps it adds |
|---|---|---|
| `github.com/karolusz/outbox` | Producer API (`Send`, `SendBatch`), `Message`, `AddressBook` value type | none — stdlib only |
| `github.com/karolusz/outbox/publisher` | `Publisher` interface + plugin registry. Plugin authors import only this. | none — stdlib only |
| `github.com/karolusz/outbox/yamlconfig` | YAML loaders: `LoadAddressBook`, `LoadAddressBookValidateOnly` | `gopkg.in/yaml.v3` |
| `github.com/karolusz/outbox/relay` | The relay engine: poll, claim, dispatch, mark | `sqlx`, `zerolog`, `lib/pq` (transitive) |
| `github.com/karolusz/outbox/outboxsqlx` | Thin `TxWriter` adapter for `*sqlx.Tx` | `sqlx` |
| `github.com/karolusz/outbox/publisher/gcppubsub` | GCP Pub/Sub plugin | GCP SDK |
| `github.com/karolusz/outbox/publisher/fake` | In-memory plugin for tests | none |

A producer that does `import "github.com/karolusz/outbox"` for `Send` inherits **zero third-party deps**. Verified by:

```sh
go list -deps github.com/karolusz/outbox | grep -E '^[a-z0-9-]+\.[a-z]'
# (empty)
```

## Roles and import sets

### Producer

Writes events in its own DB transaction. Does not run a relay, does not need to know about brokers.

```go
import (
    "github.com/karolusz/outbox"
    "github.com/karolusz/outbox/outboxsqlx" // or write a small TxWriter for another driver
)

err := outboxsqlx.Send(ctx, tx, outbox.Message{
    Data:        payload,
    Address:     "payments.completed.v1",
    OrderingKey: paymentID.String(),
    EventType:   "PaymentSucceeded",
    RetryLimit:  5,
})
```

Producer dep closure: stdlib + sqlx (only because outboxsqlx uses it). No yaml, no zerolog, no broker SDKs.

### Producer with address validation (optional)

A producer that wants to reject unknown addresses at API boundaries — before they even reach Postgres — loads the address book in validate-only mode:

```go
import (
    "github.com/karolusz/outbox"
    "github.com/karolusz/outbox/yamlconfig"
)

book, _ := yamlconfig.LoadAddressBookValidateOnly("outbox.addressbook.yaml")
// at API boundary:
if err := book.Validate(addr); err != nil {
    return fmt.Errorf("unknown outbox address: %w", err)
}
// then construct and Send the Message as usual.
```

Adds `yaml.v3` and nothing else. The publisher returned by `Resolve` is a stub that errors on `Publish` (intentional — validate-only books cannot deliver).

### Relay operator

A separate binary (or a goroutine inside the producer's binary) that polls the table and dispatches:

```go
import (
    "github.com/karolusz/outbox"
    "github.com/karolusz/outbox/relay"
    "github.com/karolusz/outbox/yamlconfig"

    _ "github.com/karolusz/outbox/publisher/gcppubsub"
    // _ "company.com/internal/outbox-kafka-plugin"  // your custom plugin
)

book, _ := yamlconfig.LoadAddressBook(ctx, "outbox.addressbook.yaml")
r := relay.New(db, &logger, book, nil)
<-r.Start(ctx, nil)
```

Plugins register themselves via blank-import (`database/sql.Register`-style). Custom plugins live in the adopter's own module — no fork of this library needed.

## Writing a custom plugin

```go
package mykafka

import (
    "context"

    "github.com/karolusz/outbox/publisher"
)

type Publisher struct { /* ... */ }

func (p *Publisher) Publish(ctx context.Context, target string, msg *publisher.Message) error { /* ... */ }
func (p *Publisher) Close(ctx context.Context) error                                          { /* ... */ }

type Config struct {
    Brokers []string `yaml:"brokers"`
}

func init() {
    publisher.Register("mykafka", func(ctx context.Context, decode publisher.ConfigDecoder) (publisher.Publisher, error) {
        var cfg Config
        if err := decode(&cfg); err != nil {
            return nil, err
        }
        // construct and return a Publisher
        return New(cfg)
    })
}
```

Adopters use it by blank-importing the package alongside the lib-shipped ones:

```go
import _ "company.com/internal/mykafka"
```

## Address book

The address book is a routing table: producer-visible logical address → (Publisher, broker target). YAML format:

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
    target: payments-topic
  - name: mandates.created.v1
    publisher: pubsub-prod
    target: mandates-topic
```

The same file is loaded by:

- the relay binary (with `yamlconfig.LoadAddressBook` — instantiates plugins),
- producers that want pre-validation (with `yamlconfig.LoadAddressBookValidateOnly` — skips plugin instantiation, no broker SDKs needed).

Adopters who need to inject a publisher that YAML cannot describe (e.g. Vault-fetched credentials) construct it in Go and pass it as an option:

```go
book, err := yamlconfig.LoadAddressBook(ctx, "outbox.addressbook.yaml",
    outbox.WithPublisher("vault-creds", customPublisher),
    outbox.WithRoute("special.event.v1", outbox.Route{Publisher: "vault-creds", Target: "topic-x"}),
)
```

## What v0.x is and isn't

This is the lift from an old project, still finding its public shape.

- `Publisher` interface is `Publish(ctx, target, *Message) + Close(ctx)`. No permanent-error discrimination yet; every error is retried until `retry_limit`.
- Schema is the one originally shipped (`outbox_events` table). Renames and structural changes are deferred to a migration tool (planned).
- Polling-only. No `LISTEN/NOTIFY`, no CDC.
- Single-replica relay assumed. No leader election yet.

Expect breaking changes between v0.x releases. v1 lands when the producer surface and plugin contract have been used by more than one real adopter.

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
