# Producer-side examples

Minimal working examples for each non-Go language. Each example is a self-contained file that inserts a single outbox row using the SQL-only integration path described in [`../insert.sql`](../insert.sql).

These exist to verify the contract works end-to-end without an SDK and to be copy-paste-able starting points for adopters.

| Language | File | Driver | UUIDv7 |
|---|---|---|---|
| Python 3 | [`python/insert.py`](./python/insert.py) | `psycopg` 3 | `uuid.uuid7` (3.14+) or `uuid7` package |
| TypeScript / Node | [`typescript/insert.ts`](./typescript/insert.ts) | `pg` (node-postgres) | `uuid` package (`v7`) |

## Smoke testing

To run any of these examples against the lib's test compose stack:

```sh
# In the repo root:
make test-up

# In another shell, point DATABASE_URL at the test stack:
export DATABASE_URL='postgres://outbox:outbox@localhost:5434/outbox'

# Run the Python example:
cd docs/protocol/examples/python
python3 insert.py

# Verify the row landed:
docker exec outbox-postgres-1 psql -U outbox outbox \
  -c "SELECT id, event_id, address FROM outbox.messages;"

# Run the relay binary to see it processed:
docker run --rm --network=host \
  -e DB_CONNECTION_STRING="$DATABASE_URL" \
  -v $(pwd)/your-addressbook.yaml:/etc/outbox/addressbook.yaml \
  ghcr.io/karolusz/outbox-relay:latest --log-level=debug
```

The relay should pick up the row, publish it to the address book's resolved publisher, and delete it.

## What these examples deliberately don't do

- **Don't ship a "Python outbox client library."** The 20-LOC `emit_event` helper *is* the integration. Wrapping it in a per-language SDK would add vendoring + version coordination overhead with marginal value.
- **Don't model CloudEvents.** The examples just pass an opaque JSON payload. Adopters who want CloudEvents serialize the CloudEvent envelope into `payload` themselves — see [`../address-book.md#cloudevents-binding-modes`](../address-book.md#cloudevents-binding-modes).
- **Don't show full error handling, retry logic, or production-grade tracing.** These are minimal demonstrations; adopt them into your own service's patterns.
