// Package outbox implements the Transactional Outbox pattern: events are
// persisted in the same transaction as the domain write that produces them,
// and a separate relay process publishes them at-least-once to a broker.
//
// This file holds the producer-side API. Adopters call Send (or SendBatch)
// inside their own transaction; the lib INSERTs a row that the relay will
// later pick up and publish.
package outbox

import (
	"context"

	"github.com/karolusz/outbox/publisher"
)

// TxWriter is the adapter the lib writes the outbox row through.
//
// Adopters do not implement this interface unless they are using a driver
// the lib does not ship a sub-package for. For sqlx, pgx, etc., import the
// matching sub-package (outbox/outboxsqlx, ...) and pass the native tx
// type directly — the sub-package owns the adapter internally.
//
// The single method signature matches database/sql's ExecContext, minus the
// unused result. The name "TxWriter" reflects the intended usage pattern:
// adopters should pass their transaction handle so the outbox row is
// inserted in the same tx as the domain write that produced it. Any type
// with an ExecContext method (including *sql.DB) structurally satisfies
// the interface, but passing a non-transactional handle defeats the point
// of the outbox.
type TxWriter interface {
	ExecContext(ctx context.Context, query string, args ...any) error
}

// Send saves a single Message inside the caller's transaction. The caller
// is responsible for committing or rolling back the transaction.
//
// If msg.EventID is empty, Send fills it with a fresh UUIDv7 before the
// INSERT. Producers who want to log or correlate the event ID before the
// row is written can populate it themselves; producers who don't care
// get a sensible default for free.
func Send(ctx context.Context, tx TxWriter, msg Message) error {
	if msg.EventID == "" {
		msg.EventID = publisher.NewEventID()
	}
	return tx.ExecContext(ctx, insertSQL, insertArgs(msg)...)
}

// SendBatch saves multiple Messages inside the caller's transaction, in
// order. Returns immediately on the first error; the caller's tx rollback
// handles cleanup of any partial writes.
func SendBatch(ctx context.Context, tx TxWriter, msgs []Message) error {
	for _, m := range msgs {
		if err := Send(ctx, tx, m); err != nil {
			return err
		}
	}
	return nil
}

// insertSQL is the producer-side INSERT, with positional placeholders so
// every Postgres driver (database/sql, sqlx, pgx) can execute it directly.
// retry_count and created_at are populated by the schema defaults so the
// producer doesn't need to know them.
const insertSQL = `
	INSERT INTO outbox.messages
		(event_id, address, data, headers, ordering_key, retry_limit)
	VALUES
		($1, $2, $3, $4, $5, $6)
`

// insertArgs returns the positional args matching insertSQL for a single
// Message. Order matches the column list in insertSQL exactly.
func insertArgs(msg Message) []any {
	return []any{
		msg.EventID,
		msg.Address,
		msg.Data,
		msg.Headers,
		msg.OrderingKey,
		msg.RetryLimit,
	}
}
