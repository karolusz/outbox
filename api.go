// Package outbox implements the Transactional Outbox pattern: events are
// persisted in the same transaction as the domain write that produces them,
// and a separate relay process publishes them at-least-once to a broker.
//
// This file holds the producer-side API. Adopters call Send (or SendBatch)
// inside their own transaction; the lib INSERTs a row that the relay will
// later pick up and publish.
package outbox

import "context"

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
func Send(ctx context.Context, tx TxWriter, msg Message) error {
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
// retry_count is always 0 on insert; the relay manages it from there.
const insertSQL = `
	INSERT INTO outbox_events (data, attributes, topic, created_at, retry_count, retry_limit, ordering_key, event_type)
	VALUES ($1, $2, $3, NOW(), 0, $4, $5, $6)
`

// insertArgs returns the positional args matching insertSQL for a single
// Message. Order matches the column list in insertSQL exactly.
func insertArgs(msg Message) []any {
	return []any{
		msg.Data,
		msg.Attributes,
		msg.Address,
		msg.RetryLimit,
		msg.OrderingKey,
		msg.EventType,
	}
}
