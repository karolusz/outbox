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
// Adopters typically do not implement this directly — for sqlx, pgx,
// etc., import the matching sub-package (outbox/outboxsqlx, ...) and
// pass the native tx type. The signature matches database/sql's
// ExecContext minus the unused result. Any type with that method
// structurally satisfies the interface, but passing a non-transactional
// handle defeats the point of the outbox.
type TxWriter interface {
	ExecContext(ctx context.Context, query string, args ...any) error
}

// Send saves a single Message inside the caller's transaction. The
// caller commits or rolls back. If msg.EventID is empty, Send fills it
// with a fresh UUIDv7 so producers can log/trace the ID before INSERT.
func Send(ctx context.Context, tx TxWriter, msg Message) error {
	if msg.EventID == "" {
		msg.EventID = publisher.NewEventID()
	}
	return tx.ExecContext(ctx, insertSQL, insertArgs(msg)...)
}

// SendBatch saves multiple Messages inside the caller's transaction,
// in order. Returns on the first error; tx rollback cleans up partial
// writes.
func SendBatch(ctx context.Context, tx TxWriter, msgs []Message) error {
	for _, m := range msgs {
		if err := Send(ctx, tx, m); err != nil {
			return err
		}
	}
	return nil
}

// insertSQL is the producer-side INSERT, with positional placeholders
// so every Postgres driver can execute it directly. retry_count and
// created_at come from schema defaults.
const insertSQL = `
	INSERT INTO outbox.messages
		(event_id, address, data, headers, ordering_key, retry_limit)
	VALUES
		($1, $2, $3, $4, $5, $6)
`

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
