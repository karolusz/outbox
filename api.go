// Package outbox implements the Transactional Outbox pattern: events are
// persisted in the same transaction as the domain write that produces them,
// and a separate relay process publishes them at-least-once to a broker.
//
// This file holds the producer-side API. The caller passes a transaction
// (or DB) plus a Message and the row is INSERTed into the outbox table.
package outbox

import "database/sql"

// NamedExecer accepts both *sqlx.Tx and *sqlx.DB. The caller decides which
// one to pass — the outbox does not own the transaction lifecycle.
type NamedExecer interface {
	NamedExec(query string, arg any) (sql.Result, error)
}

// SendMessage saves a new outbox event in the database within the provided
// transaction handle. The caller is responsible for committing or rolling
// back the transaction.
func SendMessage(ex NamedExecer, msg Message) error {
	_, err := ex.NamedExec(queryInsert, msg)
	return err
}

// SendMessageMany saves a slice of outbox events in a single statement.
func SendMessageMany(ex NamedExecer, msgs []*Message) error {
	if len(msgs) == 0 {
		return nil
	}
	_, err := ex.NamedExec(queryInsert, msgs)
	return err
}

// queryInsert is consumed by sqlx.NamedExec — :name placeholders are bound
// to the matching `db:"name"` tags on the Message struct.
var queryInsert = `
	INSERT INTO outbox_events (data, attributes, topic, created_at, retry_count, retry_limit, ordering_key, event_type)
	VALUES (:data, :attributes, :topic, NOW(), 0, :retry_limit, :ordering_key, :event_type)
`
