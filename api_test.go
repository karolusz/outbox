//go:build testing

package outbox

import (
	"context"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
)

// sqlxDBWriter is a tiny TxWriter implementation for tests that need to
// call Send against a *sqlx.DB (rather than constructing a real tx). It
// mirrors the adapter that lives in the outboxsqlx sub-package; reproduced
// here to avoid an internal-package import cycle.
type sqlxDBWriter struct{ db *sqlx.DB }

func (w sqlxDBWriter) ExecContext(ctx context.Context, query string, args ...any) error {
	_, err := w.db.ExecContext(ctx, query, args...)
	return err
}

// TestSend_CanCreateDBEntry confirms the producer-side Send issues the
// INSERT correctly against a live Postgres. Exercises the full path:
// TxWriter abstraction → positional SQL → row visible in outbox_events.
func TestSend_CanCreateDBEntry(t *testing.T) {
	db, _ := setupTest(t, "TestSend_CanCreateDBEntry", "")

	msg := Message{
		Data:        []byte("test payload"),
		Attributes:  JSONBMap{"foo": "bar"},
		Address: "test_topic",
		OrderingKey: "key1",
		EventType:   "test.event",
		RetryLimit:  5,
	}

	err := Send(context.Background(), sqlxDBWriter{db}, msg)
	assert.NoError(t, err, "Send should not return an error")

	var count int
	err = db.Get(&count, "SELECT COUNT(*) FROM outbox_events WHERE topic = 'test_topic'")
	assert.NoError(t, err)
	assert.Equal(t, 1, count, "exactly one row should be inserted")
}

// TestSendBatch_CanCreateMultipleEntries confirms SendBatch loops correctly.
func TestSendBatch_CanCreateMultipleEntries(t *testing.T) {
	db, _ := setupTest(t, "TestSendBatch_CanCreateMultipleEntries", "")

	msgs := []Message{
		{Data: []byte("one"), Address: "batch_topic", OrderingKey: "k1", EventType: "evt.1", RetryLimit: 3},
		{Data: []byte("two"), Address: "batch_topic", OrderingKey: "k2", EventType: "evt.2", RetryLimit: 3},
		{Data: []byte("three"), Address: "batch_topic", OrderingKey: "k3", EventType: "evt.3", RetryLimit: 3},
	}

	err := SendBatch(context.Background(), sqlxDBWriter{db}, msgs)
	assert.NoError(t, err)

	var count int
	err = db.Get(&count, "SELECT COUNT(*) FROM outbox_events WHERE topic = 'batch_topic'")
	assert.NoError(t, err)
	assert.Equal(t, 3, count)
}
