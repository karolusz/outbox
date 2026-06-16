//go:build testing

package outbox_test

import (
	"context"
	"os"
	"testing"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/karolusz/outbox"
	"github.com/karolusz/outbox/internal/testutils"
	"github.com/karolusz/outbox/publisher"
)

// sqlxDBWriter is a tiny TxWriter implementation for tests that need to
// call Send against a *sqlx.DB (rather than constructing a real tx). It
// mirrors the adapter that lives in the outboxsqlx sub-package; reproduced
// here to avoid an outboxsqlx import in the root test file.
type sqlxDBWriter struct{ db *sqlx.DB }

func (w sqlxDBWriter) ExecContext(ctx context.Context, query string, args ...any) error {
	_, err := w.db.ExecContext(ctx, query, args...)
	return err
}

func newAPITestDB(t *testing.T, schema string) *sqlx.DB {
	t.Helper()
	connStr := os.Getenv("DB_CONNECTION_STRING")
	if connStr == "" {
		t.Skip("DB_CONNECTION_STRING not set")
	}
	db, cleanup, err := testutils.NewTestDB(connStr, schema, 2, 2)
	require.NoError(t, err)
	t.Cleanup(cleanup)
	return db
}

// TestSend_CanCreateDBEntry confirms the producer-side Send issues the
// INSERT correctly against a live Postgres. Exercises the full path:
// TxWriter abstraction → positional SQL → row visible in outbox_events.
func TestSend_CanCreateDBEntry(t *testing.T) {
	db := newAPITestDB(t, "TestSend_CanCreateDBEntry")

	msg := outbox.Message{
		Data:        []byte("test payload"),
		Attributes:  publisher.JSONBMap{"foo": "bar"},
		Address:     "test_topic",
		OrderingKey: "key1",
		EventType:   "test.event",
		RetryLimit:  5,
	}

	err := outbox.Send(context.Background(), sqlxDBWriter{db}, msg)
	assert.NoError(t, err, "Send should not return an error")

	var count int
	err = db.Get(&count, "SELECT COUNT(*) FROM outbox_events WHERE topic = 'test_topic'")
	assert.NoError(t, err)
	assert.Equal(t, 1, count, "exactly one row should be inserted")
}

// TestSendBatch_CanCreateMultipleEntries confirms SendBatch loops correctly.
func TestSendBatch_CanCreateMultipleEntries(t *testing.T) {
	db := newAPITestDB(t, "TestSendBatch_CanCreateMultipleEntries")

	msgs := []outbox.Message{
		{Data: []byte("one"), Address: "batch_topic", OrderingKey: "k1", EventType: "evt.1", RetryLimit: 3},
		{Data: []byte("two"), Address: "batch_topic", OrderingKey: "k2", EventType: "evt.2", RetryLimit: 3},
		{Data: []byte("three"), Address: "batch_topic", OrderingKey: "k3", EventType: "evt.3", RetryLimit: 3},
	}

	err := outbox.SendBatch(context.Background(), sqlxDBWriter{db}, msgs)
	assert.NoError(t, err)

	var count int
	err = db.Get(&count, "SELECT COUNT(*) FROM outbox_events WHERE topic = 'batch_topic'")
	assert.NoError(t, err)
	assert.Equal(t, 3, count)
}
