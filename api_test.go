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
// TxWriter abstraction → positional SQL → row visible in outbox.messages.
func TestSend_CanCreateDBEntry(t *testing.T) {
	db := newAPITestDB(t, "TestSend_CanCreateDBEntry")

	msg := outbox.Message{
		Data:        []byte("test payload"),
		Headers:     publisher.JSONBMap{"foo": "bar"},
		Address:     "test_topic",
		OrderingKey: "key1",
		RetryLimit:  5,
	}

	err := outbox.Send(context.Background(), sqlxDBWriter{db}, msg)
	assert.NoError(t, err, "Send should not return an error")

	var count int
	err = db.Get(&count, "SELECT COUNT(*) FROM outbox.messages WHERE address = 'test_topic'")
	assert.NoError(t, err)
	assert.Equal(t, 1, count, "exactly one row should be inserted")
}

// TestSend_FillsEventID confirms that a Send call with an empty EventID
// gets a fresh UUIDv7 filled in client-side (and the row carries it).
func TestSend_FillsEventID(t *testing.T) {
	db := newAPITestDB(t, "TestSend_FillsEventID")

	err := outbox.Send(context.Background(), sqlxDBWriter{db}, outbox.Message{
		Data:       []byte("test"),
		Address:    "test_topic",
		RetryLimit: 5,
	})
	require.NoError(t, err)

	var eventID string
	require.NoError(t, db.Get(&eventID, "SELECT event_id::text FROM outbox.messages WHERE address = 'test_topic'"))
	assert.Len(t, eventID, 36, "event_id should be a canonical 36-char UUID string")
	// UUIDv7 has '7' as the version nibble (first hex digit of the 3rd group).
	assert.Equal(t, byte('7'), eventID[14], "expected a UUIDv7; got version nibble %c", eventID[14])
}

// TestSendBatch_CanCreateMultipleEntries confirms SendBatch loops correctly.
func TestSendBatch_CanCreateMultipleEntries(t *testing.T) {
	db := newAPITestDB(t, "TestSendBatch_CanCreateMultipleEntries")

	msgs := []outbox.Message{
		{Data: []byte("one"), Address: "batch_topic", OrderingKey: "k1", RetryLimit: 3},
		{Data: []byte("two"), Address: "batch_topic", OrderingKey: "k2", RetryLimit: 3},
		{Data: []byte("three"), Address: "batch_topic", OrderingKey: "k3", RetryLimit: 3},
	}

	err := outbox.SendBatch(context.Background(), sqlxDBWriter{db}, msgs)
	assert.NoError(t, err)

	var count int
	err = db.Get(&count, "SELECT COUNT(*) FROM outbox.messages WHERE address = 'batch_topic'")
	assert.NoError(t, err)
	assert.Equal(t, 3, count)
}
