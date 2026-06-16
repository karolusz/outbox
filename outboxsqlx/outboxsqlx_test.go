//go:build testing

package outboxsqlx_test

import (
	"context"
	"os"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"

	"github.com/karolusz/outbox"
	"github.com/karolusz/outbox/internal/testutils"
	"github.com/karolusz/outbox/outboxsqlx"
	"github.com/karolusz/outbox/publisher"
)

// TestSend_InsertsRow exercises the outboxsqlx public path end-to-end:
// open a real *sqlx.Tx, call outboxsqlx.Send, commit, verify the row is in
// outbox_events. Catches typos / wrong delegation inside the sub-package
// that the top-level api_test.go (which uses its own inline adapter) does
// not exercise.
func TestSend_InsertsRow(t *testing.T) {
	connStr := os.Getenv("DB_CONNECTION_STRING")
	if connStr == "" {
		t.Skip("DB_CONNECTION_STRING not set")
	}

	db, cleanup, err := testutils.NewTestDB(connStr, "TestSend_InsertsRow", 2, 2)
	require.NoError(t, err)
	t.Cleanup(cleanup)

	tx, err := db.Beginx()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	msg := outbox.Message{
		Data:        []byte("hello via outboxsqlx"),
		Attributes:  publisher.JSONBMap{"k": "v"},
		Address:     "sqlx_topic",
		OrderingKey: "key-1",
		EventType:   "test.sqlx",
		RetryLimit:  3,
	}

	require.NoError(t, outboxsqlx.Send(context.Background(), tx, msg))
	require.NoError(t, tx.Commit())

	var count int
	require.NoError(t, db.Get(&count, "SELECT COUNT(*) FROM outbox_events WHERE topic = $1", "sqlx_topic"))
	require.Equal(t, 1, count)
}

// TestSendBatch_InsertsAll covers the batch path through the sub-package.
func TestSendBatch_InsertsAll(t *testing.T) {
	connStr := os.Getenv("DB_CONNECTION_STRING")
	if connStr == "" {
		t.Skip("DB_CONNECTION_STRING not set")
	}

	db, cleanup, err := testutils.NewTestDB(connStr, "TestSendBatch_InsertsAll", 2, 2)
	require.NoError(t, err)
	t.Cleanup(cleanup)

	tx, err := db.Beginx()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })

	msgs := []outbox.Message{
		{Data: []byte("a"), Address: "batch", OrderingKey: "k1", EventType: "e1", RetryLimit: 3},
		{Data: []byte("b"), Address: "batch", OrderingKey: "k2", EventType: "e2", RetryLimit: 3},
	}
	require.NoError(t, outboxsqlx.SendBatch(context.Background(), tx, msgs))
	require.NoError(t, tx.Commit())

	var count int
	require.NoError(t, db.Get(&count, "SELECT COUNT(*) FROM outbox_events WHERE topic = $1", "batch"))
	require.Equal(t, 2, count)
}
