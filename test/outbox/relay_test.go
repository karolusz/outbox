//go:build testing

// Package integrationoutbox holds end-to-end relay tests that exercise a
// real Postgres and a fake publisher. The package is built only under the
// `testing` build tag.
package integrationoutbox

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"

	"github.com/karolusz/outbox"
	"github.com/karolusz/outbox/internal/testutils"
	"github.com/karolusz/outbox/publisher/fake"
)

const (
	MaxPoolConnections = 4
	MaxIdleConnections = 4
	MaxWorkersCount    = 4
)

var DBConnStr string

func TestMain(m *testing.M) {
	DBConnStr = os.Getenv("DB_CONNECTION_STRING")
	os.Exit(m.Run())
}

func setupTest(
	t *testing.T,
	testName string,
	seedSQLFile string,
	workerConfig *outbox.WorkerConfig,
	forceErrorFn func(msg *outbox.Message) error,
) (*outbox.OutboxRelay, *fake.Publisher, *sqlx.DB) {
	sqlDB, cleanup, err := testutils.NewTestDB(DBConnStr, testName, MaxPoolConnections, MaxIdleConnections)
	if err != nil {
		t.Fatalf("failed to create test database: %v", err)
	}
	t.Cleanup(cleanup)
	logger := testutils.NewTestLogger(t)

	if seedSQLFile != "" {
		seedSQLFile = "testdata/sql/" + seedSQLFile
	}
	testutils.SeedTables(t, sqlDB.DB, SQLFiles, seedSQLFile)

	successChan := make(chan *outbox.Message, 100)
	failedChan := make(chan *outbox.Message, 100)
	t.Cleanup(func() { close(successChan); close(failedChan) })

	pub := fake.New()
	pub.Logger = &logger
	pub.BroadcastChan = successChan
	pub.FailedChan = failedChan
	pub.ForceErrorFn = forceErrorFn

	relay := outbox.NewOutboxRelay(sqlDB, &logger, pub, workerConfig)
	t.Cleanup(func() { teardownTest(t, sqlDB, testName) })
	return &relay, pub, sqlDB
}

func teardownTest(t *testing.T, db *sqlx.DB, schema string) {
	t.Helper()
	if _, err := db.Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE"); err != nil {
		t.Fatalf("failed to drop schema %s: %v", schema, err)
	}
}

// TestOutbox_EmitsEventsFromOutboxTable confirms the relay publishes a
// pre-seeded batch of rows.
func TestOutbox_EmitsEventsFromOutboxTable(t *testing.T) {
	seedSQLFile := "emitseventsfromoutboxtable.sql"

	workerConfig := &outbox.WorkerConfig{
		WorkerCount: MaxWorkersCount,
		QueueSize:   500,
		BatchSize:   200,
		TickPeriod:  2 * time.Second,
	}
	relay, pub, db := setupTest(t, "TestOutbox_EmitsEventsFromOutboxTable", seedSQLFile, workerConfig, nil)

	var initCount int
	if err := db.Get(&initCount, "SELECT COUNT(*) FROM outbox_events"); err != nil {
		t.Fatalf("count: %v", err)
	}
	assert.Greater(t, initCount, 0, "there should be some messages in the outbox table")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	relayComplete := relay.Start(ctx, nil)

	received := 0
receiveLoop:
	for {
		select {
		case <-pub.BroadcastChan:
			received++
			if received == initCount {
				cancel()
				<-relayComplete
				break receiveLoop
			}
		case <-ctx.Done():
			t.Fatal("timeout waiting for messages")
		}
	}
	assert.Equal(t, initCount, received, "not all events were published successfully")
}

// TestOutbox_EmitsEventsAsTheyCome confirms the relay picks up new rows
// inserted while it is already running.
func TestOutbox_EmitsEventsAsTheyCome(t *testing.T) {
	workerConfig := &outbox.WorkerConfig{
		WorkerCount: MaxWorkersCount,
		QueueSize:   500,
		BatchSize:   200,
		TickPeriod:  250 * time.Millisecond,
	}
	relay, pub, db := setupTest(t, "TestOutbox_EmitsEventsAsTheyCome", "", workerConfig, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	relayComplete := relay.Start(ctx, nil)

	const messagesToInsert = 40
	go insertOutboxEvents(t, db, messagesToInsert, 10*time.Second)

	received := 0
receiveLoop:
	for {
		select {
		case <-pub.BroadcastChan:
			received++
			if received == messagesToInsert {
				cancel()
				<-relayComplete
				break receiveLoop
			}
		case <-ctx.Done():
			t.Fatal("timeout waiting for messages")
		}
	}
	assert.Equal(t, messagesToInsert, received, "not all events were published successfully")
}

func insertOutboxEvents(t *testing.T, db *sqlx.DB, count int, totalTime time.Duration) {
	t.Helper()
	for i := range count {
		db.MustExec(
			`INSERT INTO outbox_events (data, attributes, topic, ordering_key, event_type)
			 VALUES (decode('48656c6c6f20576f726c64', 'hex'), '{"foo":"bar"}', 'test_topic', $1, 'user.created')`,
			i,
		)
		time.Sleep(totalTime / time.Duration(count))
	}
}

// TestOutbox_IncrementsRetryCounter confirms the relay increments retry_count
// on each failed publish attempt until the retry_limit is reached.
func TestOutbox_IncrementsRetryCounter(t *testing.T) {
	workerConfig := &outbox.WorkerConfig{
		WorkerCount: MaxWorkersCount,
		QueueSize:   500,
		BatchSize:   200,
		TickPeriod:  250 * time.Millisecond,
	}
	forceErrorFn := func(msg *outbox.Message) error {
		return fmt.Errorf("forced error for testing")
	}
	relay, pub, db := setupTest(t, "TestOutbox_IncrementsRetryCounter", "emitseventsfromoutboxtable.sql", workerConfig, forceErrorFn)

	var initCount int
	if err := db.Get(&initCount, "SELECT COUNT(*) FROM outbox_events"); err != nil {
		t.Fatalf("count: %v", err)
	}
	assert.Greater(t, initCount, 0, "there should be some messages in the outbox table")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	relayComplete := relay.Start(ctx, nil)

	received := 0
receiveLoop:
	for {
		select {
		case <-pub.FailedChan:
			received++
			if received == initCount*2 { // each row is retried until retry_limit (seed has retry_limit=2)
				cancel()
				<-relayComplete
				break receiveLoop
			}
		case <-ctx.Done():
			t.Fatal("timeout waiting for messages")
		}
	}

	var atMax int
	if err := db.Get(&atMax, "SELECT COUNT(*) FROM outbox_events WHERE retry_count = retry_limit"); err != nil {
		t.Fatalf("count: %v", err)
	}
	assert.Equal(t, initCount, atMax, "all events should have retry_count at retry_limit")
}
