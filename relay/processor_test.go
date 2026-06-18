//go:build testing

package relay

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/karolusz/outbox/internal/testutils"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

const (
	MaxPoolConnections = 2
	MaxIdleConnections = 2
	MaxWorkersCount    = 2
)

var (
	DBConnStr           = os.Getenv("DB_CONNECTION_STRING")
	defaultWorkerConfig = WorkerConfig{}
)

func setupTest(
	t *testing.T,
	testName string,
	seedSQLFile string,
) (*sqlx.DB, zerolog.Logger) {
	sqlDB, cleanup, err := testutils.NewTestDB(DBConnStr, testName, MaxPoolConnections, MaxIdleConnections)
	if err != nil {
		t.Fatalf("failed to create test database: %v", err)
	}
	t.Cleanup(cleanup)
	logger := testutils.NewTestLogger(t)

	if seedSQLFile != "" {
		seedSQLFile = "testdata/sql/" + seedSQLFile
	}
	testutils.SeedTables(t, sqlDB.DB, sqlFixtures, seedSQLFile)
	t.Cleanup(func() { teardownTest(t, sqlDB, testName) })
	return sqlDB, logger
}

func teardownTest(t *testing.T, db *sqlx.DB, schema string) {
	t.Helper()
	_, err := db.Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE")
	if err != nil {
		t.Fatalf("failed to drop schema %s: %v", schema, err)
	}
}

// TestEventProcessor_CanEnqueueIDs ensures that the event processor correctly
// enqueues IDs from the database.
func TestEventProcessor_CanEnqueueIDs(t *testing.T) {
	defer testutils.NoGoroutineLeak(t)
	// Setup outside timeout — DB schema creation + seeding takes variable time
	// in CI and should not eat into the test budget.
	db, testLogger := setupTest(t, "TestEventProcessor_CanEnqueueIDs", "eventProcessor_CanEnqueueIDs.sql")

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	o := &Relay{
		db:     db,
		logger: &testLogger,
		workerCfg: &WorkerConfig{
			WorkerCount: 0,
			TickPeriod:  5 * time.Millisecond,
			BatchSize:   10,
		},
	}

	queue := make(chan int64, 2)

	go o.eventProcessor(ctx, queue)

	var processed []int64
	select {
	case id := <-queue:
		processed = append(processed, id)
	case <-ctx.Done():
		t.Fatal("timeout waiting for first event")
	}
	select {
	case id := <-queue:
		processed = append(processed, id)
	case <-ctx.Done():
		t.Fatal("timeout waiting for second event")
	}

	require.ElementsMatch(t, []int64{100, 101}, processed)
}

// TestEventProcessor_QueueFullBlocksThenExitsOnCtx confirms that when the
// queue is full and the processor cannot enqueue any more IDs, it still
// exits cleanly once the parent ctx is canceled. The processor blocks on
// the channel send by design (back-pressure); the select-on-ctx.Done()
// in the enqueue loop is what releases it.
func TestEventProcessor_QueueFullBlocksThenExitsOnCtx(t *testing.T) {
	defer testutils.NoGoroutineLeak(t)
	db, _ := setupTest(t, "TestEventProcessor_QueueFullBlocksThenExitsOnCtx", "eventProcessor_CanEnqueueIDs.sql")

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	queue := make(chan int64, 1)

	var logBuf bytes.Buffer
	logger := zerolog.New(&logBuf).Level(zerolog.DebugLevel).With().Timestamp().Logger()

	o := &Relay{
		logger: &logger,
		db:     db,
		workerCfg: &WorkerConfig{
			WorkerCount: 0,
			TickPeriod:  10 * time.Millisecond,
			BatchSize:   5,
		},
	}

	// Pre-fill the queue so the processor's send blocks.
	queue <- 999

	done := make(chan struct{})
	go func() {
		o.eventProcessor(ctx, queue)
		close(done)
	}()

	select {
	case <-done:
		// processor exited within the ctx timeout window
	case <-time.After(500 * time.Millisecond):
		t.Fatal("eventProcessor did not exit when ctx canceled while queue was full")
	}
}
