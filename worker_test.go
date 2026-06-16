//go:build testing

package outbox

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/karolusz/outbox/internal/testutils"
	"github.com/stretchr/testify/require"
)

type fakePublisher struct {
	publishFn func(e *Message) error
}

func (f fakePublisher) Publish(c context.Context, target string, e *Message) error {
	if f.publishFn != nil {
		return f.publishFn(e)
	}
	return nil
}

func (f fakePublisher) Close(ctx context.Context) error { return nil }

// TestWorker_ExitsOnQueueClose ensures the worker goroutine exits cleanly
// when its queue is closed, leaving no goroutines behind.
func TestWorker_ExitsOnQueueClose(t *testing.T) {
	defer testutils.NoGoroutineLeak(t)

	db, _, cleanup := testutils.SetupMockDB(t)
	defer cleanup()

	testLogger := testutils.NewTestLogger(t)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		o := &OutboxRelay{
			db:        db,
			logger:    &testLogger,
			workerCfg: &WorkerConfig{},
			book:      SinglePublisherAddressBook(fakePublisher{publishFn: func(e *Message) error { return nil }}),
		}

		queue := make(chan int64)
		close(queue)

		go func() { o.worker(ctx, 0, queue) }()

		<-ctx.Done()
		synctest.Wait()
	})
}

// TestWorker_ExitsOnContextCancel verifies the worker exits when the context
// is cancelled even with messages still in the queue (the post-receive
// ctx.Err() check guards against doing further work after shutdown begins).
func TestWorker_ExitsOnContextCancel(t *testing.T) {
	defer testutils.NoGoroutineLeak(t)

	db, _, cleanup := testutils.SetupMockDB(t)
	defer cleanup()

	testLogger := testutils.NewTestLogger(t)

	ctx, cancel := context.WithCancel(context.Background())

	o := &OutboxRelay{
		db:        db,
		logger:    &testLogger,
		workerCfg: &WorkerConfig{},
		book:      SinglePublisherAddressBook(fakePublisher{publishFn: func(e *Message) error { return nil }}),
	}

	queue := make(chan int64, 4)
	queue <- 1
	queue <- 2
	cancel()

	done := make(chan struct{})
	go func() {
		o.worker(ctx, 0, queue)
		close(done)
	}()

	select {
	case <-done:
		// expected
	case <-time.After(200 * time.Millisecond):
		t.Fatal("worker did not exit after context cancel")
	}
}

// TestWorker_RecoversFromPanicAndContinues verifies the end-to-end behaviour:
// a panic inside the publish step is caught, the worker continues with the
// next id from the queue, and the panicked row's retry_count is incremented
// via markPanickedDeliveryAttempt.
func TestWorker_RecoversFromPanicAndContinues(t *testing.T) {
	defer testutils.NoGoroutineLeak(t)

	db, testLogger := setupTest(t, "TestWorker_RecoversFromPanicAndContinues", "worker_recoversFromPanic.sql")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	processed := make(chan int64, 2)
	o := &OutboxRelay{
		db:        db,
		logger:    &testLogger,
		workerCfg: &WorkerConfig{WorkerCount: 1},
		book: SinglePublisherAddressBook(fakePublisher{
			publishFn: func(e *Message) error {
				if e.ID == 999 {
					panic("simulated panic")
				}
				processed <- e.ID
				return nil
			},
		}),
	}

	queue := make(chan int64, 2)
	done := make(chan struct{})
	go func() {
		o.worker(ctx, 0, queue)
		close(done)
	}()

	queue <- 999 // panic
	queue <- 42  // should be processed by the same worker after recovery
	close(queue)

	select {
	case id := <-processed:
		require.Equal(t, int64(42), id, "second event should be processed after panic recovery")
	case <-time.After(1 * time.Second):
		t.Fatal("timeout: event 42 was not processed after panic; worker may have died")
	}

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("worker did not exit after queue close")
	}

	var retryCount int
	require.NoError(t, db.Get(&retryCount, "SELECT retry_count FROM outbox_events WHERE id = 999"))
	require.Equal(t, 1, retryCount, "retry_count should be incremented after panic")

	var lastAttemptedAtSet bool
	require.NoError(t, db.Get(&lastAttemptedAtSet, "SELECT last_attempted_at IS NOT NULL FROM outbox_events WHERE id = 999"))
	require.True(t, lastAttemptedAtSet, "last_attempted_at should be set after a panic marks the row")
}

// TestProcessOne_PanicReturnsErrPanic exercises the named-return panic-to-error
// conversion directly: a panic inside the publish path becomes an error that
// wraps ErrPanic. The caller (the worker) relies on errors.Is(err, ErrPanic)
// to decide whether to call markPanickedDeliveryAttempt.
func TestProcessOne_PanicReturnsErrPanic(t *testing.T) {
	db, testLogger := setupTest(t, "TestProcessOne_PanicReturnsErrPanic", "worker_recoversFromPanic.sql")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	o := &OutboxRelay{
		db:        db,
		logger:    &testLogger,
		workerCfg: &WorkerConfig{},
		book: SinglePublisherAddressBook(fakePublisher{
			publishFn: func(e *Message) error { panic("boom") },
		}),
	}

	err := o.processOne(ctx, testLogger, 999)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrPanic), "processOne should wrap panic as ErrPanic; got: %v", err)
	require.Contains(t, err.Error(), "boom", "wrapped error should include the panic value")
}

// TestMarkPanickedDeliveryAttempt_IncrementsRetryCount is a focused unit-style
// check that the marking path actually persists the retry_count bump. The
// end-to-end worker test also covers this, but a direct assertion here makes
// it harder to break the contract by accident.
func TestMarkPanickedDeliveryAttempt_IncrementsRetryCount(t *testing.T) {
	db, testLogger := setupTest(t, "TestMarkPanickedDeliveryAttempt_IncrementsRetryCount", "worker_recoversFromPanic.sql")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	o := &OutboxRelay{db: db, logger: &testLogger, workerCfg: &WorkerConfig{}}

	var initial int
	require.NoError(t, db.Get(&initial, "SELECT retry_count FROM outbox_events WHERE id = 999"))

	require.NoError(t, o.markPanickedDeliveryAttempt(ctx, 999, fmt.Errorf("simulated panic")))

	var after int
	require.NoError(t, db.Get(&after, "SELECT retry_count FROM outbox_events WHERE id = 999"))
	require.Equal(t, initial+1, after, "retry_count should be incremented by one")
}

// TestMarkPanickedDeliveryAttempt_SkipsWhenLocked verifies that when another
// transaction already holds a conflicting lock on the row, the marking path
// returns immediately (does not block) and does NOT increment retry_count.
//
// This is the safety net for the race where the panicked row gets re-polled
// and picked up by a second worker between our processOne tx.Rollback and our
// marking tx open. The other worker will record the attempt itself, so we
// must not contend on the row lock.
func TestMarkPanickedDeliveryAttempt_SkipsWhenLocked(t *testing.T) {
	db, testLogger := setupTest(t, "TestMarkPanickedDeliveryAttempt_SkipsWhenLocked", "worker_recoversFromPanic.sql")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	o := &OutboxRelay{db: db, logger: &testLogger, workerCfg: &WorkerConfig{}}

	var initialRetry int
	require.NoError(t, db.Get(&initialRetry, "SELECT retry_count FROM outbox_events WHERE id = 999"))

	// Hold a conflicting lock on row 999 in a separate transaction.
	blockerTx, err := db.Beginx()
	require.NoError(t, err)
	defer func() { _ = blockerTx.Rollback() }()

	var locked int64
	require.NoError(t, blockerTx.Get(&locked, "SELECT id FROM outbox_events WHERE id = 999 FOR NO KEY UPDATE"))
	require.Equal(t, int64(999), locked)

	// markPanickedDeliveryAttempt must NOT block here. Run it on another goroutine
	// guarded by a tight timeout so we fail loudly if it does block.
	markResult := make(chan error, 1)
	go func() {
		markResult <- o.markPanickedDeliveryAttempt(ctx, 999, fmt.Errorf("simulated panic"))
	}()

	select {
	case err := <-markResult:
		require.NoError(t, err, "markPanickedDeliveryAttempt must not error when row is locked; it should skip")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("markPanickedDeliveryAttempt blocked waiting for row lock; it must skip via SKIP LOCKED")
	}

	// retry_count must be unchanged — the marking was skipped, not applied.
	var afterRetry int
	require.NoError(t, db.Get(&afterRetry, "SELECT retry_count FROM outbox_events WHERE id = 999"))
	require.Equal(t, initialRetry, afterRetry, "retry_count must not change when the row was locked")

	// Releasing the blocker lock and re-marking should now succeed.
	require.NoError(t, blockerTx.Rollback())

	require.NoError(t, o.markPanickedDeliveryAttempt(ctx, 999, fmt.Errorf("simulated panic")))

	var finalRetry int
	require.NoError(t, db.Get(&finalRetry, "SELECT retry_count FROM outbox_events WHERE id = 999"))
	require.Equal(t, initialRetry+1, finalRetry, "retry_count should be incremented once the lock is released")
}

// TestWorker_HandlesMultipleConsecutivePanics verifies the worker survives
// more than one panic. A single-panic test doesn't prove the recovery is
// idempotent across iterations — a bug like "the defer stack is corrupted
// after recover" would only show up on the second panic. Here both seeded
// events panic on their first attempt and succeed on their second.
func TestWorker_HandlesMultipleConsecutivePanics(t *testing.T) {
	defer testutils.NoGoroutineLeak(t)

	db, testLogger := setupTest(t, "TestWorker_HandlesMultipleConsecutivePanics", "worker_recoversFromPanic.sql")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	publishCount := 0
	o := &OutboxRelay{
		db: db, logger: &testLogger, workerCfg: &WorkerConfig{WorkerCount: 1},
		book: SinglePublisherAddressBook(fakePublisher{
			publishFn: func(e *Message) error {
				publishCount++
				if publishCount <= 2 {
					panic(fmt.Sprintf("forced panic %d on event %d", publishCount, e.ID))
				}
				return nil
			},
		}),
	}

	queue := make(chan int64, 4)
	done := make(chan struct{})
	go func() {
		o.worker(ctx, 0, queue)
		close(done)
	}()

	queue <- 999 // panic (publishCount=1)
	queue <- 42  // panic (publishCount=2)
	queue <- 999 // succeed (publishCount=3)
	queue <- 42  // succeed (publishCount=4)
	close(queue)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not exit after multiple panics")
	}

	require.Equal(t, 4, publishCount, "publisher should be called exactly 4 times (2 panics + 2 successes)")

	var remaining int
	require.NoError(t, db.Get(&remaining, "SELECT COUNT(*) FROM outbox_events WHERE id IN (999, 42)"))
	require.Equal(t, 0, remaining, "both events should be deleted after their eventual successful publish")
}

// TestWorker_DoesNotMarkOnNormalPublishError is the most important regression
// test for the supervisor removal. The new worker uses errors.Is(err, ErrPanic)
// to decide whether to call markPanickedDeliveryAttempt. If that differentiation
// regresses (e.g. a future refactor incorrectly marks on every non-nil error),
// regular publish failures would double-increment retry_count — silently
// halving the effective retry budget.
//
// Here the publisher returns a plain error (not a panic). processOne should
// increment retry_count exactly once inside its own transaction. The worker
// must NOT also call markPanickedDeliveryAttempt.
func TestWorker_DoesNotMarkOnNormalPublishError(t *testing.T) {
	defer testutils.NoGoroutineLeak(t)

	db, testLogger := setupTest(t, "TestWorker_DoesNotMarkOnNormalPublishError", "worker_recoversFromPanic.sql")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	o := &OutboxRelay{
		db: db, logger: &testLogger, workerCfg: &WorkerConfig{WorkerCount: 1},
		book: SinglePublisherAddressBook(fakePublisher{
			publishFn: func(e *Message) error {
				return fmt.Errorf("simulated publish error (NOT a panic)")
			},
		}),
	}

	queue := make(chan int64, 1)
	done := make(chan struct{})
	go func() {
		o.worker(ctx, 0, queue)
		close(done)
	}()

	queue <- 999
	close(queue)

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("worker did not exit")
	}

	var retry int
	require.NoError(t, db.Get(&retry, "SELECT retry_count FROM outbox_events WHERE id = 999"))
	require.Equal(t, 1, retry,
		"retry_count should be incremented exactly once on a regular publish error; a value of 2 would mean the panic-marking path ran on a non-panic error (regression)")
}

// TestWorker_PanicsBoundedByRetryLimit documents and verifies the natural rate
// limit on panic storms. The supervisor used to have exponential backoff
// between restarts. With the supervisor gone, there is no per-iteration delay
// in the worker — but a panicking row eventually hits retry_limit, at which
// point getEventByIDIfNotLocked returns sql.ErrNoRows (its WHERE clause
// includes retry_count < retry_limit), processOne returns nil without invoking
// publish, and no further marking happens.
//
// This test asserts the cap: with retry_limit=2 and a publisher that always
// panics, the row reaches retry_count=2 after exactly 2 publish attempts and
// stays there no matter how many more times the worker is fed the same id.
func TestWorker_PanicsBoundedByRetryLimit(t *testing.T) {
	defer testutils.NoGoroutineLeak(t)

	db, testLogger := setupTest(t, "TestWorker_PanicsBoundedByRetryLimit", "worker_recoversFromPanic.sql")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	publishCount := 0
	o := &OutboxRelay{
		db: db, logger: &testLogger, workerCfg: &WorkerConfig{WorkerCount: 1},
		book: SinglePublisherAddressBook(fakePublisher{
			publishFn: func(e *Message) error {
				publishCount++
				panic("always panic")
			},
		}),
	}

	queue := make(chan int64, 5)
	done := make(chan struct{})
	go func() {
		o.worker(ctx, 0, queue)
		close(done)
	}()

	// Feed the same id 5 times. retry_limit=2 in the seed, so only the first
	// 2 attempts should reach publish; the last 3 should short-circuit at the
	// lock query.
	for range 5 {
		queue <- 999
	}
	close(queue)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not exit")
	}

	var retry int
	require.NoError(t, db.Get(&retry, "SELECT retry_count FROM outbox_events WHERE id = 999"))
	require.Equal(t, 2, retry, "retry_count should be capped at retry_limit (2) regardless of further attempts")
	require.Equal(t, 2, publishCount, "publisher should be called exactly retry_limit (2) times; further attempts must short-circuit at the lock query")
}

// TestMarkPanickedDeliveryAttempt_NoOpOnMissingRow covers the edge case where
// a panic happens AFTER processOne's COMMIT (e.g. in the trailing debug log,
// or in any post-commit code path). The row has been deleted; markPanickedDelivery
// Attempt should be a clean no-op, not return an error that would noisy the logs.
func TestMarkPanickedDeliveryAttempt_NoOpOnMissingRow(t *testing.T) {
	db, testLogger := setupTest(t, "TestMarkPanickedDeliveryAttempt_NoOpOnMissingRow", "")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	o := &OutboxRelay{db: db, logger: &testLogger, workerCfg: &WorkerConfig{}}

	require.NoError(t, o.markPanickedDeliveryAttempt(ctx, 999999, fmt.Errorf("simulated panic")),
		"marking a non-existent row must be a no-op; covers the post-commit panic edge case")
}

// TestMarkPanickedDeliveryAttempt_SkipsAtRetryLimit verifies that the marking
// path respects the retry_count < retry_limit cap. A row already at the limit
// must not be incremented further — otherwise the panic-marking path could push
// retry_count past retry_limit, diverging from the poll query's invariant and
// producing rows that are filtered out of polling but still have wrong counts.
func TestMarkPanickedDeliveryAttempt_SkipsAtRetryLimit(t *testing.T) {
	db, testLogger := setupTest(t, "TestMarkPanickedDeliveryAttempt_SkipsAtRetryLimit", "worker_recoversFromPanic.sql")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	o := &OutboxRelay{db: db, logger: &testLogger, workerCfg: &WorkerConfig{}}

	// Force row 999 to be at the cap.
	_, err := db.Exec("UPDATE outbox_events SET retry_count = retry_limit WHERE id = 999")
	require.NoError(t, err)

	var atCap int
	require.NoError(t, db.Get(&atCap, "SELECT retry_count FROM outbox_events WHERE id = 999"))

	require.NoError(t, o.markPanickedDeliveryAttempt(ctx, 999, fmt.Errorf("simulated panic")))

	var afterRetry int
	require.NoError(t, db.Get(&afterRetry, "SELECT retry_count FROM outbox_events WHERE id = 999"))
	require.Equal(t, atCap, afterRetry, "retry_count must not exceed retry_limit even via the panic-marking path")
}
