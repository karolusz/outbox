//go:build testing

package relay

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/karolusz/outbox"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countingMetrics records IncUnknownAddress calls. Used to assert the
// relay correctly emits the unknown-address signal.
type countingMetrics struct {
	mu      sync.Mutex
	unknown []string // addresses that came in, in order
}

func (m *countingMetrics) IncUnknownAddress(address string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unknown = append(m.unknown, address)
}

func (m *countingMetrics) UnknownAddresses() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.unknown))
	copy(out, m.unknown)
	return out
}

// TestProcessOne_UnknownAddress_PreservesRetryCount is the load-bearing
// invariant for the address-book integration: a row whose address is
// not in the book must NOT have its retry_count incremented. If it did,
// the row would eventually hit retry_limit and become invisible to
// polling — silent data loss precisely when the operator would want to
// recover the row by adding the missing address to the book.
func TestProcessOne_UnknownAddress_PreservesRetryCount(t *testing.T) {
	db, testLogger := setupTest(t, "TestProcessOne_UnknownAddress_PreservesRetryCount", "unknown_address.sql")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Address book has NO entry for "address.not.in.book.v1" (the address
	// used by the seeded row).
	book, err := outbox.NewAddressBook(
		outbox.WithPublisher("some-pub", fakePublisher{}),
		outbox.WithRoute("a.different.address", outbox.Route{Publisher: "some-pub", Target: "t"}),
	)
	require.NoError(t, err)

	metrics := &countingMetrics{}
	o := &Relay{
		db:        db,
		logger:    &testLogger,
		workerCfg: &WorkerConfig{},
		book:      book,
		metrics:   metrics,
	}

	require.NoError(t, o.processOne(ctx, testLogger, 777))

	// retry_count MUST be unchanged (still 0).
	var retryCount int
	require.NoError(t, db.Get(&retryCount, "SELECT retry_count FROM outbox_events WHERE id = 777"))
	assert.Equal(t, 0, retryCount, "unknown-address handling MUST NOT increment retry_count (silent data loss otherwise)")

	// last_attempted_at MUST be set so the row is throttled out of the
	// next worker tick by the leeway window.
	var lastAttemptedAtIsSet bool
	require.NoError(t, db.Get(&lastAttemptedAtIsSet, "SELECT last_attempted_at IS NOT NULL FROM outbox_events WHERE id = 777"))
	assert.True(t, lastAttemptedAtIsSet, "unknown-address handling must set last_attempted_at")

	// The metric must have fired exactly once for this address.
	assert.Equal(t, []string{"address.not.in.book.v1"}, metrics.UnknownAddresses())

	// The row must still be in the table.
	var count int
	require.NoError(t, db.Get(&count, "SELECT COUNT(*) FROM outbox_events WHERE id = 777"))
	assert.Equal(t, 1, count, "unknown-address row must NOT be deleted — relay redeploy with updated book recovers it")
}

// TestProcessOne_UnknownAddress_RecoverableAfterBookUpdate confirms
// that once the address book is updated to know the previously-unknown
// address, the same row publishes normally on the next attempt.
func TestProcessOne_UnknownAddress_RecoverableAfterBookUpdate(t *testing.T) {
	db, testLogger := setupTest(t, "TestProcessOne_UnknownAddress_RecoverableAfterBookUpdate", "unknown_address.sql")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// First pass: book doesn't know the address.
	bookIncomplete, err := outbox.NewAddressBook(
		outbox.WithPublisher("p", fakePublisher{}),
		outbox.WithRoute("a.different.address", outbox.Route{Publisher: "p", Target: "t"}),
	)
	require.NoError(t, err)

	pub := fakePublisher{}
	o := &Relay{
		db:        db,
		logger:    &testLogger,
		workerCfg: &WorkerConfig{},
		book:      bookIncomplete,
		metrics:   noopMetrics{},
	}
	require.NoError(t, o.processOne(ctx, testLogger, 777))

	// Second pass: book now knows the address.
	bookUpdated, err := outbox.NewAddressBook(
		outbox.WithPublisher("p", pub),
		outbox.WithRoute("address.not.in.book.v1", outbox.Route{Publisher: "p", Target: "recovered-target"}),
	)
	require.NoError(t, err)
	o.book = bookUpdated

	// Need to bypass the leeway to re-process the row. Forcibly clear
	// last_attempted_at to simulate the next poll cycle picking it up.
	_, err = db.Exec("UPDATE outbox_events SET last_attempted_at = NULL WHERE id = 777")
	require.NoError(t, err)

	require.NoError(t, o.processOne(ctx, testLogger, 777))

	// Row should now be deleted — published successfully.
	var count int
	require.NoError(t, db.Get(&count, "SELECT COUNT(*) FROM outbox_events WHERE id = 777"))
	assert.Equal(t, 0, count, "after book update, the row publishes and is deleted")
}

// TestSinglePublisherAddressBook_Resolve_PassesAddressAsTarget exercises
// the v0.1 migration aid: SinglePublisherAddressBook routes every
// address straight to the supplied publisher with target=address.
func TestSinglePublisherAddressBook_Resolve_PassesAddressAsTarget(t *testing.T) {
	pub := fakePublisher{}
	book := outbox.SinglePublisherAddressBook(pub)

	gotPub, target, err := book.Resolve("any.address.you.like")
	require.NoError(t, err)
	assert.Equal(t, pub, gotPub)
	assert.Equal(t, "any.address.you.like", target)
}

func TestSinglePublisherAddressBook_HasAlwaysTrue(t *testing.T) {
	book := outbox.SinglePublisherAddressBook(fakePublisher{})
	assert.True(t, book.Has("literally.anything"))
	assert.True(t, book.Has(""))
}

func TestSinglePublisherAddressBook_ValidateAlwaysNil(t *testing.T) {
	book := outbox.SinglePublisherAddressBook(fakePublisher{})
	assert.NoError(t, book.Validate("literally.anything"))
}

// TestSetMetrics_NilRestoresNoop covers the contract that SetMetrics(nil)
// restores the noop default rather than panicking later.
func TestSetMetrics_NilRestoresNoop(t *testing.T) {
	o := &Relay{metrics: &countingMetrics{}}
	o.SetMetrics(nil)
	require.NotNil(t, o.metrics, "SetMetrics(nil) must install a non-nil noop metrics impl")
	// Calling a method shouldn't panic.
	o.metrics.IncUnknownAddress("anything")
}
