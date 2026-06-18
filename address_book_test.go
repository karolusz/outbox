//go:build testing

package outbox

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubPublisher is a minimal Publisher implementation for testing the
// AddressBook in isolation. It records nothing and always succeeds.
type stubPublisher struct{ name string }

func (p stubPublisher) Publish(ctx context.Context, target string, msg *Message) error {
	return nil
}
func (p stubPublisher) Close(ctx context.Context) error { return nil }

func TestNewAddressBook_HappyPath(t *testing.T) {
	pub1 := stubPublisher{name: "p1"}
	pub2 := stubPublisher{name: "p2"}

	book, err := NewAddressBook(
		WithPublisher("primary", pub1),
		WithPublisher("secondary", pub2),
		WithRoute("event.a.v1", Route{Publisher: "primary", Target: "topic-a"}),
		WithRoute("event.b.v1", Route{Publisher: "primary", Target: "topic-b"}),
		WithRoute("event.c.v1", Route{Publisher: "secondary", Target: "topic-c"}),
	)
	require.NoError(t, err)
	require.NotNil(t, book)

	gotPub, gotTarget, err := book.Resolve("event.a.v1")
	require.NoError(t, err)
	require.Equal(t, pub1, gotPub)
	require.Equal(t, "topic-a", gotTarget)

	gotPub, gotTarget, err = book.Resolve("event.c.v1")
	require.NoError(t, err)
	require.Equal(t, pub2, gotPub)
	require.Equal(t, "topic-c", gotTarget)
}

func TestNewAddressBook_EmptyOptions_Errors(t *testing.T) {
	_, err := NewAddressBook()
	require.Error(t, err)
	assert.ErrorContains(t, err, "no routes registered")
}

func TestNewAddressBook_PublishersWithoutRoutes_Errors(t *testing.T) {
	_, err := NewAddressBook(WithPublisher("primary", stubPublisher{}))
	require.Error(t, err)
	assert.ErrorContains(t, err, "no routes registered")
}

func TestNewAddressBook_UnusedPublisher_Allowed(t *testing.T) {
	// A publisher registered without any route using it is permitted.
	// The adopter might be staging it for a future route in the next deploy.
	book, err := NewAddressBook(
		WithPublisher("primary", stubPublisher{name: "p1"}),
		WithPublisher("staged", stubPublisher{name: "p2"}),
		WithRoute("event.a.v1", Route{Publisher: "primary", Target: "topic-a"}),
	)
	require.NoError(t, err)
	require.NotNil(t, book)
}

func TestNewAddressBook_DuplicateRoute_Errors(t *testing.T) {
	_, err := NewAddressBook(
		WithPublisher("p", stubPublisher{}),
		WithRoute("event.a.v1", Route{Publisher: "p", Target: "t1"}),
		WithRoute("event.a.v1", Route{Publisher: "p", Target: "t2"}),
	)
	require.Error(t, err)
	assert.ErrorContains(t, err, `address "event.a.v1" registered 2 times`)
}

func TestNewAddressBook_DuplicatePublisher_Errors(t *testing.T) {
	_, err := NewAddressBook(
		WithPublisher("p", stubPublisher{}),
		WithPublisher("p", stubPublisher{}),
		WithRoute("event.a.v1", Route{Publisher: "p", Target: "t1"}),
	)
	require.Error(t, err)
	assert.ErrorContains(t, err, `publisher key "p" registered 2 times`)
}

func TestNewAddressBook_RouteToUnknownPublisher_Errors(t *testing.T) {
	_, err := NewAddressBook(
		WithPublisher("known", stubPublisher{}),
		WithRoute("event.a.v1", Route{Publisher: "missing", Target: "t1"}),
	)
	require.Error(t, err)
	assert.ErrorContains(t, err, `route "event.a.v1" references unregistered publisher "missing"`)
}

func TestNewAddressBook_RouteWithEmptyPublisher_Errors(t *testing.T) {
	_, err := NewAddressBook(
		WithPublisher("p", stubPublisher{}),
		WithRoute("event.a.v1", Route{Publisher: "", Target: "t1"}),
	)
	require.Error(t, err)
	assert.ErrorContains(t, err, `route "event.a.v1" has empty Publisher reference`)
}

func TestNewAddressBook_RouteWithEmptyTarget_Errors(t *testing.T) {
	_, err := NewAddressBook(
		WithPublisher("p", stubPublisher{}),
		WithRoute("event.a.v1", Route{Publisher: "p", Target: ""}),
	)
	require.Error(t, err)
	assert.ErrorContains(t, err, `route "event.a.v1" has empty Target`)
}

func TestNewAddressBook_AggregatesAllErrors(t *testing.T) {
	// Multiple problems are reported in a single error so the adopter can
	// fix them all in one pass.
	_, err := NewAddressBook(
		WithPublisher("p", stubPublisher{}),
		WithPublisher("p", stubPublisher{}), // duplicate publisher
		WithRoute("event.a.v1", Route{Publisher: "p", Target: "t1"}),
		WithRoute("event.a.v1", Route{Publisher: "p", Target: "t2"}),       // duplicate route
		WithRoute("event.b.v1", Route{Publisher: "missing", Target: "t3"}), // unknown publisher
		WithRoute("event.c.v1", Route{Publisher: "p", Target: ""}),         // empty target
	)
	require.Error(t, err)

	msg := err.Error()
	assert.Contains(t, msg, `publisher key "p" registered 2 times`)
	assert.Contains(t, msg, `address "event.a.v1" registered 2 times`)
	assert.Contains(t, msg, `route "event.b.v1" references unregistered publisher "missing"`)
	assert.Contains(t, msg, `route "event.c.v1" has empty Target`)
}

func TestAddressBook_Resolve_Hit(t *testing.T) {
	pub := stubPublisher{name: "test"}
	book, err := NewAddressBook(
		WithPublisher("p", pub),
		WithRoute("addr", Route{Publisher: "p", Target: "tgt"}),
	)
	require.NoError(t, err)

	gotPub, gotTarget, err := book.Resolve("addr")
	require.NoError(t, err)
	require.Equal(t, pub, gotPub)
	require.Equal(t, "tgt", gotTarget)
}

func TestAddressBook_Resolve_Miss_WrapsErrUnknownAddress(t *testing.T) {
	book, err := NewAddressBook(
		WithPublisher("p", stubPublisher{}),
		WithRoute("known", Route{Publisher: "p", Target: "tgt"}),
	)
	require.NoError(t, err)

	_, _, err = book.Resolve("unknown")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrUnknownAddress), "Resolve should wrap ErrUnknownAddress; got: %v", err)
	assert.ErrorContains(t, err, "unknown")
}

func TestAddressBook_Has(t *testing.T) {
	book, err := NewAddressBook(
		WithPublisher("p", stubPublisher{}),
		WithRoute("known", Route{Publisher: "p", Target: "tgt"}),
	)
	require.NoError(t, err)

	assert.True(t, book.Has("known"))
	assert.False(t, book.Has("unknown"))
	assert.False(t, book.Has(""))
}

func TestAddressBook_Validate(t *testing.T) {
	book, err := NewAddressBook(
		WithPublisher("p", stubPublisher{}),
		WithRoute("known", Route{Publisher: "p", Target: "tgt"}),
	)
	require.NoError(t, err)

	require.NoError(t, book.Validate("known"))

	err = book.Validate("unknown")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrUnknownAddress))
}

// TestAddressBook_MultipleRoutesShareOnePublisher exercises the design
// point that multiple addresses can share a single publisher instance —
// the connection pool / batching behaviour lives in the publisher, and
// addresses are just routing labels in front of it.
func TestAddressBook_MultipleRoutesShareOnePublisher(t *testing.T) {
	shared := stubPublisher{name: "shared"}
	book, err := NewAddressBook(
		WithPublisher("gcp-prod", shared),
		WithRoute("event.a.v1", Route{Publisher: "gcp-prod", Target: "topic-a"}),
		WithRoute("event.b.v1", Route{Publisher: "gcp-prod", Target: "topic-b"}),
		WithRoute("event.c.v1", Route{Publisher: "gcp-prod", Target: "topic-c"}),
	)
	require.NoError(t, err)

	pubA, _, _ := book.Resolve("event.a.v1")
	pubB, _, _ := book.Resolve("event.b.v1")
	pubC, _, _ := book.Resolve("event.c.v1")
	assert.Equal(t, pubA, pubB)
	assert.Equal(t, pubB, pubC)
}

// closingPublisher is a stub that records each Close call and can be
// configured to return an error. Pointer-based so the counter is
// shared across copies passed by value through WithPublisher.
type closingPublisher struct {
	name     string
	closes   int
	closeErr error
}

func (p *closingPublisher) Publish(ctx context.Context, target string, msg *Message) error {
	return nil
}
func (p *closingPublisher) Close(ctx context.Context) error {
	p.closes++
	return p.closeErr
}

// TestAddressBook_Close_ClosesEveryPublisher verifies that AddressBook.Close
// calls Close on every Publisher registered via WithPublisher.
func TestAddressBook_Close_ClosesEveryPublisher(t *testing.T) {
	pubA := &closingPublisher{name: "a"}
	pubB := &closingPublisher{name: "b"}
	pubC := &closingPublisher{name: "c"}

	book, err := NewAddressBook(
		WithPublisher("a", pubA),
		WithPublisher("b", pubB),
		WithPublisher("c", pubC),
		WithRoute("event.a.v1", Route{Publisher: "a", Target: "topic-a"}),
		WithRoute("event.b.v1", Route{Publisher: "b", Target: "topic-b"}),
		WithRoute("event.c.v1", Route{Publisher: "c", Target: "topic-c"}),
	)
	require.NoError(t, err)

	require.NoError(t, book.Close(context.Background()))

	assert.Equal(t, 1, pubA.closes, "publisher a should have been closed exactly once")
	assert.Equal(t, 1, pubB.closes, "publisher b should have been closed exactly once")
	assert.Equal(t, 1, pubC.closes, "publisher c should have been closed exactly once")
}

// TestAddressBook_Close_JoinsErrors verifies that errors from individual
// publishers are aggregated via errors.Join and that Close attempts every
// publisher even if earlier ones errored.
func TestAddressBook_Close_JoinsErrors(t *testing.T) {
	pubA := &closingPublisher{name: "a", closeErr: errors.New("a went boom")}
	pubB := &closingPublisher{name: "b"} // succeeds
	pubC := &closingPublisher{name: "c", closeErr: errors.New("c went boom")}

	book, err := NewAddressBook(
		WithPublisher("a", pubA),
		WithPublisher("b", pubB),
		WithPublisher("c", pubC),
		WithRoute("event.x.v1", Route{Publisher: "a", Target: "topic-a"}),
		WithRoute("event.y.v1", Route{Publisher: "b", Target: "topic-b"}),
		WithRoute("event.z.v1", Route{Publisher: "c", Target: "topic-c"}),
	)
	require.NoError(t, err)

	err = book.Close(context.Background())
	require.Error(t, err)

	// Each publisher should have been attempted even though some errored.
	assert.Equal(t, 1, pubA.closes)
	assert.Equal(t, 1, pubB.closes)
	assert.Equal(t, 1, pubC.closes)

	// Both error messages should be present in the joined error.
	assert.Contains(t, err.Error(), `close publisher "a"`)
	assert.Contains(t, err.Error(), "a went boom")
	assert.Contains(t, err.Error(), `close publisher "c"`)
	assert.Contains(t, err.Error(), "c went boom")
}

// TestSinglePublisherAddressBook_Close closes the passthrough publisher.
func TestSinglePublisherAddressBook_Close(t *testing.T) {
	pub := &closingPublisher{name: "single"}
	book := SinglePublisherAddressBook(pub)

	require.NoError(t, book.Close(context.Background()))

	assert.Equal(t, 1, pub.closes, "passthrough publisher should have been closed exactly once")
}

// TestAddressBook_Close_EmptyBookNoError covers the degenerate case where
// the book is constructed with no publishers (would fail NewAddressBook
// validation today, but the Close method should still be safe to call on
// a zero AddressBook).
func TestAddressBook_Close_EmptyBookNoError(t *testing.T) {
	book := &AddressBook{}
	require.NoError(t, book.Close(context.Background()))
}

// TestAddressBook_Close_Idempotent verifies that calling Close twice does
// not panic; the second call invokes Close on each publisher again, and
// since stub publishers tolerate multiple Closes, both calls succeed.
// This documents the AddressBook side of the idempotency expectation —
// the per-Publisher idempotency requirement lives in the Publisher
// interface contract.
func TestAddressBook_Close_Idempotent(t *testing.T) {
	pub := &closingPublisher{name: "a"}

	book, err := NewAddressBook(
		WithPublisher("a", pub),
		WithRoute("event.a.v1", Route{Publisher: "a", Target: "topic-a"}),
	)
	require.NoError(t, err)

	require.NoError(t, book.Close(context.Background()))
	require.NoError(t, book.Close(context.Background()))

	assert.Equal(t, 2, pub.closes, "Close was called twice on the book, so per-publisher Close should also have been invoked twice")
}
