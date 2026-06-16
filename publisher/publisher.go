// Package publisher defines the contract every broker plugin satisfies and
// the in-process registry used to look plugins up by name. It also owns the
// Message type that travels along that contract — the same row stored in
// the outbox table and handed to a Publisher by the relay.
//
// Plugin authors import only this package: the Publisher interface, the
// Message they receive, and the Register call they put in init(). The root
// outbox package re-exports Message as a type alias so producer code stays
// outbox.Message{...} without importing this package directly.
package publisher

import "context"

// Publisher is the contract every broker plugin satisfies.
//
// Publish is called by the relay's worker for each row. target is the
// broker-specific destination name (e.g. a Pub/Sub topic) resolved by the
// address book from msg.Address. msg is the full row for context
// (payload, ordering key, attributes, id). Implementations MUST be safe
// for concurrent calls — multiple workers share the same Publisher
// instance.
//
// Close releases any resources the publisher holds (broker connections,
// background batching goroutines, etc.). Called once at relay shutdown.
// Plugins with nothing to release return nil.
type Publisher interface {
	Publish(ctx context.Context, target string, msg *Message) error
	Close(ctx context.Context) error
}
