// Package publisher defines the contract every broker plugin satisfies
// and the in-process registry used to look plugins up by name. It also
// owns the Message type that travels along that contract — the same row
// stored in the outbox table and handed to a Publisher by the relay.
//
// Plugin authors import only this package: the Publisher interface, the
// Message they receive, and the Register call they put in init(). The
// root outbox package re-exports Message as a type alias so producer
// code stays outbox.Message{...} without importing this package
// directly.
package publisher

import "context"

// Publisher is the contract every broker plugin satisfies.
//
// # Publish
//
// target is the broker-specific destination name resolved by the
// address book from msg.Address (e.g. a Pub/Sub topic, a Kafka topic,
// an SQS queue ARN). Its semantics are publisher-defined; the relay
// treats it as an opaque string.
//
// Implementations of Publish MUST:
//
//   - Be safe for concurrent calls. The relay shares a single Publisher
//     across all worker goroutines.
//
//   - Honor ctx cancellation. The relay derives a child ctx with the
//     configured PublishTimeout for every call. SDKs that accept ctx
//     natively handle this for free; SDKs that don't need a select on
//     ctx.Done() around the call. A plugin that ignores ctx blocks the
//     worker and holds its DB tx open, defeating the safety net.
//
//   - Return broker errors verbatim. The relay does not classify
//     errors; every error increments retry_count and re-attempts on the
//     next poll cycle. (Exception: ctx.Canceled returned while the
//     relay's parent ctx is also Canceled is treated as graceful
//     shutdown, not a failure.)
//
// # Close
//
// Close releases publisher-held resources (broker connections,
// background goroutines, sockets). It is invoked by AddressBook.Close
// after the relay has stopped. Implementations MUST:
//
//   - Be idempotent. A second Close must not panic.
//
//   - Block until in-flight publishes have flushed (or the SDK has
//     given up). Otherwise the adopter has no signal that exiting is
//     safe.
//
//   - Honor ctx as best-effort. On ctx.Done(), return ctx.Err() and
//     abandon in-flight work. Some SDKs (Pub/Sub, MQTT) ignore the
//     deadline on Close; adopters accept that those may flush past it.
//
// Plugins with no resources to release return nil immediately.
type Publisher interface {
	Publish(ctx context.Context, target string, msg *Message) error
	Close(ctx context.Context) error
}
