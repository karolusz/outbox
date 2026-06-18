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
// # Implementation requirements
//
// Implementations of Publish MUST:
//
//   - Be safe for concurrent calls. The relay shares a single Publisher
//     instance across all worker goroutines; many in-flight calls may
//     overlap.
//
//   - Honor ctx cancellation. The relay derives a child ctx with a
//     deadline (relay.WorkerConfig.PublishTimeout, default 30s) for
//     every Publish call. Implementations MUST return promptly when
//     ctx.Done() fires, returning ctx.Err() unwrapped or wrapped with
//     additional broker context.
//
//     Broker SDKs that accept ctx natively (e.g. cloud.google.com/go/pubsub
//     via result.Get(ctx), franz-go for Kafka, AWS SDK v2) handle this
//     automatically. SDKs that don't (e.g. sarama, Eclipse Paho MQTT,
//     amqp091-go) require an internal wrapper inside the plugin:
//
//     done := make(chan publishResult, 1)
//     go func() { done <- doActualPublish(msg) }()
//     select {
//     case <-ctx.Done():
//     return ctx.Err()  // best-effort; abandons the in-flight goroutine
//     case r := <-done:
//     return r.err
//     }
//
//     A plugin that ignores ctx will block the relay's worker (and hold
//     a DB transaction open) for as long as the broker takes, defeating
//     the worker-level timeout entirely.
//
//   - Return broker errors verbatim on failure. The relay performs no
//     error classification in v0; every error is treated as a publish
//     failure that increments retry_count and re-attempts on the next
//     poll cycle. (Exception: a returned ctx.Canceled where the relay's
//     parent ctx is also Canceled is recognised as graceful shutdown
//     and the row is NOT marked as a failure.)
//
// # target
//
// target is the broker-specific destination name resolved by the address
// book from msg.Address (e.g. a Pub/Sub topic, a Kafka topic, an SQS
// queue ARN). The semantics of target are publisher-defined; the relay
// treats it as an opaque string.
//
// # Close
//
// Close releases any resources the publisher holds — broker connections,
// background batching goroutines, network sockets. The relay calls Close
// once per Publisher at shutdown after all workers have drained. Plugins
// with nothing to release return nil. Implementations SHOULD honor ctx
// for the close operation but MAY ignore it if their underlying SDK's
// Close call does not accept one.
type Publisher interface {
	Publish(ctx context.Context, target string, msg *Message) error
	Close(ctx context.Context) error
}
