// Package gcppubsub adapts cloud.google.com/go/pubsub as an outbox Publisher.
//
// One Publisher holds one GCP Pub/Sub client and resolves the
// outbox.Message.Destination as the topic name. Ordering is honoured via
// the Pub/Sub OrderingKey when the topic has EnableMessageOrdering set.
package gcppubsub

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"

	"github.com/karolusz/outbox"
)

// Publisher publishes outbox Messages to GCP Pub/Sub topics.
//
// It satisfies the outbox.Publisher interface (`Publish(ctx, *Message) error`).
type Publisher struct {
	client *pubsub.Client
}

// New constructs a Publisher with a fresh Pub/Sub client for projectID.
// The caller is responsible for ensuring credentials (ADC, key file, etc.)
// are available to the Pub/Sub SDK.
func New(ctx context.Context, projectID string) (*Publisher, error) {
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("pubsub: new client: %w", err)
	}
	return &Publisher{client: client}, nil
}

// NewWithClient wraps an existing *pubsub.Client. Useful for tests with the
// Pub/Sub emulator or for callers who want to share a client across multiple
// publishers.
func NewWithClient(client *pubsub.Client) *Publisher {
	return &Publisher{client: client}
}

// Publish sends msg to the Pub/Sub topic named in msg.Destination and blocks
// until the broker acks (or returns an error). The full broker error is
// returned to the relay verbatim — there is no error-classification logic
// in v0.
func (p *Publisher) Publish(ctx context.Context, msg *outbox.Message) error {
	if msg.Destination == "" {
		return fmt.Errorf("pubsub: empty destination on message id=%d", msg.ID)
	}

	topic := p.client.Topic(msg.Destination)
	result := topic.Publish(ctx, &pubsub.Message{
		Data:        msg.Data,
		Attributes:  msg.Attributes,
		OrderingKey: msg.OrderingKey,
	})
	_, err := result.Get(ctx)
	return err
}

// Close releases the underlying Pub/Sub client. Idempotent across multiple
// calls; safe to defer.
func (p *Publisher) Close() error {
	return p.client.Close()
}
