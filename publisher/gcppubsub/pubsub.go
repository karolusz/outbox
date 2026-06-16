// Package gcppubsub adapts cloud.google.com/go/pubsub as an outbox Publisher.
//
// One Publisher holds one GCP Pub/Sub client. The relay resolves a row's
// logical address to a Pub/Sub topic name via the address book and hands
// the target topic name to Publish as a separate argument. The Publisher
// uses that target as the topic. Ordering keys are honoured via the
// Pub/Sub OrderingKey when the topic has EnableMessageOrdering set.
//
// Adopters wire this plugin two ways:
//
//  1. YAML: blank-import this package, reference plugin "gcppubsub" in
//     the address-book YAML. The factory below is invoked by the loader.
//  2. Go: call New or NewFromConfig directly and inject the Publisher
//     into the address book via outbox.WithPublisher.
package gcppubsub

import (
	"context"
	"fmt"

	"cloud.google.com/go/pubsub"
	"gopkg.in/yaml.v3"

	"github.com/karolusz/outbox"
)

// Publisher publishes outbox Messages to GCP Pub/Sub topics. Satisfies
// outbox.Publisher.
type Publisher struct {
	client *pubsub.Client
}

// New constructs a Publisher with a fresh Pub/Sub client for projectID
// using Application Default Credentials. Thin shorthand over NewFromConfig
// for adopters who only need to name the project.
func New(ctx context.Context, projectID string) (*Publisher, error) {
	return NewFromConfig(ctx, Config{Project: projectID})
}

// NewFromConfig constructs a Publisher from a fully-populated Config.
// Validates required fields and resolves credentials according to
// cfg.Credentials.
func NewFromConfig(ctx context.Context, cfg Config) (*Publisher, error) {
	if cfg.Project == "" {
		return nil, fmt.Errorf("gcppubsub: project is required")
	}
	opts, err := resolveCredentials(cfg.Credentials)
	if err != nil {
		return nil, err
	}
	client, err := pubsub.NewClient(ctx, cfg.Project, opts...)
	if err != nil {
		return nil, fmt.Errorf("gcppubsub: new client: %w", err)
	}
	return &Publisher{client: client}, nil
}

// NewWithClient wraps an existing *pubsub.Client. Useful for tests with the
// Pub/Sub emulator or for callers who want to share a client across multiple
// publishers.
func NewWithClient(client *pubsub.Client) *Publisher {
	return &Publisher{client: client}
}

// Publish sends msg to the Pub/Sub topic named in target and blocks until
// the broker acks (or returns an error). The full broker error is returned
// to the relay verbatim — there is no error-classification logic in v0.
func (p *Publisher) Publish(ctx context.Context, target string, msg *outbox.Message) error {
	if target == "" {
		return fmt.Errorf("pubsub: empty target for message id=%d (address=%q)", msg.ID, msg.Address)
	}

	topic := p.client.Topic(target)
	result := topic.Publish(ctx, &pubsub.Message{
		Data:        msg.Data,
		Attributes:  msg.Attributes,
		OrderingKey: msg.OrderingKey,
	})
	_, err := result.Get(ctx)
	return err
}

// Close releases the underlying Pub/Sub client. Idempotent across multiple
// calls; safe to defer. The ctx argument is accepted for interface
// compatibility; the GCP client's Close does not consult it.
func (p *Publisher) Close(ctx context.Context) error {
	return p.client.Close()
}

// init registers the gcppubsub plugin with the outbox registry. Adopters
// trigger this side effect by blank-importing the package:
//
//	import _ "github.com/karolusz/outbox/publisher/gcppubsub"
//
// After the import, the YAML loader (or any caller of outbox's plugin
// registry) can instantiate gcppubsub publishers by name.
func init() {
	outbox.RegisterPlugin("gcppubsub", func(ctx context.Context, raw []byte) (outbox.Publisher, error) {
		var cfg Config
		if err := yaml.Unmarshal(raw, &cfg); err != nil {
			return nil, fmt.Errorf("gcppubsub: parse config: %w", err)
		}
		return NewFromConfig(ctx, cfg)
	})
}
