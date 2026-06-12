// Package fake provides an in-memory outbox Publisher for tests.
//
// Unlike a typical test double living under a //go:build testing tag, this
// publisher is intended to be importable from any test in any project, so
// it lives in a normal package.
package fake

import (
	"context"
	"sync"

	"github.com/rs/zerolog"

	"github.com/karolusz/outbox"
)

// Publisher records every message handed to Publish, partitioned by
// Destination. Optional channels broadcast successful and failed publishes
// for assertion-driven tests; an optional ForceErrorFn injects a per-message
// failure decision.
type Publisher struct {
	// Logger is optional. If nil, no log lines are emitted.
	Logger *zerolog.Logger

	// BroadcastChan, if non-nil, receives every successfully-published
	// Message. The test owns drain duties — an unbuffered channel will
	// block the publisher.
	BroadcastChan chan *outbox.Message

	// FailedChan, if non-nil, receives every Message whose Publish call
	// returned an error (because ForceErrorFn returned non-nil).
	FailedChan chan *outbox.Message

	// ForceErrorFn, if set, is consulted for every Publish call. A non-nil
	// return is treated as the Publish error and the Message is sent to
	// FailedChan (if set).
	ForceErrorFn func(msg *outbox.Message) error

	mu               sync.Mutex
	messagesPerTopic map[string][]*outbox.Message
}

// New returns an empty Publisher; configure the public fields directly.
func New() *Publisher {
	return &Publisher{messagesPerTopic: make(map[string][]*outbox.Message)}
}

// Publish records msg or simulates a failure per ForceErrorFn.
// Safe for concurrent use across multiple workers.
func (p *Publisher) Publish(ctx context.Context, msg *outbox.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.Logger != nil {
		p.Logger.Debug().Int64("event_id", msg.ID).Str("destination", msg.Destination).Msg("fake publish")
	}

	if p.ForceErrorFn != nil {
		if err := p.ForceErrorFn(msg); err != nil {
			if p.FailedChan != nil {
				p.FailedChan <- msg
			}
			return err
		}
	}

	if p.messagesPerTopic == nil {
		p.messagesPerTopic = make(map[string][]*outbox.Message)
	}
	p.messagesPerTopic[msg.Destination] = append(p.messagesPerTopic[msg.Destination], msg)

	if p.BroadcastChan != nil {
		p.BroadcastChan <- msg
	}
	return nil
}

// PublishedCount returns how many messages were recorded against the given
// destination. Pass "" for the total across all destinations.
func (p *Publisher) PublishedCount(destination string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	if destination == "" {
		n := 0
		for _, msgs := range p.messagesPerTopic {
			n += len(msgs)
		}
		return n
	}
	return len(p.messagesPerTopic[destination])
}

// PublishedTo returns the recorded messages for the given destination, or
// nil if none. The returned slice is a shallow copy; the messages themselves
// are not deep-copied.
func (p *Publisher) PublishedTo(destination string) []*outbox.Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	src := p.messagesPerTopic[destination]
	if src == nil {
		return nil
	}
	out := make([]*outbox.Message, len(src))
	copy(out, src)
	return out
}
