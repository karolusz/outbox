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

// Publisher records every message handed to Publish, partitioned by the
// resolved broker target. Optional channels broadcast successful and failed
// publishes for assertion-driven tests; an optional ForceErrorFn injects a
// per-message failure decision.
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

	mu                sync.Mutex
	messagesPerTarget map[string][]*outbox.Message
	closed            bool
}

// New returns an empty Publisher; configure the public fields directly.
func New() *Publisher {
	return &Publisher{messagesPerTarget: make(map[string][]*outbox.Message)}
}

// Publish records msg under target or simulates a failure per ForceErrorFn.
// Safe for concurrent use across multiple workers.
func (p *Publisher) Publish(ctx context.Context, target string, msg *outbox.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.Logger != nil {
		p.Logger.Debug().
			Int64("event_id", msg.ID).
			Str("address", msg.Address).
			Str("target", target).
			Msg("fake publish")
	}

	if p.ForceErrorFn != nil {
		if err := p.ForceErrorFn(msg); err != nil {
			if p.FailedChan != nil {
				p.FailedChan <- msg
			}
			return err
		}
	}

	if p.messagesPerTarget == nil {
		p.messagesPerTarget = make(map[string][]*outbox.Message)
	}
	p.messagesPerTarget[target] = append(p.messagesPerTarget[target], msg)

	if p.BroadcastChan != nil {
		p.BroadcastChan <- msg
	}
	return nil
}

// Close marks the publisher closed. Records the call so tests can assert
// that the relay shutdown path invoked it; otherwise a no-op.
func (p *Publisher) Close(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	return nil
}

// Closed reports whether Close has been called. Useful for tests verifying
// the relay's shutdown path invokes the Publisher's Close.
func (p *Publisher) Closed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

// PublishedCount returns how many messages were recorded against the given
// target. Pass "" for the total across all targets.
func (p *Publisher) PublishedCount(target string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	if target == "" {
		n := 0
		for _, msgs := range p.messagesPerTarget {
			n += len(msgs)
		}
		return n
	}
	return len(p.messagesPerTarget[target])
}

// PublishedTo returns the recorded messages for the given target, or
// nil if none. The returned slice is a shallow copy; the messages themselves
// are not deep-copied.
func (p *Publisher) PublishedTo(target string) []*outbox.Message {
	p.mu.Lock()
	defer p.mu.Unlock()
	src := p.messagesPerTarget[target]
	if src == nil {
		return nil
	}
	out := make([]*outbox.Message, len(src))
	copy(out, src)
	return out
}
