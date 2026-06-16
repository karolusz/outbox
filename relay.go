// Package outbox implements the Transactional Outbox pattern: events are
// persisted in the same transaction as the domain write that produces
// them, and a separate relay process publishes them at-least-once to a
// broker.
//
// This file defines the relay engine and the Publisher contract every
// broker plugin satisfies.
package outbox

import (
	"context"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog"
)

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

// Metrics is the relay's optional observability hook. Adopters wire it to
// whatever metrics stack they use (Prometheus, OpenTelemetry, expvar);
// the lib stays metrics-agnostic. A noop default is used when no Metrics
// is set via SetMetrics.
//
// New methods will be added to this interface as new metrics are wired
// into the relay. While the lib is at v0.x, adopters implementing Metrics
// should expect breaking additions; embed a noopMetrics into custom
// implementations to receive default no-op behaviour for newly-added
// methods.
type Metrics interface {
	// IncUnknownAddress is called when the worker resolves a row whose
	// Address is not registered in the address book. The address string
	// is passed so adopters can label / partition the metric.
	IncUnknownAddress(address string)
}

// noopMetrics is the default Metrics implementation. Drops every event
// on the floor. Adopters who don't care about metrics get this for free.
type noopMetrics struct{}

func (noopMetrics) IncUnknownAddress(address string) {}

type WorkerConfig struct {
	WorkerCount       int           // number of worker goroutines
	QueueSize         int           // size of the job queue channel
	BatchSize         int           // number of messages to fetch from DB in one go
	TickPeriod        time.Duration // interval between DB reads
	LeewayDurationSec int           // seconds inbetween delivery attempts
}

type OutboxRelay struct {
	ctx       context.Context
	db        *sqlx.DB
	dbSchema  string
	logger    *zerolog.Logger
	book      *AddressBook
	metrics   Metrics
	workerCfg *WorkerConfig
}

// NewOutboxRelay constructs the relay with the given DB, logger, address
// book, and worker config. Metrics default to a no-op implementation;
// override via SetMetrics if you want them wired to your observability
// stack.
//
// The book must be non-nil. Adopters with a single publisher who want
// v0.1-style "address = broker target" semantics should pass
// SinglePublisherAddressBook(pub).
func NewOutboxRelay(db *sqlx.DB, logger *zerolog.Logger, book *AddressBook, workerConfig *WorkerConfig) OutboxRelay {
	if workerConfig == nil {
		workerConfig = &WorkerConfig{
			WorkerCount:       8,
			QueueSize:         500,
			BatchSize:         200,
			TickPeriod:        2 * time.Second,
			LeewayDurationSec: 5,
		}
	}
	return OutboxRelay{
		db:        db,
		logger:    logger,
		book:      book,
		metrics:   noopMetrics{},
		workerCfg: workerConfig,
	}
}

// SetMetrics installs a Metrics implementation for the relay. Call before
// Start. Passing nil restores the default no-op metrics.
func (o *OutboxRelay) SetMetrics(m Metrics) {
	if m == nil {
		o.metrics = noopMetrics{}
		return
	}
	o.metrics = m
}

// Start runs the relay.
// It returns a channel that will be closed when the relay has completely stopped.
// It periodically polls the database for new outbox events and dispatches them
// to a pool of worker goroutines via a job queue. processOne converts panics
// inside it into errors so worker goroutines never die mid-loop; a panic that
// escapes that scope (e.g. in the producer or relay setup) crashes the process
// and defers recovery to whatever runs the binary (typically k8s).
func (o *OutboxRelay) Start(ctx context.Context, heartbeatFn func() error) <-chan struct{} {
	completeChan := make(chan struct{})
	go func() {
		defer close(completeChan)
		relayCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		// create a work queue
		queue := make(chan int64, o.workerCfg.QueueSize)
		var wg sync.WaitGroup

		// start workers — long-lived; each recovers panics inline.
		for idx := range o.workerCfg.WorkerCount {
			wg.Add(1)
			go func() {
				defer wg.Done()
				o.worker(relayCtx, idx, queue)
			}()
		}
		// Start producer (ticker) — runs until context is done.
		producerDone := make(chan struct{})
		go func() {
			defer close(producerDone)
			o.eventProcessor(relayCtx, queue, heartbeatFn)
		}()

		// Wait for context cancellation or for the producer to exit.
		// On either signal we cancel the relay context, wait for the producer
		// to finish, then close the queue so workers drain to a natural exit.
		select {
		case <-relayCtx.Done():
			o.logger.Debug().Msg("OutboxRelay: context canceled, shutting down")
		case <-producerDone:
			o.logger.Debug().Msg("OutboxRelay: producer exited, shutting down")
			cancel()
		}

		<-producerDone
		close(queue)

		wg.Wait() // wait for all workers to finish
		o.logger.Debug().Msg("OutboxRelay: all workers stopped, exiting")
	}()
	return completeChan
}
