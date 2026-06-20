// Package relay implements the dispatch side of the transactional outbox:
// it polls the outbox table for pending rows, resolves each row's logical
// address through the address book, and hands the message to the resolved
// Publisher. The Publisher interface and the Message type live in
// github.com/karolusz/outbox/publisher; this package only consumes them.
package relay

import (
	"context"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog"

	"github.com/karolusz/outbox"
)

type WorkerConfig struct {
	WorkerCount       int           // number of worker goroutines
	QueueSize         int           // size of the job queue channel
	BatchSize         int           // number of messages to fetch from DB in one go
	TickPeriod        time.Duration // interval between DB reads
	LeewayDurationSec int           // seconds inbetween delivery attempts

	// PublishTimeout caps each Publisher.Publish call. On expiry the
	// publish fails with context.DeadlineExceeded, retry_count is
	// incremented, and the row is re-attempted on the next tick. Plugins
	// may apply their own tighter deadline; the shorter wins. Zero or
	// negative values fall back to the default (30s); the timeout is not
	// opt-in.
	PublishTimeout time.Duration

	// ShutdownTimeout caps how long Start waits for AddressBook.Close to
	// flush publisher buffers after workers have stopped. Derived from
	// context.Background() (not the relay's parent ctx, which is already
	// canceled by this point) so broker SDKs that honor ctx get a chance
	// to flush. Zero or negative falls back to the default (30s).
	// Ignored under WithoutBookClose.
	ShutdownTimeout time.Duration
}

type Relay struct {
	ctx       context.Context
	db        *sqlx.DB
	dbSchema  string
	logger    *zerolog.Logger
	book      *outbox.AddressBook
	workerCfg *WorkerConfig

	// closeBook controls whether Start calls o.book.Close after workers
	// drain. Default true; flipped to false by WithoutBookClose for
	// adopters who want to manage publisher lifetimes themselves.
	closeBook bool
}

// Option configures a Relay at construction time. Passed to New as a
// variadic trailing arg.
type Option func(*Relay)

// WithDBSchema overrides the Postgres schema the relay uses for its
// tables. Default "outbox" (matching the migration's CREATE SCHEMA).
func WithDBSchema(name string) Option {
	return func(r *Relay) { r.dbSchema = name }
}

// WithoutBookClose suppresses the relay's automatic AddressBook.Close
// after Start returns. The adopter is then responsible for calling
// book.Close at the appropriate point in their lifecycle. Use when
// sharing the book with a producer-side validator or reusing it across
// multiple Start cycles.
func WithoutBookClose() Option {
	return func(r *Relay) { r.closeBook = false }
}

// New constructs the relay. The book must be non-nil and have at least
// one route registered. Optional Options (e.g. WithDBSchema) override
// defaults.
func New(db *sqlx.DB, logger *zerolog.Logger, book *outbox.AddressBook, workerConfig *WorkerConfig, opts ...Option) Relay {
	if workerConfig == nil {
		workerConfig = &WorkerConfig{
			WorkerCount:       8,
			QueueSize:         500,
			BatchSize:         200,
			TickPeriod:        2 * time.Second,
			LeewayDurationSec: 5,
			PublishTimeout:    30 * time.Second,
			ShutdownTimeout:   30 * time.Second,
		}
	}
	// Normalize the safety-net timeouts. Copy so the adopter's struct
	// is not mutated under them; zero or negative falls back to default.
	cfg := *workerConfig
	if cfg.PublishTimeout <= 0 {
		cfg.PublishTimeout = 30 * time.Second
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 30 * time.Second
	}
	workerConfig = &cfg
	r := Relay{
		db:        db,
		dbSchema:  "outbox",
		logger:    logger,
		book:      book,
		workerCfg: workerConfig,
		closeBook: true,
	}
	for _, opt := range opts {
		opt(&r)
	}
	return r
}

// Start runs the relay. Returns a channel closed when the relay has
// fully stopped. The relay polls the outbox table on a ticker and
// dispatches IDs to a worker pool. Panics inside the publish path are
// converted to errors so worker goroutines never die mid-loop; panics
// outside that scope crash the process.
func (o *Relay) Start(ctx context.Context) <-chan struct{} {
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
			o.eventProcessor(relayCtx, queue)
		}()

		// Wait for context cancellation or for the producer to exit.
		// On either signal we cancel the relay context, wait for the producer
		// to finish, then close the queue so workers drain to a natural exit.
		select {
		case <-relayCtx.Done():
			o.logger.Debug().Msg("relay: context canceled, shutting down")
		case <-producerDone:
			o.logger.Debug().Msg("relay: producer exited, shutting down")
			cancel()
		}

		<-producerDone
		close(queue)

		wg.Wait() // wait for all workers to finish
		o.logger.Debug().Msg("relay: all workers stopped")

		// Flush publishers via AddressBook.Close unless the adopter
		// opted out. Derive a fresh ctx (the parent is already canceled
		// by this point) so SDKs that honor ctx don't abandon in-flight
		// work immediately.
		if o.closeBook && o.book != nil {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), o.workerCfg.ShutdownTimeout)
			defer closeCancel()
			if err := o.book.Close(closeCtx); err != nil {
				o.logger.Warn().Err(err).Msg("relay: address book close returned errors")
			}
		}
		o.logger.Debug().Msg("relay: exiting")
	}()
	return completeChan
}
