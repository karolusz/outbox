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

	// PublishTimeout caps how long the worker waits for a single
	// Publisher.Publish call. It is a safety net against hung publishers:
	// when it expires, the publish fails with context.DeadlineExceeded,
	// retry_count is incremented, and the row is re-attempted next tick.
	//
	// Zero or negative values are treated as "use the default" (30s).
	// The worker-level timeout is NOT opt-in — if it were, a misconfigured
	// adopter could disable the safety net by accident and hang workers
	// indefinitely on a bad broker.
	//
	// Plugins MAY apply their own (tighter) timeout internally; the
	// shorter deadline wins automatically because ctx chains compose
	// that way.
	PublishTimeout time.Duration

	// ShutdownTimeout caps how long the relay waits for AddressBook.Close
	// to flush publisher buffers after workers have stopped. Used by
	// Start to derive a fresh context.Background()+timeout for the
	// Close call — the parent ctx is already canceled by this point,
	// and propagating that cancellation would force broker SDKs to
	// abandon in-flight work instead of flushing.
	//
	// Zero or negative values are treated as "use the default" (30s).
	// Ignored when the relay was constructed with WithoutBookClose.
	ShutdownTimeout time.Duration
}

type Relay struct {
	ctx       context.Context
	db        *sqlx.DB
	dbSchema  string
	logger    *zerolog.Logger
	book      *outbox.AddressBook
	metrics   Metrics
	workerCfg *WorkerConfig

	// closeBook controls whether Start calls o.book.Close after workers
	// drain. Default true (the relay owns publisher lifetimes for the
	// duration of Start); flipped to false by WithoutBookClose for
	// adopters who want to reuse the book across Start cycles or share
	// it with a producer-side validator.
	closeBook bool
}

// Option configures a Relay at construction time. Passed to New as a
// variadic trailing arg.
type Option func(*Relay)

// WithDBSchema overrides the Postgres schema the relay uses for its
// tables. Default "outbox" (matching the migration's CREATE SCHEMA).
// Useful for adopters with name conflicts or unusual setups; most should
// leave it alone.
func WithDBSchema(name string) Option {
	return func(r *Relay) { r.dbSchema = name }
}

// WithoutBookClose tells the relay NOT to call AddressBook.Close after
// Start returns. Use this when the adopter retains ownership of the
// book — e.g. sharing it with a producer-side validator, reusing it
// across multiple Start cycles, or doing custom flush coordination.
//
// Default behavior (without this option): the relay closes the book on
// shutdown using a fresh context.Background()+ShutdownTimeout (default
// 30s), absorbing the subtle ctx-handling that adopters writing their
// own main would otherwise need to get right.
//
// Adopters who use WithoutBookClose are responsible for calling
// book.Close themselves at the appropriate point in their lifecycle.
func WithoutBookClose() Option {
	return func(r *Relay) { r.closeBook = false }
}

// New constructs the relay with the given DB, logger, address book, and
// worker config. Metrics default to a no-op implementation; override via
// SetMetrics if you want them wired to your observability stack.
//
// The book must be non-nil. Adopters with a single publisher who want
// v0.1-style "address = broker target" semantics should pass
// outbox.SinglePublisherAddressBook(pub).
//
// Optional arguments via Option (e.g. WithDBSchema) configure adopter-
// specific overrides; default values are sensible.
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
	// Normalize the safety-net timeouts. Both are non-opt-in (zero is
	// rewritten to the default rather than honored as "disable"). We
	// copy the struct so adopter-supplied configs aren't mutated under
	// them.
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
		metrics:   noopMetrics{},
		workerCfg: workerConfig,
		closeBook: true, // default: relay owns publisher lifetimes
	}
	for _, opt := range opts {
		opt(&r)
	}
	return r
}

// SetMetrics installs a Metrics implementation for the relay. Call before
// Start. Passing nil restores the default no-op metrics.
func (o *Relay) SetMetrics(m Metrics) {
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
		// opted out via WithoutBookClose. We derive a fresh ctx (NOT
		// the relay's parent ctx, which is already canceled by this
		// point) so broker SDKs that honor ctx don't immediately
		// abandon in-flight work — they get up to ShutdownTimeout to
		// flush. After ShutdownTimeout, the deadline fires and the
		// SDKs propagate that downward to whatever they were doing.
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
