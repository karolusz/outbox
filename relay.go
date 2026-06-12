// Package outbox is responsible for implementing the Outbox Pattern.
// The package:
// - Defines the Message model (the canonical struct your service uses for events).
// - Defines interfaces for persistence (Repository) and publishing (Publisher).
// - Implements the Relay (the background processor that moves events from persistance layer → publishing service).
// - Contains any shared constants/enums for statuses.
//
// Relay is not responsible for creating/saving events to the outbox table.
// Other services should use the repo to EventOutBox repo directly.
package outbox

import (
	"context"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog"
)

type Publisher interface {
	// Publish events and return successfully published ids
	Publish(ctx context.Context, event *Message) error
}

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
	pub       Publisher
	workerCfg *WorkerConfig
}

func NewOutboxRelay(db *sqlx.DB, logger *zerolog.Logger, pub Publisher, workerConfig *WorkerConfig) OutboxRelay {
	// use default config if none provided
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
		pub:       pub,
		workerCfg: workerConfig,
	}
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
