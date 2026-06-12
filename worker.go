package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/rs/zerolog"
)

var (
	ErrMaxRetry = errors.New("max retry limit reached")
	// ErrPanic wraps a recovered panic so the worker can distinguish it from a
	// regular publish failure. A regular failure already increments retry_count
	// inside processOne's transaction; a panic does not, so the worker must mark
	// the row separately via markPanickedDeliveryAttempt.
	ErrPanic = errors.New("panic during processing")
)

// worker runs in a loop reading ids from queue and processing each id transactionally.
// Worker exits on either:
//   - the context being cancelled (the current id is dropped and will be
//     re-picked on the next poll)
//   - the queue being closed (normal shutdown; the relay closes the queue)
//
// processOne and markPanickedDeliveryAttempt convert any panic they encounter
// into an error via a named return + defer recover, so the worker goroutine
// never dies and always proceeds to the next id.
func (o *OutboxRelay) worker(ctx context.Context, idx int, queue <-chan int64) {
	logger := o.logger.With().Int("worker", idx).Logger()
	logger.Debug().Msg("worker started")

	for id := range queue {
		if ctx.Err() != nil {
			logger.Debug().Msg("context cancelled, exiting")
			return
		}
		err := o.processOne(ctx, logger, id)
		if err == nil {
			continue
		}
		if errors.Is(err, ErrPanic) {
			// processOne's tx is already rolled back via its defer by the time
			// the panic-wrapped error reaches us, so the row's lock is released
			// and a fresh tx in markPanickedDeliveryAttempt won't contend.
			if markErr := o.markPanickedDeliveryAttempt(ctx, id, err); markErr != nil {
				logger.Warn().Err(markErr).Int64("event_id", id).
					Msg("failed to record panicked delivery attempt; row will be retried next tick")
			}
			continue
		}
		logger.Warn().Err(err).Int64("event_id", id).Msg("event couldn't be published")
	}
	logger.Debug().Msg("queue closed, exiting")
}

// markPanickedDeliveryAttempt opens a new transaction and increments the
// retry_count for the row that caused a panic, so the row participates in the
// same retry/exhaustion logic as a normal publish failure and does not re-enter
// a tight panic loop.
//
// The mark is skipped (no-op, returns nil) in two cases:
//   - the row is locked by another worker — that worker will record the attempt
//   - the row is already at retry_limit — the cap is respected, no further
//     increments would do anything useful (the row would still be filtered out
//     of the poll query by the same predicate)
//
// We never block on a row lock here; tryIncrementRetryCount uses SKIP LOCKED to
// fail fast.
//
// The named return + defer recover make the function self-contained against
// any panic that occurs inside it (e.g. driver bug, OOM mid-call): the panic
// is converted to an error and returned to the caller. If marking fails, the
// row stays unchanged and is re-picked on the next poll cycle.
func (o *OutboxRelay) markPanickedDeliveryAttempt(ctx context.Context, id int64, cause error) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("panic while marking panicked attempt: %v", p)
		}
	}()

	tx, err := o.db.Beginx()
	if err != nil {
		return fmt.Errorf("begin tx for marking panicked attempt: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(); rbErr != nil && rbErr != sql.ErrTxDone {
			o.logger.Error().Err(rbErr).Int64("event_id", id).Msg("rollback failed during panic marking")
		}
	}()

	marked, err := tryIncrementRetryCount(tx, id)
	if err != nil {
		return fmt.Errorf("increment retry count for panicked event %d: %w", id, err)
	}
	if !marked {
		o.logger.Debug().Int64("event_id", id).
			Msg("panic mark skipped: row is locked by another worker or already at retry_limit")
		return nil
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit retry count for panicked event %d: %w", id, err)
	}
	o.logger.Error().Err(cause).Int64("event_id", id).
		Msg("recorded panicked delivery attempt; retry_count incremented")
	return nil
}

// processOne performs one delivery attempt for a single outbox event id.
//
// Defer ordering matters: the recover defer is registered first so it runs
// LAST, after tx.Rollback has fired and released the row lock. That way a
// panic-derived ErrPanic is observed by the caller only once the row is
// no longer locked, so the caller can safely open a fresh tx to mark the row.
//
// On panic: the named return is populated with ErrPanic wrapping the recovered
// value. retry_count is NOT touched here (the panic blew out before the
// publish-failure path could increment it); the caller marks the row.
//
// On regular publish failure: retry_count is incremented inside this tx and
// the original publish error is returned. The caller does NOT mark again.
func (o *OutboxRelay) processOne(ctx context.Context, logger zerolog.Logger, id int64) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("%w: %v", ErrPanic, p)
		}
	}()

	tx, err := o.db.Beginx()
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(); rbErr != nil && rbErr != sql.ErrTxDone {
			logger.Error().Err(rbErr).Msg("rollback failed")
		}
	}()

	logger.Debug().Int64("event_id", id).Msg("processing outbox event")
	outboxEvent, err := getEventByIDIfNotLocked(tx, id)
	if err == sql.ErrNoRows {
		logger.Debug().Int64("event_id", id).Msg("no event retrieved — already processed or locked elsewhere")
		return nil
	}
	if err != nil {
		return fmt.Errorf("get and lock outbox event: %w", err)
	}
	attemptNum := outboxEvent.RetryCount + 1
	logger = logger.With().Int("attempt", attemptNum).Int64("event_id", outboxEvent.ID).Logger()

	if publishErr := o.pub.Publish(ctx, outboxEvent); publishErr != nil {
		logger.Debug().Err(publishErr).Msg("failed to publish outbox event")
		outboxEvent.RetryCount++
		if updateErr := incrementRetryCount(tx, outboxEvent.ID); updateErr != nil {
			return fmt.Errorf("update retry count: %w (publishErr: %w)", updateErr, publishErr)
		}
		if commitErr := tx.Commit(); commitErr != nil {
			return fmt.Errorf("commit retry count update: %w (publishErr: %w)", commitErr, publishErr)
		}
		if outboxEvent.RetryCount >= outboxEvent.RetryLimit {
			logger.Error().Err(ErrMaxRetry).Msg("event reached max retry limit, consider manual intervention")
		}
		return publishErr
	}

	if deleteErr := deleteOneEvent(tx, id); deleteErr != nil {
		return fmt.Errorf("delete published event: %w", deleteErr)
	}
	if commitErr := tx.Commit(); commitErr != nil {
		return fmt.Errorf("commit deletion of published event: %w", commitErr)
	}
	logger.Debug().Msg("deleted event from db")
	return nil
}
