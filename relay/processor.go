package relay

import (
	"context"
	"time"
)

// eventProcessor continuously fetches pending outbox event IDs at each
// tick and sends them to the worker queue. If the queue is full, the
// send blocks until a worker drains a slot (we elect to apply
// back-pressure rather than skip IDs; the alternative is documented as
// a sketch below if regular saturation becomes an issue).
func (o *Relay) eventProcessor(ctx context.Context, queue chan int64) {
	ticker := time.NewTicker(o.workerCfg.TickPeriod)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pendingIDs, err := getAllPendingEventIDs(o.db, o.dbSchema, o.workerCfg.BatchSize, o.workerCfg.LeewayDurationSec)
			if err != nil {
				o.logger.Warn().Err(err).Msg("eventProducer: failed to get pending outbox event IDs")
				continue
			}
			for _, pendingID := range pendingIDs {
				// Back-pressure: a full queue blocks the producer rather
				// than dropping IDs, so a slow publisher cannot silently
				// discard work.
				select {
				case queue <- pendingID:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}
