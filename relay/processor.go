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
				select {
				case queue <- pendingID:
					// enqueued successfully
				case <-ctx.Done():
					return
					//	NOTE: We elect for the eventProcessor to get blocked if the event ID queue is full
					// If required, we can change this to issue a warning and skip the event ID or
					// implement some worker scaling, if it becomes a regular occurence
					/*
						default:
							o.logger.Warn().Msg("eventProducer: queue is full, skipping event ID enqueue")
							break enqueueLoop
					*/
				}
			}
		}
	}
}
