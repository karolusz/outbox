package outbox

import (
	"context"
	"time"
)

// eventProcessor continuously (every tick) fetches pending outbox event IDs and sends them to the queue for processing.
// if queue is full, skip and issue warning.
// if provided will call the heartbeat callback function (hearbeatFn)
func (o *OutboxRelay) eventProcessor(ctx context.Context, queue chan int64, hearbeatFn func() error) {
	ticker := time.NewTicker(o.workerCfg.TickPeriod)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// heartbeat if callback provided
			if hearbeatFn != nil {
				_ = hearbeatFn()
			}
			// retrireve pending event IDs
			pendingIDs, err := getAllPendingEventIDs(o.db, o.workerCfg.BatchSize, o.workerCfg.LeewayDurationSec)
			if err != nil {
				o.logger.Warn().Err(err).Msg("eventProducer: failed to get pending outbox event IDs")
				continue
			}
			for _, pendingID := range pendingIDs {
				// if you cant send to queue (full buffer) issue warning and break the loop
				select {
				case queue <- pendingID:
					// enqueued successfully
				case <-ctx.Done():
					// if context closes exit
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
