package outbox

import (
	"context"
	"log/slog"
	"math"
	"time"
)

// relays events to kafka, runs at the inverval of 250ms to publish endpoint and subscription changes to kafka from the outbox table
// delivery service workers later consume it maintain up to date projection of endpoint data

type Publisher interface {
	Publish(context.Context, Message) error
}

type Relay struct {
	repo      *Repository
	publisher Publisher
	workerID  string
	batchSize int
	lease     time.Duration
	logger    *slog.Logger
}

func NewRelay(repo *Repository, p Publisher, workerID string, batchSize int, lease time.Duration, logger *slog.Logger) *Relay {
	return &Relay{
		repo:      repo,
		publisher: p,
		workerID:  workerID,
		batchSize: batchSize,
		lease:     lease,
		logger:    logger,
	}
}

func (r *Relay) Run(ctx context.Context) {
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.flush(ctx)
		}
	}
}

func (r *Relay) flush(ctx context.Context) {
	// get locks on pending event in outbox table
	msgs, err := r.repo.Claim(ctx, r.workerID, r.batchSize, r.lease)
	if err != nil {
		r.logger.Error("claim outbox", "err", err)
		return
	}
	for _, m := range msgs {
		// publish events to kafka
		err := r.publisher.Publish(ctx, m)
		if err != nil {
			// release lock to retry (after delay) if failed
			delay := retryDelay(m.Attempts)
			e := r.repo.Release(ctx, m.ID, r.workerID, delay)
			if e != nil {
				r.logger.Error("release outbox", "err", e, "message_id", m.ID)
			}
			r.logger.Error("publish outbox", "err", err, "message_id", m.ID, "retry_in", delay)
			continue
		}
		// release lock on the event if successfully published
		err = r.repo.MarkPublished(ctx, m.ID, r.workerID)
		if err != nil {
			r.logger.Error("mark outbox published", "err", err, "message_id", m.ID)
		}
	}
}

func retryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	seconds := math.Pow(2, float64(attempt-1))
	if seconds > 300 {
		seconds = 300
	}
	return time.Duration(seconds) * time.Second
}
