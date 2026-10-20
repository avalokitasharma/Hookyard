package scheduler

import (
	"context"
	"log/slog"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/avalokitasharma/HookYard/delivery-service/internal/delivery"
)

type Scheduler struct {
	repo        *delivery.Repository
	service     *delivery.Service
	concurrency int
	batchSize   int
	lease       time.Duration
	logger      *slog.Logger
}

func New(repo *delivery.Repository, service *delivery.Service, concurrency, batchSize int, lease time.Duration, logger *slog.Logger) *Scheduler {
	if concurrency <= 0 {
		concurrency = runtime.NumCPU() * 4
	}
	if batchSize <= 0 {
		batchSize = concurrency
	}
	return &Scheduler{repo: repo, service: service, concurrency: concurrency, batchSize: batchSize, lease: lease, logger: logger}
}

func (s *Scheduler) Run(ctx context.Context) {
	workerID, _ := os.Hostname()
	if workerID == "" {
		workerID = "delivery-worker"
	}

	sem := make(chan struct{}, s.concurrency)
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			available := s.concurrency - len(sem)
			if available <= 0 {
				continue
			}
			limit := s.batchSize
			if limit > available {
				limit = available
			}
			deliveries, err := s.repo.ClaimBatch(ctx, workerID, limit, s.lease)
			if err != nil {
				s.logger.Error("claim deliveries failed", "error", err)
				continue
			}
			for _, d := range deliveries {
				sem <- struct{}{}
				wg.Add(1)
				go func(d delivery.Delivery) {
					defer wg.Done()
					defer func() {
						<-sem
					}()
					err := s.service.Deliver(ctx, d)
					if err != nil {
						s.logger.Error("delivery failed", "delivery_id", d.ID, "error", err)
					}
				}(d)
			}

		}
	}
}
