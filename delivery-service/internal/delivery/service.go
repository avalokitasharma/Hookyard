package delivery

import (
	"context"
	"fmt"

	"github.com/avalokitasharma/HookYard/delivery-service/internal/webhook"
	"github.com/google/uuid"
)

type EndpointReader interface {
	GetForEvent(context.Context, uuid.UUID, string) ([]EndpointSnapshotWithID, error)
}

type Service struct {
	repo      *Repository
	endpoints EndpointReader
	client    *webhook.Client
}

func NewService(repo *Repository, endpoints EndpointReader, client *webhook.Client) *Service {
	return &Service{repo: repo, endpoints: endpoints, client: client}
}

func (s *Service) HandleEventAccepted(ctx context.Context, e EventAccepted) error {
	if e.EventID == uuid.Nil || e.TenantID == uuid.Nil || e.EventType == "" {
		return fmt.Errorf("invalid EventAccepted")
	}
	if len(e.Payload) == 0 {
		return fmt.Errorf("EventAccepted missing payload")
	}

	endpoints, err := s.endpoints.GetForEvent(ctx, e.TenantID, e.EventType)
	if err != nil {
		return err
	}
	if len(endpoints) == 0 {
		return nil
	}

	return s.repo.CreateDeliveries(ctx, e, endpoints)
}
