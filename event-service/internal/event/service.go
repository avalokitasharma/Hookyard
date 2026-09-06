package event

import (
	"context"

	"github.com/google/uuid"
)

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{
		repo: repo,
	}
}

func (s *Service) Publish(ctx context.Context, tenantID uuid.UUID, req PublishRequest) (*Event, error) {

	if req.Type == "" {
		return nil, ErrInvalidRequest
	}

	if req.IdempotencyKey == "" {
		return nil, ErrInvalidRequest
	}

	if len(req.Data) == 0 {
		return nil, ErrInvalidRequest
	}

	return s.repo.CreateEvent(ctx, req, tenantID)
}

func (s *Service) Get(ctx context.Context, tenantID uuid.UUID, id uuid.UUID) (*Event, error) {
	return s.repo.GetByID(ctx, tenantID, id)
}
