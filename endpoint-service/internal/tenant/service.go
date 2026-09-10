package tenant

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

type Service struct{ repo *Repository }

func NewService(repo *Repository) *Service { return &Service{repo: repo} }

func (s *Service) Create(ctx context.Context, req CreateRequest) (Tenant, error) {
	req.Name = strings.TrimSpace(req.Name)
	if req.Name == "" {
		return Tenant{}, fmt.Errorf("%w: name is required", ErrInvalid)
	}
	if len(req.Name) > 255 {
		return Tenant{}, fmt.Errorf("%w: name must be at most 255 characters", ErrInvalid)
	}
	return s.repo.Create(ctx, req.Name)
}

func (s *Service) Get(ctx context.Context, id uuid.UUID) (Tenant, error) {
	if err := validateID(id); err != nil {
		return Tenant{}, err
	}
	return s.repo.Get(ctx, id)
}

func (s *Service) Patch(ctx context.Context, id uuid.UUID, req PatchRequest) (Tenant, error) {
	if err := validateID(id); err != nil {
		return Tenant{}, err
	}
	if req.Name != nil {
		name := strings.TrimSpace(*req.Name)
		if name == "" {
			return Tenant{}, fmt.Errorf("%w: name cannot be empty", ErrInvalid)
		}
		if len(name) > 255 {
			return Tenant{}, fmt.Errorf("%w: name must be at most 255 characters", ErrInvalid)
		}
		req.Name = &name
	}
	return s.repo.Patch(ctx, id, req)
}

func (s *Service) Delete(ctx context.Context, id uuid.UUID) (Tenant, error) {
	if err := validateID(id); err != nil {
		return Tenant{}, err
	}
	return s.repo.Delete(ctx, id)
}

func (s *Service) EnsureActive(ctx context.Context, id uuid.UUID) error {
	if err := validateID(id); err != nil {
		return err
	}
	return s.repo.EnsureActive(ctx, id)
}

func validateID(id uuid.UUID) error {
	if id == uuid.Nil {
		return fmt.Errorf("%w: invalid tenant id", ErrInvalid)
	}
	return nil
}
