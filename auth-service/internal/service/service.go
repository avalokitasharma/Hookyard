package service

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/avalokitasharma/HookYard/auth-service/internal/model"
	"github.com/avalokitasharma/HookYard/auth-service/internal/repository"
	"github.com/google/uuid"
)

type Service struct{ repo *repository.Repository }

func New(r *repository.Repository) *Service { return &Service{repo: r} }
func (s *Service) CreateTenant(ctx context.Context, req model.CreateTenantRequest, actor string) (model.Tenant, model.CreatedKey, error) {
	t, err := s.repo.CreateTenant(ctx, req.Name)
	if err != nil {
		return t, model.CreatedKey{}, err
	}
	name := strings.TrimSpace(req.KeyName)
	if name == "" {
		name = "initial"
	}
	key, err := s.newKey(ctx, t.ID, model.CreateKeyRequest{Name: name}, actor)
	return t, key, err
}

func (s *Service) GetTenant(ctx context.Context, id uuid.UUID) (model.Tenant, error) {
	return s.repo.GetTenant(ctx, id)
}

func (s *Service) ListTenants(ctx context.Context) ([]model.Tenant, error) {
	return s.repo.ListTenants(ctx)
}

func (s *Service) UpdateTenant(ctx context.Context, id uuid.UUID, req model.UpdateTenantRequest) (model.Tenant, error) {
	return s.repo.UpdateTenant(ctx, id, req)
}

func (s *Service) DeleteTenant(ctx context.Context, id uuid.UUID) (model.Tenant, error) {
	return s.repo.DeleteTenant(ctx, id)
}

func (s *Service) CreateKey(ctx context.Context, tenantID uuid.UUID, req model.CreateKeyRequest, actor string) (model.CreatedKey, error) {
	return s.newKey(ctx, tenantID, req, actor)
}
func (s *Service) newKey(ctx context.Context, tenantID uuid.UUID, req model.CreateKeyRequest, actor string) (model.CreatedKey, error) {
	if len(req.Scopes) == 0 {
		req.Scopes = []string{"events:write", "events:read", "endpoints:manage", "deliveries:read", "deliveries:replay"}
	}
	allowed := map[string]bool{"events:write": true, "events:read": true, "endpoints:manage": true, "deliveries:read": true, "deliveries:replay": true}
	seen := map[string]bool{}
	for _, scope := range req.Scopes {
		if !allowed[scope] || seen[scope] {
			return model.CreatedKey{}, fmt.Errorf("invalid or duplicate scope %q", scope)
		}
		seen[scope] = true
	}
	if req.ExpiresAt != nil && !req.ExpiresAt.After(time.Now()) {
		return model.CreatedKey{}, fmt.Errorf("expires_at must be in the future")
	}
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		return model.CreatedKey{}, err
	}
	prefix := uuid.NewString()
	secret := "hk_live_" + prefix + "." + base64.RawURLEncoding.EncodeToString(random)
	k, err := s.repo.CreateKey(ctx, tenantID, req.Name, secret, prefix, req.Scopes, req.ExpiresAt, actor)
	if err != nil {
		return model.CreatedKey{}, err
	}
	return model.CreatedKey{APIKey: k, Secret: secret}, nil
}
func (s *Service) ListKeys(ctx context.Context, id uuid.UUID) ([]model.APIKey, error) {
	return s.repo.ListKeys(ctx, id)
}
func (s *Service) RevokeKey(ctx context.Context, tenantID, keyID uuid.UUID, actor string) error {
	return s.repo.RevokeKey(ctx, tenantID, keyID, actor)
}
func (s *Service) Verify(ctx context.Context, key string) (model.VerifyResponse, error) {
	return s.repo.Verify(ctx, key)
}
