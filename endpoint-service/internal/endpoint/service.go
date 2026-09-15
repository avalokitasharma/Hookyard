package endpoint

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/avalokitasharma/HookYard/endpoint-service/internal/retrypolicy"
	"github.com/avalokitasharma/HookYard/endpoint-service/internal/secrets"
	"github.com/avalokitasharma/HookYard/endpoint-service/internal/tenant"
	"github.com/avalokitasharma/HookYard/endpoint-service/internal/validation"
	"github.com/google/uuid"
)

type Service struct {
	repo      *Repository
	policies  *retrypolicy.Repository
	cipher    *secrets.Cipher
	tenants   *tenant.Repository
	allowHTTP bool
}

func NewService(repo *Repository, policies *retrypolicy.Repository, cipher *secrets.Cipher, tenants *tenant.Repository, allowHTTP bool) *Service {
	return &Service{
		repo:      repo,
		policies:  policies,
		cipher:    cipher,
		tenants:   tenants,
		allowHTTP: allowHTTP,
	}
}

func (s *Service) Create(ctx context.Context, tenantID uuid.UUID, req CreateRequest) (Endpoint, error) {
	if err := s.ensureActiveTenant(ctx, tenantID); err != nil {
		return Endpoint{}, err
	}
	if err := validateTenant(tenantID); err != nil {
		return Endpoint{}, err
	}
	normalizeCreate(&req)
	if err := validation.EndpointName(req.Name); err != nil {
		return Endpoint{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if err := validation.WebhookURL(req.URL, s.allowHTTP); err != nil {
		return Endpoint{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if req.Secret == "" || len(req.Secret) > 4096 {
		return Endpoint{}, fmt.Errorf("%w: secret must be between 1 and 4096 characters", ErrInvalid)
	}
	if err := validation.Timeout(req.ConnectTimeoutMS, "connect_timeout_ms"); err != nil {
		return Endpoint{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if err := validation.Timeout(req.RequestTimeoutMS, "request_timeout_ms"); err != nil {
		return Endpoint{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	types, err := validation.EventTypes(req.EventTypes)
	if err != nil {
		return Endpoint{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	req.EventTypes = types
	policy, err := s.resolvePolicy(ctx, tenantID, req.RetryPolicyID)
	if err != nil {
		return Endpoint{}, err
	}
	enc, err := s.cipher.Encrypt(req.Secret)
	if err != nil {
		return Endpoint{}, fmt.Errorf("encrypt endpoint secret: %w", err)
	}
	return s.repo.CreateEndpoint(ctx, tenantID, req, enc, policy)
}

func (s *Service) Get(ctx context.Context, tenantID, id uuid.UUID) (Endpoint, retrypolicy.Policy, error) {
	if err := s.ensureActiveTenant(ctx, tenantID); err != nil {
		return Endpoint{}, retrypolicy.Policy{}, err
	}
	return s.repo.GetEndpoint(ctx, tenantID, id)
}

func (s *Service) Patch(ctx context.Context, tenantID, id uuid.UUID, req PatchRequest) (Endpoint, retrypolicy.Policy, error) {
	if err := s.ensureActiveTenant(ctx, tenantID); err != nil {
		return Endpoint{}, retrypolicy.Policy{}, err
	}
	if err := validateTenant(tenantID); err != nil {
		return Endpoint{}, retrypolicy.Policy{}, err
	}
	var enc []byte
	if req.Name != nil {
		*req.Name = strings.TrimSpace(*req.Name)
		if err := validation.EndpointName(*req.Name); err != nil {
			return Endpoint{}, retrypolicy.Policy{}, fmt.Errorf("%w: %v", ErrInvalid, err)
		}
	}
	if req.URL != nil {
		*req.URL = strings.TrimSpace(*req.URL)
		if err := validation.WebhookURL(*req.URL, s.allowHTTP); err != nil {
			return Endpoint{}, retrypolicy.Policy{}, fmt.Errorf("%w: %v", ErrInvalid, err)
		}
	}
	if req.Secret != nil {
		if *req.Secret == "" || len(*req.Secret) > 4096 {
			return Endpoint{}, retrypolicy.Policy{}, fmt.Errorf("%w: secret must be between 1 and 4096 characters", ErrInvalid)
		}
		var err error
		enc, err = s.cipher.Encrypt(*req.Secret)
		if err != nil {
			return Endpoint{}, retrypolicy.Policy{}, fmt.Errorf("encrypt endpoint secret: %w", err)
		}
	}
	if req.ConnectTimeoutMS != nil {
		if err := validation.Timeout(*req.ConnectTimeoutMS, "connect_timeout_ms"); err != nil {
			return Endpoint{}, retrypolicy.Policy{}, fmt.Errorf("%w: %v", ErrInvalid, err)
		}
	}
	if req.RequestTimeoutMS != nil {
		if err := validation.Timeout(*req.RequestTimeoutMS, "request_timeout_ms"); err != nil {
			return Endpoint{}, retrypolicy.Policy{}, fmt.Errorf("%w: %v", ErrInvalid, err)
		}
	}
	if req.EventTypes != nil {
		types, err := validation.EventTypes(*req.EventTypes)
		if err != nil {
			return Endpoint{}, retrypolicy.Policy{}, fmt.Errorf("%w: %v", ErrInvalid, err)
		}
		*req.EventTypes = types
	}
	var p *retrypolicy.Policy
	if req.RetryPolicyID != nil {
		policy, err := s.resolvePolicy(ctx, tenantID, req.RetryPolicyID)
		if err != nil {
			return Endpoint{}, retrypolicy.Policy{}, err
		}
		p = &policy
	}
	return s.repo.Patch(ctx, tenantID, id, req, enc, p)
}

func (s *Service) Delete(ctx context.Context, tenantID, id uuid.UUID) (Endpoint, retrypolicy.Policy, error) {
	if err := s.ensureActiveTenant(ctx, tenantID); err != nil {
		return Endpoint{}, retrypolicy.Policy{}, err
	}
	return s.repo.Delete(ctx, tenantID, id)
}
func (s *Service) Subscriptions(ctx context.Context, tenantID uuid.UUID, eventType string) ([]Endpoint, error) {
	if err := s.ensureActiveTenant(ctx, tenantID); err != nil {
		return nil, err
	}
	if err := validateTenant(tenantID); err != nil {
		return nil, err
	}
	types, err := validation.EventTypes([]string{eventType})
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	return s.repo.GetSubscriptions(ctx, tenantID, types[0])
}

func (s *Service) resolvePolicy(ctx context.Context, tenantID uuid.UUID, id *uuid.UUID) (retrypolicy.Policy, error) {
	if id == nil {
		return s.policies.GetOrCreateDefault(ctx, tenantID)
	}
	p, err := s.policies.Get(ctx, tenantID, *id)
	if err != nil {
		return retrypolicy.Policy{}, fmt.Errorf("%w: retry policy not found", ErrInvalid)
	}
	return p, nil
}
func normalizeCreate(r *CreateRequest) {
	r.Name = strings.TrimSpace(r.Name)
	r.URL = strings.TrimSpace(r.URL)
	if r.ConnectTimeoutMS == 0 {
		r.ConnectTimeoutMS = 5000
	}
	if r.RequestTimeoutMS == 0 {
		r.RequestTimeoutMS = 10000
	}
}
func (s *Service) ensureActiveTenant(ctx context.Context, id uuid.UUID) error {
	if err := validateTenant(id); err != nil {
		return err
	}
	if s.tenants == nil {
		return fmt.Errorf("tenant repository is not configured")
	}
	if err := s.tenants.EnsureActive(ctx, id); err != nil {
		if errors.Is(err, tenant.ErrNotFound) {
			return fmt.Errorf("%w: tenant not found", ErrInvalid)
		}
		if errors.Is(err, tenant.ErrInactive) {
			return fmt.Errorf("%w: tenant is not active", ErrInvalid)
		}
		return fmt.Errorf("check tenant: %w", err)
	}
	return nil
}

func validateTenant(id uuid.UUID) error {
	if id == uuid.Nil {
		return fmt.Errorf("%w: invalid tenant id", ErrInvalid)
	}
	return nil
}
