package model

import (
	"time"

	"github.com/google/uuid"
)

type Tenant struct {
	ID        uuid.UUID  `json:"tenant_id"`
	Name      string     `json:"name"`
	Status    string     `json:"status"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
	DeletedAt *time.Time `json:"deleted_at,omitempty"`
}

type APIKey struct {
	ID         uuid.UUID  `json:"key_id"`
	TenantID   uuid.UUID  `json:"tenant_id"`
	Name       string     `json:"name"`
	Prefix     string     `json:"key_prefix"`
	Scopes     []string   `json:"scopes"`
	Status     string     `json:"status"`
	CreatedAt  time.Time  `json:"created_at"`
	LastUsedAt *time.Time `json:"last_used_at,omitempty"`
	ExpiresAt  *time.Time `json:"expires_at,omitempty"`
}
type CreateTenantRequest struct {
	Name    string `json:"name"`
	KeyName string `json:"key_name"`
}
type CreateKeyRequest struct {
	Name      string     `json:"name"`
	Scopes    []string   `json:"scopes"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}
type CreatedKey struct {
	APIKey
	Secret string `json:"api_key"`
}

type UpdateTenantRequest struct {
	Name   *string `json:"name,omitempty"`
	Status *string `json:"status,omitempty"`
}

type VerifyResponse struct {
	Valid    bool      `json:"valid"`
	TenantID uuid.UUID `json:"tenant_id,omitempty"`
	KeyID    uuid.UUID `json:"key_id,omitempty"`
	Scopes   []string  `json:"scopes,omitempty"`
}
