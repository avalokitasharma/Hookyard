package endpoint

import (
	"time"

	"github.com/google/uuid"
)

const (
	StatusActive  = "ACTIVE"
	StatusDeleted = "DELETED"
)

type Endpoint struct {
	ID               uuid.UUID  `json:"endpoint_id"`
	TenantID         uuid.UUID  `json:"tenant_id"`
	Name             string     `json:"name"`
	URL              string     `json:"url"`
	SecretEncrypted  []byte     `json:"-"`
	Status           string     `json:"status"`
	ConnectTimeoutMS int        `json:"connect_timeout_ms"`
	RequestTimeoutMS int        `json:"request_timeout_ms"`
	RetryPolicyID    uuid.UUID  `json:"retry_policy_id"`
	Version          int64      `json:"version"`
	EventTypes       []string   `json:"event_types"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
	DeletedAt        *time.Time `json:"deleted_at,omitempty"`
}

type CreateRequest struct {
	Name             string     `json:"name"`
	URL              string     `json:"url"`
	Secret           string     `json:"secret"`
	ConnectTimeoutMS int        `json:"connect_timeout_ms"`
	RequestTimeoutMS int        `json:"request_timeout_ms"`
	RetryPolicyID    *uuid.UUID `json:"retry_policy_id,omitempty"`
	EventTypes       []string   `json:"event_types"`
}

type PatchRequest struct {
	Name             *string    `json:"name,omitempty"`
	URL              *string    `json:"url,omitempty"`
	Secret           *string    `json:"secret,omitempty"`
	ConnectTimeoutMS *int       `json:"connect_timeout_ms,omitempty"`
	RequestTimeoutMS *int       `json:"request_timeout_ms,omitempty"`
	RetryPolicyID    *uuid.UUID `json:"retry_policy_id,omitempty"`
	EventTypes       *[]string  `json:"event_types,omitempty"`
}

func (e Endpoint) PublicView() map[string]any {
	return map[string]any{
		"endpoint_id":        e.ID,
		"tenant_id":          e.TenantID,
		"name":               e.Name,
		"url":                e.URL,
		"status":             e.Status,
		"connect_timeout_ms": e.ConnectTimeoutMS,
		"request_timeout_ms": e.RequestTimeoutMS,
		"retry_policy_id":    e.RetryPolicyID,
		"version":            e.Version,
		"event_types":        e.EventTypes,
		"has_secret":         len(e.SecretEncrypted) > 0,
		"created_at":         e.CreatedAt,
		"updated_at":         e.UpdatedAt,
		"deleted_at":         e.DeletedAt,
	}
}
