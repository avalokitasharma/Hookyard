package endpoint

import (
	"time"

	"github.com/avalokitasharma/HookYard/endpoint-service/internal/retrypolicy"
	"github.com/google/uuid"
)

// EndpointChanged is the full immutable endpoint snapshot consumed by Delivery Service.
// It intentionally carries encrypted secret material because Delivery Service snapshots
// it for webhook signing. It must never be exposed through a public/read API.
type EndpointChanged struct {
	MessageID        string             `json:"message_id"`
	EventName        string             `json:"event_name"`
	SchemaVersion    int                `json:"schema_version"`
	TenantID         uuid.UUID          `json:"tenant_id"`
	EndpointID       uuid.UUID          `json:"endpoint_id"`
	Name             string             `json:"name"`
	URL              string             `json:"url"`
	SecretEncrypted  []byte             `json:"secret_encrypted"`
	Status           string             `json:"status"`
	ConnectTimeoutMS int                `json:"connect_timeout_ms"`
	RequestTimeoutMS int                `json:"request_timeout_ms"`
	RetryPolicy      retrypolicy.Policy `json:"retry_policy"`
	Version          int64              `json:"version"`
	UpdatedAt        time.Time          `json:"updated_at"`
	EventTypes       []string           `json:"event_types"`
}

// SubscriptionChanged is the least-privilege contract for Event Service's local
// subscription projection. It deliberately contains no secret material.
type SubscriptionChanged struct {
	MessageID     string    `json:"message_id"`
	EventName     string    `json:"event_name"`
	SchemaVersion int       `json:"schema_version"`
	TenantID      uuid.UUID `json:"tenant_id"`
	EndpointID    uuid.UUID `json:"endpoint_id"`
	URL           string    `json:"url"`
	Status        string    `json:"status"`
	Version       int64     `json:"version"`
	UpdatedAt     time.Time `json:"updated_at"`
	EventTypes    []string  `json:"event_types"`
}
