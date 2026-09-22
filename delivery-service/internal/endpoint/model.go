package endpoint

import (
	"time"

	"github.com/avalokitasharma/HookYard/delivery-service/internal/delivery"
	"github.com/google/uuid"
)

type EndpointChanged struct {
	MessageID        string               `json:"message_id"`
	TenantID         uuid.UUID            `json:"tenant_id"`
	EndpointID       uuid.UUID            `json:"endpoint_id"`
	Name             string               `json:"name"`
	URL              string               `json:"url"`
	SecretEncrypted  []byte               `json:"secret_encrypted"`
	Status           string               `json:"status"`
	ConnectTimeoutMS int                  `json:"connect_timeout_ms"`
	RequestTimeoutMS int                  `json:"request_timeout_ms"`
	RetryPolicy      delivery.RetryPolicy `json:"retry_policy"`
	Version          int64                `json:"version"`
	UpdatedAt        time.Time            `json:"updated_at"`
	EventTypes       []string             `json:"event_types"`
}
