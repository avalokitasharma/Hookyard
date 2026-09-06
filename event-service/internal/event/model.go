package event

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Event struct {
	ID         uuid.UUID       `json:"id"`
	TenantID   uuid.UUID       `json:"tenant_id"`
	Type       string          `json:"type"`
	Payload    json.RawMessage `json:"data"`
	OccurredAt *time.Time      `json:"occurred_at,omitempty"`
	Source     *string         `json:"source,omitempty"`
	Metadata   json.RawMessage `json:"metadata,omitempty"`
	CreatedAt  time.Time       `json:"created_at"`
}

type PublishRequest struct {
	Type           string          `json:"type"`
	IdempotencyKey string          `json:"idempotency_key"`
	Data           json.RawMessage `json:"data"`
	OccurredAt     *time.Time      `json:"occurred_at,omitempty"`
	Source         *string         `json:"source,omitempty"`
	Metadata       json.RawMessage `json:"metadata,omitempty"`
}
