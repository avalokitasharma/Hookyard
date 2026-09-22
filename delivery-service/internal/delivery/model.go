package delivery

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Status string

const (
	StatusPending    Status = "PENDING"
	StatusInProgress Status = "IN_PROGRESS"
	StatusRetrying   Status = "RETRYING"
	StatusDelivered  Status = "DELIVERED"
	StatusFailed     Status = "FAILED"
	StatusDLQ        Status = "DLQ"
	StatusCancelled  Status = "CANCELLED"
)

type RetryPolicy struct {
	MaxAttempts    int    `json:"max_attempts"`
	BackoffType    string `json:"backoff_type"`
	InitialDelayMS int64  `json:"initial_delay_ms"`
	MaxDelayMS     int64  `json:"max_delay_ms"`
	JitterPercent  int    `json:"jitter_percent"`
}

type EventAccepted struct {
	MessageID string          `json:"message_id"`
	EventID   uuid.UUID       `json:"event_id"`
	TenantID  uuid.UUID       `json:"tenant_id"`
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

type EndpointSnapshot struct {
	EventType        string
	URL              string
	SecretEncrypted  []byte
	ConnectTimeoutMS int
	RequestTimeoutMS int
	RetryPolicy      RetryPolicy
}

type EndpointSnapshotWithID struct {
	EndpointID uuid.UUID
	EventType  string
	Snapshot   EndpointSnapshot
}

type Delivery struct {
	ID                 uuid.UUID
	TenantID           uuid.UUID
	EventID            uuid.UUID
	EndpointID         uuid.UUID
	Status             Status
	AttemptCount       int
	NextAttemptAt      time.Time
	LastAttemptAt      *time.Time
	LastStatusCode     *int
	LastErrorCode      *string
	LastErrorMessage   *string
	LeaseOwner         *string
	LeaseExpiresAt     *time.Time
	ReplayOfDeliveryID *uuid.UUID
	EndpointSnapshot   EndpointSnapshot
	EventPayload       json.RawMessage
	CreatedAt          time.Time
	UpdatedAt          time.Time
	CompletedAt        *time.Time
}
