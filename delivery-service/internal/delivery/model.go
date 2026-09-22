package delivery

import "github.com/google/uuid"

type RetryPolicy struct {
	MaxAttempts    int    `json:"max_attempts"`
	BackoffType    string `json:"backoff_type"`
	InitialDelayMS int64  `json:"initial_delay_ms"`
	MaxDelayMS     int64  `json:"max_delay_ms"`
	JitterPercent  int    `json:"jitter_percent"`
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
