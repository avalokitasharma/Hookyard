package retrypolicy

import "github.com/google/uuid"

type Policy struct {
	ID            uuid.UUID `json:"retry_policy_id"`
	TenantID      uuid.UUID `json:"tenant_id"`
	Name          string    `json:"name"`
	MaxAttempts   int       `json:"max_attempts"`
	BackoffType   string    `json:"backoff_type"`
	InitialDelay  int       `json:"initial_delay_ms"`
	MaxDelay      int       `json:"max_delay_ms"`
	JitterPercent int       `json:"jitter_percent"`
}

const (
	BackoffExponential = "exponential"
	BackoffFixed       = "fixed"
)
