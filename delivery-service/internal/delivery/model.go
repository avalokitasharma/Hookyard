package delivery

type RetryPolicy struct {
	MaxAttempts    int    `json:"max_attempts"`
	BackoffType    string `json:"backoff_type"`
	InitialDelayMS int64  `json:"initial_delay_ms"`
	MaxDelayMS     int64  `json:"max_delay_ms"`
	JitterPercent  int    `json:"jitter_percent"`
}
