package attempt

import "time"

type ResultStatus string

const (
	StatusSuccess   ResultStatus = "SUCCESS"
	StatusRetryable ResultStatus = "RETRYABLE_FAILURE"
	StatusPermanent ResultStatus = "PERMANENT_FAILURE"
)

type Result struct {
	Status          ResultStatus
	HTTPStatusCode  *int
	ErrorCode       *string
	ErrorMessage    *string
	ResponseHeaders map[string]string
	ResponseBody    string
	Latency         time.Duration
}
