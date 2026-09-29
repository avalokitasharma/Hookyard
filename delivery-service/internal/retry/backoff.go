package retry

import (
	"math"
	"math/rand/v2"
	"time"
)

type Policy struct {
	BackoffType    string
	InitialDelayMS int64
	MaxDelayMS     int64
	JitterPercent  int
}

func NextDelay(policy Policy, nextAttemptNumber int) time.Duration {
	if nextAttemptNumber < 1 {
		nextAttemptNumber = 1
	}

	var base float64
	switch policy.BackoffType {
	case "linear":
		base = float64(policy.InitialDelayMS) * float64(nextAttemptNumber)
	default:
		exponent := float64(nextAttemptNumber - 1)
		base = float64(policy.InitialDelayMS) * math.Pow(2, exponent)
	}

	if policy.MaxDelayMS > 0 && base > float64(policy.MaxDelayMS) {
		base = float64(policy.MaxDelayMS)
	}

	if policy.JitterPercent > 0 {
		jitter := base * float64(policy.JitterPercent) / 100
		base += (rand.Float64()*2 - 1) * jitter
	}

	if base < 0 {
		base = 0
	}
	return time.Duration(base) * time.Millisecond
}
