package outbox

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Message struct {
	ID          uuid.UUID
	Topic       string
	Key         string
	Payload     json.RawMessage
	Attempts    int
	AvailableAt time.Time
}
