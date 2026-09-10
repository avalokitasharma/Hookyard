package tenant

import (
	"time"

	"github.com/google/uuid"
)

const (
	StatusActive  = "ACTIVE"
	StatusDeleted = "DELETED"
)

type Tenant struct {
	ID        uuid.UUID  `json:"tenant_id"`
	Name      string     `json:"name"`
	Status    string     `json:"status"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
	DeletedAt *time.Time `json:"deleted_at,omitempty"`
}

type CreateRequest struct {
	Name string `json:"name"`
}

type PatchRequest struct {
	Name *string `json:"name,omitempty"`
}
