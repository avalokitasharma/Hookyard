package tenant

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound = errors.New("tenant not found")
	ErrConflict = errors.New("tenant conflict")
	ErrInvalid  = errors.New("invalid tenant request")
	ErrInactive = errors.New("tenant is not active")
)

type Repository struct{ db *pgxpool.Pool }

func NewRepository(db *pgxpool.Pool) *Repository { return &Repository{db: db} }

func (r *Repository) Create(ctx context.Context, name string) (Tenant, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return Tenant{}, fmt.Errorf("begin create tenant: %w", err)
	}
	defer tx.Rollback(ctx)

	id := uuid.New()
	var t Tenant
	err = tx.QueryRow(ctx, `
		INSERT INTO tenants(tenant_id, name, status, created_at, updated_at)
		VALUES($1, $2, $3, NOW(), NOW())
		RETURNING tenant_id, name, status, created_at, updated_at, deleted_at`,
		id, name, StatusActive,
	).Scan(&t.ID, &t.Name, &t.Status, &t.CreatedAt, &t.UpdatedAt, &t.DeletedAt)
	if err != nil {
		return Tenant{}, mapDBError(err)
	}

	// Keep tenant creation and the default retry policy in one transaction.
	// Endpoint creation can therefore safely assume a default policy exists.
	_, err = tx.Exec(ctx, `
		INSERT INTO retry_policies(
			tenant_id, name, max_attempts, backoff_type,
			initial_delay_ms, max_delay_ms, jitter_percent
		) VALUES($1, 'default', 5, 'exponential', 10000, 600000, 20)`, id)
	if err != nil {
		return Tenant{}, fmt.Errorf("create default retry policy: %w", err)
	}

	if err = tx.Commit(ctx); err != nil {
		return Tenant{}, fmt.Errorf("commit create tenant: %w", err)
	}
	return t, nil
}

func (r *Repository) Get(ctx context.Context, id uuid.UUID) (Tenant, error) {
	var t Tenant
	err := r.db.QueryRow(ctx, `
		SELECT tenant_id, name, status, created_at, updated_at, deleted_at
		FROM tenants WHERE tenant_id=$1`, id).
		Scan(&t.ID, &t.Name, &t.Status, &t.CreatedAt, &t.UpdatedAt, &t.DeletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Tenant{}, ErrNotFound
	}
	if err != nil {
		return Tenant{}, fmt.Errorf("get tenant: %w", err)
	}
	return t, nil
}

func (r *Repository) EnsureActive(ctx context.Context, id uuid.UUID) error {
	var status string
	err := r.db.QueryRow(ctx, `SELECT status FROM tenants WHERE tenant_id=$1`, id).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("check tenant status: %w", err)
	}
	if status != StatusActive {
		return ErrInactive
	}
	return nil
}

func (r *Repository) Patch(ctx context.Context, id uuid.UUID, req PatchRequest) (Tenant, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return Tenant{}, fmt.Errorf("begin patch tenant: %w", err)
	}
	defer tx.Rollback(ctx)

	var t Tenant
	err = tx.QueryRow(ctx, `
		SELECT tenant_id, name, status, created_at, updated_at, deleted_at
		FROM tenants WHERE tenant_id=$1 FOR UPDATE`, id).
		Scan(&t.ID, &t.Name, &t.Status, &t.CreatedAt, &t.UpdatedAt, &t.DeletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Tenant{}, ErrNotFound
	}
	if err != nil {
		return Tenant{}, fmt.Errorf("lock tenant: %w", err)
	}
	if t.Status == StatusDeleted {
		return Tenant{}, fmt.Errorf("%w: deleted tenant cannot be patched", ErrConflict)
	}

	name := t.Name
	if req.Name != nil {
		name = *req.Name
	}

	err = tx.QueryRow(ctx, `
		UPDATE tenants SET name=$2, updated_at=NOW()
		WHERE tenant_id=$1
		RETURNING tenant_id, name, status, created_at, updated_at, deleted_at`,
		id, name,
	).Scan(&t.ID, &t.Name, &t.Status, &t.CreatedAt, &t.UpdatedAt, &t.DeletedAt)
	if err != nil {
		return Tenant{}, mapDBError(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return Tenant{}, fmt.Errorf("commit patch tenant: %w", err)
	}
	return t, nil
}

func (r *Repository) Delete(ctx context.Context, id uuid.UUID) (Tenant, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return Tenant{}, fmt.Errorf("begin delete tenant: %w", err)
	}
	defer tx.Rollback(ctx)

	var t Tenant
	err = tx.QueryRow(ctx, `
		SELECT tenant_id, name, status, created_at, updated_at, deleted_at
		FROM tenants WHERE tenant_id=$1 FOR UPDATE`, id).
		Scan(&t.ID, &t.Name, &t.Status, &t.CreatedAt, &t.UpdatedAt, &t.DeletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Tenant{}, ErrNotFound
	}
	if err != nil {
		return Tenant{}, fmt.Errorf("lock tenant: %w", err)
	}
	if t.Status == StatusDeleted {
		return t, nil
	}

	var activeEndpoints int
	if err = tx.QueryRow(ctx, `
		SELECT COUNT(*) FROM endpoints
		WHERE tenant_id=$1 AND status=$2 AND deleted_at IS NULL`, id, "ACTIVE").Scan(&activeEndpoints); err != nil {
		return Tenant{}, fmt.Errorf("check tenant endpoints: %w", err)
	}
	if activeEndpoints > 0 {
		return Tenant{}, fmt.Errorf("%w: delete or disable all endpoints before deleting tenant", ErrConflict)
	}

	now := time.Now().UTC()
	_, err = tx.Exec(ctx, `
		UPDATE tenants SET status=$2, deleted_at=$3, updated_at=$3
		WHERE tenant_id=$1`, id, StatusDeleted, now)
	if err != nil {
		return Tenant{}, fmt.Errorf("delete tenant: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return Tenant{}, fmt.Errorf("commit delete tenant: %w", err)
	}
	t.Status = StatusDeleted
	t.DeletedAt = &now
	t.UpdatedAt = now
	return t, nil
}

func mapDBError(err error) error {
	if strings.Contains(strings.ToLower(err.Error()), "duplicate key") {
		return fmt.Errorf("%w: duplicate tenant data", ErrConflict)
	}
	return fmt.Errorf("database error: %w", err)
}
