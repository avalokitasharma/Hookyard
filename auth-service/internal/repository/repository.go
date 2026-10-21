package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/avalokitasharma/HookYard/auth-service/internal/model"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("not found")
var ErrConflict = errors.New("conflict")

type Repository struct{ db *pgxpool.Pool }

func New(db *pgxpool.Pool) *Repository { return &Repository{db: db} }

func (r *Repository) CreateTenant(ctx context.Context, name string) (model.Tenant, error) {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > 255 {
		return model.Tenant{}, fmt.Errorf("name must be 1-255 characters")
	}
	var t model.Tenant
	err := r.db.QueryRow(ctx, `INSERT INTO tenants(name) VALUES($1) RETURNING tenant_id,name,status,created_at,updated_at,deleted_at`, name).Scan(&t.ID, &t.Name, &t.Status, &t.CreatedAt, &t.UpdatedAt, &t.DeletedAt)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unique") {
			return t, ErrConflict
		}
		return t, fmt.Errorf("create tenant: %w", err)
	}
	return t, nil
}

func (r *Repository) GetTenant(ctx context.Context, id uuid.UUID) (model.Tenant, error) {
	var t model.Tenant
	err := r.db.QueryRow(ctx, `
		SELECT tenant_id, name, status, created_at, updated_at, deleted_at
		FROM tenants WHERE tenant_id=$1`, id,
	).Scan(&t.ID, &t.Name, &t.Status, &t.CreatedAt, &t.UpdatedAt, &t.DeletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Tenant{}, ErrNotFound
	}
	if err != nil {
		return model.Tenant{}, fmt.Errorf("get tenant: %w", err)
	}
	return t, nil
}

func (r *Repository) ListTenants(ctx context.Context) ([]model.Tenant, error) {
	rows, err := r.db.Query(ctx, `
		SELECT tenant_id, name, status, created_at, updated_at, deleted_at
		FROM tenants ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("list tenants: %w", err)
	}
	defer rows.Close()

	out := make([]model.Tenant, 0)
	for rows.Next() {
		var t model.Tenant
		if err := rows.Scan(&t.ID, &t.Name, &t.Status, &t.CreatedAt, &t.UpdatedAt, &t.DeletedAt); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (r *Repository) UpdateTenant(ctx context.Context, id uuid.UUID, req model.UpdateTenantRequest) (model.Tenant, error) {
	if req.Name == nil && req.Status == nil {
		return model.Tenant{}, fmt.Errorf("at least one field must be provided")
	}

	if req.Name != nil {
		name := strings.TrimSpace(*req.Name)
		if name == "" || len(name) > 255 {
			return model.Tenant{}, fmt.Errorf("name must be 1-255 characters")
		}
		req.Name = &name
	}

	if req.Status != nil &&
		*req.Status != "ACTIVE" &&
		*req.Status != "SUSPENDED" {
		return model.Tenant{}, fmt.Errorf("status must be ACTIVE or SUSPENDED")
	}

	var t model.Tenant
	err := r.db.QueryRow(ctx, `
		UPDATE tenants
		SET name = COALESCE($2, name),
		    status = COALESCE($3, status),
		    updated_at = now()
		WHERE tenant_id = $1
		  AND status <> 'DELETED'
		RETURNING tenant_id, name, status, created_at, updated_at, deleted_at`,
		id, req.Name, req.Status,
	).Scan(
		&t.ID,
		&t.Name,
		&t.Status,
		&t.CreatedAt,
		&t.UpdatedAt,
		&t.DeletedAt,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return model.Tenant{}, ErrNotFound
	}
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return model.Tenant{}, ErrConflict
		}
		return model.Tenant{}, fmt.Errorf("update tenant: %w", err)
	}

	return t, nil
}
