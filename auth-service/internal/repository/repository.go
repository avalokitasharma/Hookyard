package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/avalokitasharma/HookYard/auth-service/internal/model"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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
