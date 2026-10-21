package repository

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"time"

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

func (r *Repository) DeleteTenant(ctx context.Context, id uuid.UUID) (model.Tenant, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return model.Tenant{}, fmt.Errorf("begin delete tenant: %w", err)
	}
	defer tx.Rollback(ctx)

	var t model.Tenant
	err = tx.QueryRow(ctx, `
		UPDATE tenants SET status='DELETED', deleted_at=COALESCE(deleted_at, now()), updated_at=now()
		WHERE tenant_id=$1 AND status <> 'DELETED'
		RETURNING tenant_id, name, status, created_at, updated_at, deleted_at`, id,
	).Scan(&t.ID, &t.Name, &t.Status, &t.CreatedAt, &t.UpdatedAt, &t.DeletedAt)

	if errors.Is(err, pgx.ErrNoRows) {
		// Make DELETE idempotent for an already deleted tenant.
		err = tx.QueryRow(ctx, `SELECT tenant_id,name,status,created_at,updated_at,deleted_at FROM tenants WHERE tenant_id=$1`, id).
			Scan(&t.ID, &t.Name, &t.Status, &t.CreatedAt, &t.UpdatedAt, &t.DeletedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return model.Tenant{}, ErrNotFound
		}
		if err != nil {
			return model.Tenant{}, err
		}
		if t.Status != "DELETED" {
			return model.Tenant{}, fmt.Errorf("delete tenant: %w", err)
		}
	} else if err != nil {
		return model.Tenant{}, fmt.Errorf("delete tenant: %w", err)
	}

	if _, err = tx.Exec(ctx, `UPDATE api_keys SET status='REVOKED', revoked_at=COALESCE(revoked_at, now()) WHERE tenant_id=$1 AND status='ACTIVE'`, id); err != nil {
		return model.Tenant{}, fmt.Errorf("revoke tenant keys: %w", err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO auth_audit_log(tenant_id,action,actor) VALUES($1,'tenant.deleted','system')`, id); err != nil {
		return model.Tenant{}, fmt.Errorf("audit tenant deletion: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return model.Tenant{}, fmt.Errorf("commit tenant deletion: %w", err)
	}
	return t, nil
}

func (r *Repository) CreateKey(ctx context.Context, tenantID uuid.UUID, name, secret, prefix string, scopes []string, expires *time.Time, actor string) (model.APIKey, error) {
	name = strings.TrimSpace(name)
	if name == "" || len(name) > 120 {
		return model.APIKey{}, fmt.Errorf("key name must be 1-120 characters")
	}

	var active bool
	err := r.db.QueryRow(ctx, `SELECT EXISTS ( SELECT 1 FROM tenants WHERE tenant_id = $1 AND status = 'ACTIVE')`,
		tenantID).Scan(&active)

	if err != nil {
		return model.APIKey{}, fmt.Errorf("check tenant: %w", err)
	}
	if !active {
		return model.APIKey{}, ErrNotFound
	}

	hash := sha256.Sum256([]byte(secret))

	var k model.APIKey
	err = r.db.QueryRow(ctx, `
		INSERT INTO api_keys (
			tenant_id, name, key_prefix, secret_hash,
			scopes, expires_at, created_by
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING
			key_id, tenant_id, name, key_prefix,
			scopes, status, created_at, expires_at`,
		tenantID, name, prefix, hash[:], scopes, expires, actor,
	).Scan(
		&k.ID, &k.TenantID, &k.Name, &k.Prefix,
		&k.Scopes, &k.Status, &k.CreatedAt, &k.ExpiresAt,
	)
	if err != nil {
		return model.APIKey{}, fmt.Errorf("create API key: %w", err)
	}

	_, _ = r.db.Exec(ctx, `
		INSERT INTO auth_audit_log (tenant_id, key_id, action, actor)
		VALUES ($1, $2, 'api_key.created', $3)`,
		tenantID, k.ID, actor,
	)

	return k, nil
}

func (r *Repository) ListKeys(ctx context.Context, tenantID uuid.UUID) ([]model.APIKey, error) {
	rows, err := r.db.Query(ctx, `SELECT key_id,tenant_id,name,key_prefix,scopes,status,created_at,last_used_at,expires_at FROM api_keys WHERE tenant_id=$1 ORDER BY created_at DESC`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.APIKey{}
	for rows.Next() {
		var k model.APIKey
		if err := rows.Scan(&k.ID, &k.TenantID, &k.Name, &k.Prefix, &k.Scopes, &k.Status, &k.CreatedAt, &k.LastUsedAt, &k.ExpiresAt); err != nil {
			return nil, err
		}
		out = append(out, k)
	}
	return out, rows.Err()
}

func (r *Repository) RevokeKey(ctx context.Context, tenantID, keyID uuid.UUID, actor string) error {
	tag, err := r.db.Exec(ctx, `UPDATE api_keys SET status='REVOKED',revoked_at=COALESCE(revoked_at,now()) WHERE tenant_id=$1 AND key_id=$2 AND status='ACTIVE'`, tenantID, keyID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	_, _ = r.db.Exec(ctx, `INSERT INTO auth_audit_log(tenant_id,key_id,action,actor) VALUES($1,$2,'api_key.revoked',$3)`, tenantID, keyID, actor)
	return nil
}

func (r *Repository) Verify(ctx context.Context, token string) (model.VerifyResponse, error) {
	parts := strings.SplitN(token, ".", 2)
	if len(parts) != 2 || !strings.HasPrefix(parts[0], "hk_live_") {
		return model.VerifyResponse{Valid: false}, nil
	}
	prefix := strings.TrimPrefix(parts[0], "hk_live_")
	sum := sha256.Sum256([]byte(token))

	var v model.VerifyResponse

	err := r.db.QueryRow(ctx, `SELECT k.tenant_id,k.key_id,k.scopes FROM api_keys k JOIN tenants t ON t.tenant_id=k.tenant_id
 WHERE k.key_prefix=$1 AND k.secret_hash=$2 AND k.status='ACTIVE' AND t.status='ACTIVE' AND (k.expires_at IS NULL OR k.expires_at>now())`, prefix, sum[:]).Scan(&v.TenantID, &v.KeyID, &v.Scopes)
	if errors.Is(err, pgx.ErrNoRows) {
		return model.VerifyResponse{Valid: false}, nil
	}
	if err != nil {
		return v, fmt.Errorf("verify key: %w", err)
	}
	v.Valid = true
	_, _ = r.db.Exec(ctx, `UPDATE api_keys SET last_used_at=now() WHERE key_id=$1`, v.KeyID)
	return v, nil
}
