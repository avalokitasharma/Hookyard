package retrypolicy

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	db *pgxpool.Pool
}

func (r *Repository) Get(ctx context.Context, tenantID, id uuid.UUID) (Policy, error) {
	var p Policy
	err := r.db.QueryRow(ctx,
		`SELECT retry_policy_id, tenant_id, name, max_attempts, backoff_type, initial_delay_ms, max_delay_ms, jitter_percent 
		 FROM retry_policies 
		 WHERE tenant_id=$1 AND retry_policy_id=$2`,
		tenantID, id).
		Scan(&p.ID, &p.TenantID, &p.Name, &p.MaxAttempts, &p.BackoffType, &p.InitialDelay, &p.MaxDelay, &p.JitterPercent)

	if err != nil {
		return Policy{}, err
	}
	return p, nil
}

func (r *Repository) GetOrCreateDefault(ctx context.Context, tenantID uuid.UUID) (Policy, error) {
	var p Policy
	err := r.db.QueryRow(ctx, `
		INSERT INTO retry_policies (tenant_id, name, max_attempts, backoff_type, initial_delay_ms, max_delay_ms, jitter_percent)
		VALUES ($1,'default',5,'exponential',10000,600000,20)
		ON CONFLICT (tenant_id,name) DO UPDATE SET name=EXCLUDED.name
		RETURNING retry_policy_id, tenant_id, name, max_attempts, backoff_type, initial_delay_ms, max_delay_ms, jitter_percent`, tenantID).
		Scan(&p.ID, &p.TenantID, &p.Name, &p.MaxAttempts, &p.BackoffType, &p.InitialDelay, &p.MaxDelay, &p.JitterPercent)
	if err != nil {
		return Policy{}, fmt.Errorf("get default retry policy: %w", err)
	}
	return p, nil
}

func (r *Repository) Exists(ctx context.Context, tenantID, id uuid.UUID) (bool, error) {
	var ok bool
	err := r.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM retry_policies WHERE tenant_id=$1 AND retry_policy_id=$2)`, tenantID, id).Scan(&ok)
	return ok, err
}
