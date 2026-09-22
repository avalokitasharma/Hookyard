package endpoint

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/avalokitasharma/HookYard/delivery-service/internal/delivery"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	db *pgxpool.Pool
}

func NewRepository(db *pgxpool.Pool) *Repository {
	return &Repository{db: db}
}

func (r *Repository) Upsert(ctx context.Context, e EndpointChanged) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
        INSERT INTO endpoint_projection (
            endpoint_id, tenant_id, name, url, secret_encrypted, status,
            connect_timeout_ms, request_timeout_ms, retry_policy, version, updated_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        ON CONFLICT (endpoint_id) DO UPDATE SET
            tenant_id = EXCLUDED.tenant_id,
            name = EXCLUDED.name,
            url = EXCLUDED.url,
            secret_encrypted = EXCLUDED.secret_encrypted,
            status = EXCLUDED.status,
            connect_timeout_ms = EXCLUDED.connect_timeout_ms,
            request_timeout_ms = EXCLUDED.request_timeout_ms,
            retry_policy = EXCLUDED.retry_policy,
            version = EXCLUDED.version,
            updated_at = EXCLUDED.updated_at
        WHERE endpoint_projection.version < EXCLUDED.version
    `, e.EndpointID, e.TenantID, e.Name, e.URL, e.SecretEncrypted, e.Status,
		e.ConnectTimeoutMS, e.RequestTimeoutMS, e.RetryPolicy, e.Version, e.UpdatedAt)
	if err != nil {
		return fmt.Errorf("upsert endpoint: %w", err)
	}

	_, err = tx.Exec(ctx, `DELETE FROM endpoint_subscription_projection WHERE endpoint_id = $1`, e.EndpointID)
	if err != nil {
		return fmt.Errorf("delete subscriptions: %w", err)
	}

	for _, eventType := range e.EventTypes {
		_, err = tx.Exec(ctx, `
            INSERT INTO endpoint_subscription_projection(endpoint_id, tenant_id, event_type)
            VALUES ($1,$2,$3)
            ON CONFLICT DO NOTHING
        `, e.EndpointID, e.TenantID, eventType)
		if err != nil {
			return fmt.Errorf("insert subscription: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func (r *Repository) GetForEvent(ctx context.Context, tenantID uuid.UUID, eventType string) ([]delivery.EndpointSnapshotWithID, error) {
	rows, err := r.db.Query(ctx, `
        SELECT e.endpoint_id, e.event_type, p.url, p.secret_encrypted,
               p.connect_timeout_ms, p.request_timeout_ms, p.retry_policy
        FROM endpoint_subscription_projection e
        JOIN endpoint_projection p ON p.endpoint_id = e.endpoint_id
        WHERE e.tenant_id = $1
          AND e.event_type = $2
          AND p.status = 'ACTIVE'
    `, tenantID, eventType)
	if err != nil {
		return nil, fmt.Errorf("query endpoints: %w", err)
	}
	defer rows.Close()

	var result []delivery.EndpointSnapshotWithID
	for rows.Next() {
		var x delivery.EndpointSnapshotWithID
		var policyJSON []byte
		if err := rows.Scan(&x.EndpointID, &x.EventType, &x.Snapshot.URL, &x.Snapshot.SecretEncrypted,
			&x.Snapshot.ConnectTimeoutMS, &x.Snapshot.RequestTimeoutMS, &policyJSON); err != nil {
			return nil, fmt.Errorf("scan endpoint: %w", err)
		}
		if err := json.Unmarshal(policyJSON, &x.Snapshot.RetryPolicy); err != nil {
			return nil, fmt.Errorf("decode retry policy: %w", err)
		}
		result = append(result, x)
	}
	return result, rows.Err()
}
