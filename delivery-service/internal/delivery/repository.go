package delivery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("delivery not found")

const consumerName = "delivery-event-consumer"

type Repository struct {
	db *pgxpool.Pool
}

func NewRepository(db *pgxpool.Pool) *Repository {
	return &Repository{db: db}
}

func (r *Repository) CreateDeliveries(ctx context.Context, event EventAccepted, endpoints []EndpointSnapshotWithID) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, ep := range endpoints {
		policy, err := json.Marshal(ep.Snapshot.RetryPolicy)
		if err != nil {
			return fmt.Errorf("marshal retry policy: %w", err)
		}

		_, err = tx.Exec(ctx, `
            INSERT INTO deliveries(
                tenant_id,
                event_id,
                endpoint_id,
                event_type,
                event_payload,
                status,
                attempt_count,
                next_attempt_at,
                endpoint_url,
                endpoint_secret_encrypted,
                connect_timeout_ms,
                request_timeout_ms,
                retry_policy
            )
            VALUES(
                $1,$2,$3,$4,$5,$6,0,NOW(),
                $7,$8,$9,$10,$11
            )
            ON CONFLICT (event_id, endpoint_id) DO NOTHING
        `,
			event.TenantID,
			event.EventID,
			ep.EndpointID,
			event.EventType,
			event.Payload,
			StatusPending,
			ep.Snapshot.URL,
			ep.Snapshot.SecretEncrypted,
			ep.Snapshot.ConnectTimeoutMS,
			ep.Snapshot.RequestTimeoutMS,
			policy,
		)

		if err != nil {
			return fmt.Errorf("create delivery: %w", err)
		}
	}

	return tx.Commit(ctx)
}
