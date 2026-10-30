package delivery

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/avalokitasharma/HookYard/delivery-service/internal/attempt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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

func (r *Repository) ClaimBatch(ctx context.Context, workerID string, limit int, lease time.Duration) ([]Delivery, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
        SELECT id,tenant_id,event_id,endpoint_id,event_type,event_payload,status,attempt_count,next_attempt_at,
               last_attempt_at,last_status_code,last_error_code,last_error_message,
               lease_owner,lease_expires_at,replay_of_delivery_id,endpoint_url,endpoint_secret_encrypted,
               connect_timeout_ms,request_timeout_ms,retry_policy,created_at,updated_at,completed_at
        FROM deliveries
        WHERE (status IN ('PENDING','RETRYING') AND next_attempt_at <= NOW())
           OR (status = 'IN_PROGRESS' AND lease_expires_at < NOW())
        ORDER BY next_attempt_at, created_at
        FOR UPDATE SKIP LOCKED
        LIMIT $1
    `, limit)
	if err != nil {
		return nil, fmt.Errorf("claim query: %w", err)
	}
	defer rows.Close()

	type claimed struct {
		d       Delivery
		started time.Time
	}
	var result []claimed
	now := time.Now()
	expires := now.Add(30 * time.Second)

	for rows.Next() {
		var d Delivery
		var policyJSON []byte
		if err := rows.Scan(&d.ID, &d.TenantID, &d.EventID, &d.EndpointID, &d.EndpointSnapshot.EventType, &d.EventPayload, &d.Status, &d.AttemptCount, &d.NextAttemptAt,
			&d.LastAttemptAt, &d.LastStatusCode, &d.LastErrorCode, &d.LastErrorMessage, &d.LeaseOwner, &d.LeaseExpiresAt,
			&d.ReplayOfDeliveryID, &d.EndpointSnapshot.URL, &d.EndpointSnapshot.SecretEncrypted, &d.EndpointSnapshot.ConnectTimeoutMS,
			&d.EndpointSnapshot.RequestTimeoutMS, &policyJSON, &d.CreatedAt, &d.UpdatedAt, &d.CompletedAt); err != nil {
			return nil, fmt.Errorf("scan delivery: %w", err)
		}
		if err := json.Unmarshal(policyJSON, &d.EndpointSnapshot.RetryPolicy); err != nil {
			return nil, fmt.Errorf("decode policy: %w", err)
		}

		_, err = tx.Exec(ctx, `
            UPDATE deliveries
            SET status='IN_PROGRESS', lease_owner=$2, lease_expires_at=$3,
                attempt_count=attempt_count+1, last_attempt_at=$4, updated_at=NOW()
            WHERE id=$1
        `, d.ID, workerID, expires, now)
		if err != nil {
			return nil, fmt.Errorf("lease delivery: %w", err)
		}
		d.AttemptCount++
		d.LastAttemptAt = &now
		result = append(result, claimed{d: d, started: now})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit claim: %w", err)
	}

	deliveries := make([]Delivery, len(result))
	for i := range result {
		deliveries[i] = result[i].d
	}
	return deliveries, nil
}

func (r *Repository) Get(ctx context.Context, tenantID, id uuid.UUID) (*Delivery, error) {
	var d Delivery
	var policyJSON []byte
	err := r.db.QueryRow(ctx, `
        SELECT id,tenant_id,event_id,endpoint_id,event_type,event_payload,status,attempt_count,next_attempt_at,last_attempt_at,
               last_status_code,last_error_code,last_error_message,lease_owner,lease_expires_at,replay_of_delivery_id,
               endpoint_url,endpoint_secret_encrypted,connect_timeout_ms,request_timeout_ms,retry_policy,created_at,updated_at,completed_at
        FROM deliveries WHERE id=$1 AND tenant_id=$2
    `, id, tenantID).Scan(&d.ID, &d.TenantID, &d.EventID, &d.EndpointID, &d.EndpointSnapshot.EventType, &d.EventPayload, &d.Status, &d.AttemptCount, &d.NextAttemptAt,
		&d.LastAttemptAt, &d.LastStatusCode, &d.LastErrorCode, &d.LastErrorMessage, &d.LeaseOwner, &d.LeaseExpiresAt, &d.ReplayOfDeliveryID,
		&d.EndpointSnapshot.URL, &d.EndpointSnapshot.SecretEncrypted, &d.EndpointSnapshot.ConnectTimeoutMS, &d.EndpointSnapshot.RequestTimeoutMS,
		&policyJSON, &d.CreatedAt, &d.UpdatedAt, &d.CompletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(policyJSON, &d.EndpointSnapshot.RetryPolicy); err != nil {
		return nil, err
	}
	return &d, nil
}

func (r *Repository) RecordAttemptAndTransition(ctx context.Context, d Delivery, res attempt.Result, nextAttemptAt *time.Time) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	headersJSON, _ := json.Marshal(res.ResponseHeaders)
	status := StatusDelivered
	switch res.Status {
	case attempt.StatusRetryable:
		if d.AttemptCount >= d.EndpointSnapshot.RetryPolicy.MaxAttempts {
			status = StatusDLQ
		} else {
			status = StatusRetrying
		}
	case attempt.StatusPermanent:
		status = StatusFailed
	}

	startedAt := time.Now().Add(-res.Latency)
	if d.LastAttemptAt != nil {
		startedAt = *d.LastAttemptAt
	}

	_, err = tx.Exec(ctx, `
        INSERT INTO delivery_attempts(
            delivery_id,attempt_number,started_at,completed_at,status,http_status_code,
            latency_ms,error_code,error_message,response_headers,response_body
        ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
    `, d.ID, d.AttemptCount, startedAt, time.Now(), string(res.Status), res.HTTPStatusCode,
		res.Latency.Milliseconds(), res.ErrorCode, res.ErrorMessage, headersJSON, res.ResponseBody)
	if err != nil {
		return fmt.Errorf("insert attempt: %w", err)
	}

	_, err = tx.Exec(ctx, `
        UPDATE deliveries
        SET status=$2,
            last_status_code=$3,
            last_error_code=$4,
            last_error_message=$5,
            lease_owner=NULL,
            lease_expires_at=NULL,
            completed_at=CASE WHEN $2 IN ('DELIVERED','FAILED','DLQ') THEN NOW() ELSE NULL END,
            next_attempt_at=CASE WHEN $2='RETRYING' THEN $6 ELSE next_attempt_at END,
            updated_at=NOW()
        WHERE id=$1 AND status='IN_PROGRESS'
    `, d.ID, status, res.HTTPStatusCode, res.ErrorCode, res.ErrorMessage, nextAttemptAt)
	if err != nil {
		return fmt.Errorf("transition delivery: %w", err)
	}
	return tx.Commit(ctx)
}

func (r *Repository) CreateReplay(ctx context.Context, original Delivery) (uuid.UUID, error) {
	id := uuid.New()
	policy, err := json.Marshal(original.EndpointSnapshot.RetryPolicy)
	if err != nil {
		return uuid.Nil, err
	}
	_, err = r.db.Exec(ctx, `
        INSERT INTO deliveries(
            id,tenant_id,event_id,endpoint_id,event_type,event_payload,status,attempt_count,next_attempt_at,
            replay_of_delivery_id,endpoint_url,endpoint_secret_encrypted,connect_timeout_ms,request_timeout_ms,retry_policy
        ) VALUES($1,$2,$3,$4,$5,$6,'PENDING',0,NOW(),$7,$8,$9,$10,$11,$12)
    `, id, original.TenantID, original.EventID, original.EndpointID, original.EndpointSnapshot.EventType, original.EventPayload, original.ID,
		original.EndpointSnapshot.URL, original.EndpointSnapshot.SecretEncrypted,
		original.EndpointSnapshot.ConnectTimeoutMS, original.EndpointSnapshot.RequestTimeoutMS, policy)
	return id, err
}

func (r *Repository) List(ctx context.Context, tenantID uuid.UUID, filter ListFilter, dlqOnly bool) (DeliveryPage, error) {
	limit := filter.Limit
	if limit < 1 || limit > 100 {
		limit = 50
	}
	status := strings.ToUpper(strings.TrimSpace(filter.Status))
	if dlqOnly {
		if status != "" && status != string(StatusDLQ) {
			return DeliveryPage{}, fmt.Errorf("invalid DLQ status filter")
		}
		status = string(StatusDLQ)
	} else if status != "" && status != string(StatusPending) && status != string(StatusInProgress) && status != string(StatusRetrying) && status != string(StatusDelivered) && status != string(StatusFailed) && status != string(StatusDLQ) && status != string(StatusCancelled) {
		return DeliveryPage{}, fmt.Errorf("invalid delivery status filter")
	}

	var cursorTime time.Time
	var cursorID uuid.UUID
	if filter.Cursor != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(filter.Cursor)
		if err != nil {
			return DeliveryPage{}, fmt.Errorf("invalid cursor")
		}
		parts := strings.Split(string(decoded), "|")
		if len(parts) != 2 {
			return DeliveryPage{}, fmt.Errorf("invalid cursor")
		}
		cursorTime, err = time.Parse(time.RFC3339Nano, parts[0])
		if err != nil {
			return DeliveryPage{}, fmt.Errorf("invalid cursor")
		}
		cursorID, err = uuid.Parse(parts[1])
		if err != nil {
			return DeliveryPage{}, fmt.Errorf("invalid cursor")
		}
	}

	args := []any{tenantID}
	where := "WHERE tenant_id=$1"
	if status != "" {
		args = append(args, status)
		where += fmt.Sprintf(" AND status=$%d", len(args))
	}
	if filter.EndpointID != nil {
		args = append(args, *filter.EndpointID)
		where += fmt.Sprintf(" AND endpoint_id=$%d", len(args))
	}
	if filter.EventID != nil {
		args = append(args, *filter.EventID)
		where += fmt.Sprintf(" AND event_id=$%d", len(args))
	}
	if !cursorTime.IsZero() {
		args = append(args, cursorTime, cursorID)
		where += fmt.Sprintf(" AND (created_at,id)<($%d,$%d)", len(args)-1, len(args))
	}
	args = append(args, limit+1)
	query := fmt.Sprintf(`
		SELECT id,tenant_id,event_id,endpoint_id,status,attempt_count,next_attempt_at,last_attempt_at,
		       last_status_code,last_error_code,replay_of_delivery_id,created_at,updated_at,completed_at
		FROM deliveries
		%s
		ORDER BY created_at DESC,id DESC
		LIMIT $%d`, where, len(args))
	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return DeliveryPage{}, fmt.Errorf("list deliveries: %w", err)
	}
	defer rows.Close()

	items := make([]Delivery, 0, limit+1)
	for rows.Next() {
		var d Delivery
		if err := rows.Scan(&d.ID, &d.TenantID, &d.EventID, &d.EndpointID, &d.Status, &d.AttemptCount,
			&d.NextAttemptAt, &d.LastAttemptAt, &d.LastStatusCode, &d.LastErrorCode,
			&d.ReplayOfDeliveryID, &d.CreatedAt, &d.UpdatedAt, &d.CompletedAt); err != nil {
			return DeliveryPage{}, fmt.Errorf("scan delivery: %w", err)
		}
		items = append(items, d)
	}
	if err := rows.Err(); err != nil {
		return DeliveryPage{}, err
	}

	page := DeliveryPage{Items: items}
	if len(items) > limit {
		last := items[limit-1]
		page.Items = items[:limit]
		page.NextCursor = base64.RawURLEncoding.EncodeToString([]byte(last.CreatedAt.UTC().Format(time.RFC3339Nano) + "|" + last.ID.String()))
	}
	return page, nil
}
