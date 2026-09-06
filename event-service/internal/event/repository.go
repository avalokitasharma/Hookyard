package event

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository interface {
	CreateEvent(ctx context.Context, req PublishRequest, tenantID uuid.UUID) (*Event, error)
	GetByID(ctx context.Context, tenantID uuid.UUID, id uuid.UUID) (*Event, error)
}

type PostgresRepository struct {
	db *pgxpool.Pool
}

func NewPostgresRepository(db *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{
		db: db,
	}
}

var ErrInvalidRequest = errors.New("invalid event request")

func (r *PostgresRepository) CreateEvent(ctx context.Context, req PublishRequest, tenantID uuid.UUID) (*Event, error) {
	if req.Type == "" || req.IdempotencyKey == "" || len(req.Data) == 0 {
		return nil, ErrInvalidRequest
	}
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// check whether this idempotency key have been already processed .
	var existingEventID uuid.UUID
	err = tx.QueryRow(
		ctx,
		`
		SELECT event_id
		FROM event_idempotency
		WHERE tenant_id = $1
		  AND idempotency_key = $2
		`,
		tenantID,
		req.IdempotencyKey,
	).Scan(&existingEventID)

	if err == nil {
		return r.getEventTx(ctx, tx, tenantID, existingEventID)
	}

	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("check idempotency: %w", err)
	}
	eventID := uuid.New()

	event := &Event{
		ID:         eventID,
		TenantID:   tenantID,
		Type:       req.Type,
		Payload:    req.Data,
		OccurredAt: req.OccurredAt,
		Source:     req.Source,
		Metadata:   req.Metadata,
	}
	_, err = tx.Exec(ctx,
		`
		INSERT INTO events (
			id,
			tenant_id,
			event_type,
			payload,
			occurred_at,
			source,
			metadata
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		`,
		event.ID,
		event.TenantID,
		event.Type,
		event.Payload,
		event.OccurredAt,
		event.Source,
		nullableJSON(event.Metadata),
	)
	if err != nil {
		return nil, fmt.Errorf("insert event: %w", err)
	}
	_, err = tx.Exec(
		ctx,
		`
		INSERT INTO event_idempotency (
			tenant_id,
			idempotency_key,
			event_id
		)
		VALUES ($1, $2, $3)
		`,
		tenantID,
		req.IdempotencyKey,
		eventID,
	)

	if err != nil {
		return nil, fmt.Errorf("insert idempotency record: %w", err)
	}
	// Event persistence and publication intent are committed atomically

	outboxPayload, err := json.Marshal(map[string]interface{}{
		"event_id":    event.ID,
		"tenant_id":   event.TenantID,
		"event_type":  event.Type,
		"occurred_at": event.CreatedAt,
	})

	if err != nil {
		return nil, fmt.Errorf("marshal outbox payload: %w", err)
	}
	_, err = tx.Exec(
		ctx,
		`
		INSERT INTO outbox (
			aggregate_type,
			aggregate_id,
			event_type,
			payload
		)
		VALUES ($1, $2, $3, $4)
		`,
		"event",
		event.ID,
		"EventAccepted",
		outboxPayload,
	)
	if err != nil {
		return nil, fmt.Errorf("insert outbox event: %w", err)
	}

	err = tx.Commit(ctx)

	if err != nil {
		return nil, fmt.Errorf("commit transaction: %w", err)
	}

	return event, nil

}

func nullableJSON(data json.RawMessage) interface{} {
	if len(data) == 0 {
		return nil
	}

	return data
}

func (r *PostgresRepository) getEventTx(ctx context.Context, tx pgx.Tx, tenantID uuid.UUID, eventID uuid.UUID) (*Event, error) {

	var e Event

	err := tx.QueryRow(
		ctx,
		`
		SELECT
			id,
			tenant_id,
			event_type,
			payload,
			occurred_at,
			source,
			metadata,
			created_at
		FROM events
		WHERE id = $1
		  AND tenant_id = $2
		`,
		eventID,
		tenantID,
	).Scan(&e.ID, &e.TenantID, &e.Type, &e.Payload, &e.OccurredAt, &e.Source, &e.Metadata, &e.CreatedAt)

	if err != nil {
		return nil, fmt.Errorf("get event: %w", err)
	}

	return &e, nil
}

func (r *PostgresRepository) GetByID(ctx context.Context, tenantID uuid.UUID, id uuid.UUID) (*Event, error) {

	var e Event

	err := r.db.QueryRow(
		ctx,
		`
		SELECT
			id,
			tenant_id,
			event_type,
			payload,
			occurred_at,
			source,
			metadata,
			created_at
		FROM events
		WHERE id = $1
		  AND tenant_id = $2
		`,
		id,
		tenantID,
	).Scan(&e.ID, &e.TenantID, &e.Type, &e.Payload, &e.OccurredAt, &e.Source, &e.Metadata, &e.CreatedAt)

	if err != nil {
		return nil, fmt.Errorf("get event: %w", err)
	}

	return &e, nil
}
