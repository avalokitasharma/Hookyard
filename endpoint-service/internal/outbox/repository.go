package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	db *pgxpool.Pool
}

func NewRepository(db *pgxpool.Pool) *Repository {
	return &Repository{db: db}
}

// Enqueue must run in the same DB transaction as the aggregate mutation.
func Enqueue(ctx context.Context, tx pgx.Tx, topic, key string, payload any) (uuid.UUID, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return uuid.Nil, fmt.Errorf("marshal outbox payload: %w", err)
	}
	id := uuid.New()
	_, err = tx.Exec(ctx, `INSERT INTO outbox_messages(id,topic,message_key,payload,available_at) VALUES($1,$2,$3,$4,NOW())`, id, topic, key, body)
	if err != nil {
		return uuid.Nil, fmt.Errorf("insert outbox message: %w", err)
	}
	return id, nil
}

// lock pending events and subscription changes rows in outbox table

func (r *Repository) Claim(ctx context.Context, worker string, limit int, lease time.Duration) ([]Message, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `
		SELECT id,topic,message_key,payload,attempts,available_at
		FROM outbox_messages
		WHERE published_at IS NULL AND available_at <= NOW()
		  AND (locked_until IS NULL OR locked_until < NOW())
		ORDER BY created_at,id
		FOR UPDATE SKIP LOCKED LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("claim outbox rows: %w", err)
	}
	defer rows.Close()
	var msgs []Message
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.ID, &m.Topic, &m.Key, &m.Payload, &m.Attempts, &m.AvailableAt); err != nil {
			return nil, err
		}
		msgs = append(msgs, m)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, m := range msgs {
		_, err = tx.Exec(ctx, `UPDATE outbox_messages SET locked_by=$2,locked_until=NOW()+$3::interval,attempts=attempts+1 WHERE id=$1`, m.ID, worker, fmt.Sprintf("%f seconds", lease.Seconds()))
		if err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return msgs, nil
}

// release lock and mark successfully published
func (r *Repository) MarkPublished(ctx context.Context, id uuid.UUID, worker string) error {
	_, err := r.db.Exec(ctx, `UPDATE outbox_messages SET published_at=NOW(),locked_by=NULL,locked_until=NULL WHERE id=$1 AND locked_by=$2`, id, worker)
	return err
}

// release lock to be retried later, locked by worked "id" ßfor the next "delay" seconds
func (r *Repository) Release(ctx context.Context, id uuid.UUID, worker string, delay time.Duration) error {
	_, err := r.db.Exec(ctx, `UPDATE outbox_messages SET available_at=NOW()+$3::interval,locked_by=NULL,locked_until=NULL WHERE id=$1 AND locked_by=$2`, id, worker, fmt.Sprintf("%f seconds", delay.Seconds()))
	return err
}
