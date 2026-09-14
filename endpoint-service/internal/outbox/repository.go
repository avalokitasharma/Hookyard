package outbox

import (
	"context"
	"encoding/json"
	"fmt"

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
