package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

type Publisher struct {
	db     *pgxpool.Pool
	writer *kafka.Writer
	logger *slog.Logger
}

type Message struct {
	ID            string          `json:"id"`
	EventType     string          `json:"event_type"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   string          `json:"aggregate_id"`
	Payload       json.RawMessage `json:"payload"`
}

func NewPublisher(db *pgxpool.Pool, brokers []string, topic string, logger *slog.Logger) *Publisher {

	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}

	return &Publisher{
		db:     db,
		writer: writer,
		logger: logger,
	}
}

func (p *Publisher) Run(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			if err := p.publishBatch(ctx); err != nil {
				p.logger.Error(
					"outbox publish failed",
					"error", err,
				)
			}
		}
	}
}

func (p *Publisher) publishBatch(ctx context.Context) error {
	tx, err := p.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(
		ctx,
		`
		SELECT
			id,
			aggregate_type,
			aggregate_id,
			event_type,
			payload
		FROM outbox
		WHERE published_at IS NULL
		ORDER BY created_at
		FOR UPDATE SKIP LOCKED
		LIMIT 100
		`,
	)

	if err != nil {
		return fmt.Errorf("query outbox: %w", err)
	}
	defer rows.Close()

	type outboxRow struct {
		id            string
		aggregateType string
		aggregateID   string
		eventType     string
		payload       []byte
	}

	var messages []outboxRow

	for rows.Next() {
		var row outboxRow

		if err := rows.Scan(
			&row.id,
			&row.aggregateType,
			&row.aggregateID,
			&row.eventType,
			&row.payload,
		); err != nil {
			return fmt.Errorf("scan outbox: %w", err)
		}

		messages = append(messages, row)
	}

	for _, row := range messages {
		message := Message{
			ID:            row.id,
			EventType:     row.eventType,
			AggregateType: row.aggregateType,
			AggregateID:   row.aggregateID,
			Payload:       row.payload,
		}

		payload, err := json.Marshal(message)
		if err != nil {
			return fmt.Errorf("marshal message: %w", err)
		}

		err = p.writer.WriteMessages(
			ctx,
			kafka.Message{
				Key:   []byte(row.aggregateID),
				Value: payload,
			},
		)

		if err != nil {
			_, updateErr := tx.Exec(
				ctx,
				`
				UPDATE outbox
				SET attempts = attempts + 1,
				    last_error = $2
				WHERE id = $1
				`,
				row.id,
				err.Error(),
			)

			if updateErr != nil {
				return fmt.Errorf(
					"update failed outbox: %w",
					updateErr,
				)
			}

			continue
		}

		_, err = tx.Exec(
			ctx,
			`
			UPDATE outbox
			SET published_at = NOW()
			WHERE id = $1
			`,
			row.id,
		)

		if err != nil {
			return fmt.Errorf(
				"mark outbox published: %w",
				err,
			)
		}
	}

	return tx.Commit(ctx)
}
