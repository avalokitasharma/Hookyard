package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/avalokitasharma/HookYard/endpoint-service/internal/outbox"
	"github.com/segmentio/kafka-go"
)

type Publisher struct {
	writer *kafka.Writer
}

func NewPublisher(brokers []string) *Publisher {
	return &Publisher{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
			BatchTimeout: 20 * time.Millisecond,
			WriteTimeout: 10 * time.Second,
			ReadTimeout:  10 * time.Second,
			Async:        false,
		},
	}
}

func (p *Publisher) Close() error {
	return p.writer.Close()
}

func (p *Publisher) Publish(ctx context.Context, m outbox.Message) error {
	err := p.writer.WriteMessages(ctx,
		kafka.Message{
			Topic: m.Topic,
			Key:   []byte(m.Key),
			Value: m.Payload,
			Headers: []kafka.Header{
				{Key: "content-type", Value: []byte("application/json")},
				{Key: "message-id", Value: []byte(m.ID.String())},
			},
		})
	if err != nil {
		return fmt.Errorf("publish outbox message: %w", err)
	}
	return nil
}
