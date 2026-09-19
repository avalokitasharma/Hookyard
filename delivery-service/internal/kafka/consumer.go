package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	kafkaGo "github.com/segmentio/kafka-go"
)

type Handler func(context.Context, kafkaGo.Message) error

type Consumer struct {
	reader  *kafkaGo.Reader
	handler Handler
	logger  *slog.Logger
}

func NewConsumer(brokers []string, topic, groupID string, handler Handler, logger *slog.Logger) *Consumer {
	return &Consumer{
		reader: kafkaGo.NewReader(kafkaGo.ReaderConfig{
			Brokers:        brokers,
			Topic:          topic,
			GroupID:        groupID,
			MinBytes:       1,
			MaxBytes:       10 << 20,
			MaxWait:        250 * time.Millisecond,
			CommitInterval: 0,
		}),
		handler: handler,
		logger:  logger,
	}
}

func (c *Consumer) Run(ctx context.Context) error {

	for {
		// read
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("fetch Kafka message: %w", err)
		}

		// process
		err = c.handler(ctx, msg)
		if err != nil {
			c.logger.Error("Kafka message processing failed", "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset, "error", err)
			// Do not commit. Kafka will redeliver after the consumer restarts/rebalances.
			time.Sleep(time.Second)
			continue
		}

		// commit offset
		err = c.reader.CommitMessages(ctx, msg)
		if err != nil {
			return fmt.Errorf("commit Kafka message: %w", err)
		}
	}
}

func DecodeEnvelope(msg kafkaGo.Message, dst any) error {
	if err := json.Unmarshal(msg.Value, dst); err != nil {
		return fmt.Errorf("decode Kafka message: %w", err)
	}
	return nil
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
