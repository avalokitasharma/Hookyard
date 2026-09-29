package kafka

import (
	"context"
	"log/slog"
	"strconv"

	"github.com/avalokitasharma/HookYard/delivery-service/internal/delivery"
	"github.com/avalokitasharma/HookYard/delivery-service/internal/eventClient"
	kafkaGo "github.com/segmentio/kafka-go"
)

type EventHandler struct {
	deliveryService *delivery.Service
	eventClient     *eventClient.Client
	logger          *slog.Logger
}

func NewEventHandler(deliveryService *delivery.Service, eventClient *eventClient.Client, logger *slog.Logger) *EventHandler {
	return &EventHandler{
		deliveryService: deliveryService,
		eventClient:     eventClient,
		logger:          logger,
	}
}

func (h *EventHandler) Handle(ctx context.Context, msg kafkaGo.Message) error {
	var e delivery.EventAccepted
	if err := DecodeEnvelope(msg, &e); err != nil {
		return err
	}
	if e.MessageID == "" {
		e.MessageID = strconv.FormatInt(msg.Offset, 10)
	}
	if len(e.Payload) == 0 {
		fetched, err := h.eventClient.Get(ctx, e.TenantID, e.EventID)
		if err != nil {
			return err
		}
		e.Payload = fetched.Data
		e.EventType = fetched.Type
	}
	return h.deliveryService.HandleEventAccepted(ctx, e)
}
