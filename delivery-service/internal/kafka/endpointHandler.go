package kafka

import (
	"context"

	"github.com/avalokitasharma/HookYard/delivery-service/internal/endpoint"
	kafkaGo "github.com/segmentio/kafka-go"
)

type EndpointHandler struct {
	repository *endpoint.Repository
}

func NewEndpointHandler(repository *endpoint.Repository) *EndpointHandler {
	return &EndpointHandler{repository: repository}
}

func (h *EndpointHandler) Handle(ctx context.Context, msg kafkaGo.Message) error {
	var e endpoint.EndpointChanged
	if err := DecodeEnvelope(msg, &e); err != nil {
		return err
	}

	return h.repository.Upsert(ctx, endpoint.EndpointChanged{
		MessageID:        e.MessageID,
		TenantID:         e.TenantID,
		EndpointID:       e.EndpointID,
		Name:             e.Name,
		URL:              e.URL,
		SecretEncrypted:  e.SecretEncrypted,
		Status:           e.Status,
		ConnectTimeoutMS: e.ConnectTimeoutMS,
		RequestTimeoutMS: e.RequestTimeoutMS,
		RetryPolicy:      e.RetryPolicy,
		Version:          e.Version,
		UpdatedAt:        e.UpdatedAt,
		EventTypes:       e.EventTypes,
	})
}
