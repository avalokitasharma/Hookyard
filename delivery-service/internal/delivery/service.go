package delivery

import (
	"context"
	"fmt"
	"time"

	"github.com/avalokitasharma/HookYard/delivery-service/internal/attempt"
	"github.com/avalokitasharma/HookYard/delivery-service/internal/retry"
	"github.com/avalokitasharma/HookYard/delivery-service/internal/secrets"
	"github.com/avalokitasharma/HookYard/delivery-service/internal/webhook"
	"github.com/google/uuid"
)

type EndpointReader interface {
	GetForEvent(context.Context, uuid.UUID, string) ([]EndpointSnapshotWithID, error)
}
type SecretDecryptor interface {
	Decrypt([]byte) ([]byte, error)
}

type Service struct {
	repo      *Repository
	endpoints EndpointReader
	client    *webhook.Client
	decryptor SecretDecryptor
}

func NewService(repo *Repository, endpoints EndpointReader, client *webhook.Client) *Service {
	return &Service{repo: repo, endpoints: endpoints, client: client}
}

func (s *Service) HandleEventAccepted(ctx context.Context, e EventAccepted) error {
	if e.EventID == uuid.Nil || e.TenantID == uuid.Nil || e.EventType == "" {
		return fmt.Errorf("invalid EventAccepted")
	}
	if len(e.Payload) == 0 {
		return fmt.Errorf("EventAccepted missing payload")
	}

	endpoints, err := s.endpoints.GetForEvent(ctx, e.TenantID, e.EventType)
	if err != nil {
		return err
	}
	if len(endpoints) == 0 {
		return nil
	}

	return s.repo.CreateDeliveries(ctx, e, endpoints)
}

func (s *Service) Deliver(ctx context.Context, d Delivery) error {
	secret, err := s.decryptor.Decrypt(d.EndpointSnapshot.SecretEncrypted)
	if err != nil {
		return fmt.Errorf("decrypt endpoint secret: %w", err)
	}

	d.EndpointSnapshot.SecretEncrypted = secret
	result := s.client.Send(ctx, webhook.Request{
		URL:              d.EndpointSnapshot.URL,
		Secret:           secret,
		RequestTimeoutMS: d.EndpointSnapshot.RequestTimeoutMS,
		EventID:          d.EventID.String(),
		EventType:        d.EndpointSnapshot.EventType,
		Body:             d.EventPayload,
	})

	var next *time.Time
	if result.Status == attempt.StatusRetryable && d.AttemptCount < d.EndpointSnapshot.RetryPolicy.MaxAttempts {
		t := time.Now().Add(retry.NextDelay(retry.Policy{
			BackoffType:    d.EndpointSnapshot.RetryPolicy.BackoffType,
			InitialDelayMS: d.EndpointSnapshot.RetryPolicy.InitialDelayMS,
			MaxDelayMS:     d.EndpointSnapshot.RetryPolicy.MaxDelayMS,
			JitterPercent:  d.EndpointSnapshot.RetryPolicy.JitterPercent,
		}, d.AttemptCount))
		next = &t
	}

	return s.repo.RecordAttemptAndTransition(ctx, d, result, next)
}

var _ SecretDecryptor = (*secrets.Cipher)(nil)
