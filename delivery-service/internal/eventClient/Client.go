package eventClient

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
)

type Event struct {
	ID       uuid.UUID       `json:"id"`
	TenantID uuid.UUID       `json:"tenant_id"`
	Type     string          `json:"type"`
	Data     json.RawMessage `json:"data"`
}

type Client struct {
	baseURL string
	client  *http.Client
}

func New(baseURL string) *Client {
	return &Client{baseURL: baseURL, client: &http.Client{Timeout: 5 * time.Second}}
}

func (c *Client) Get(ctx context.Context, tenantID, eventID uuid.UUID) (*Event, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/v1/events/"+eventID.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Tenant-ID", tenantID.String())
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("event service request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("event service returned %d", resp.StatusCode)
	}
	var e Event
	if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
		return nil, err
	}
	return &e, nil
}
