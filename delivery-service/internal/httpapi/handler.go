package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/avalokitasharma/HookYard/delivery-service/internal/delivery"
	"github.com/google/uuid"
)

type Handler struct {
	service *delivery.Service
}

func NewHandler(service *delivery.Service) *Handler {
	return &Handler{
		service: service,
	}
}

func (h *Handler) GetDelivery(w http.ResponseWriter, r *http.Request) {
	tenantID, err := tenantID(r)
	if err != nil {
		writeError(w, 401, "invalid tenant")
		return
	}
	id, err := uuid.Parse(strings.TrimPrefix(r.URL.Path, "/v1/deliveries/"))
	if err != nil {
		writeError(w, 400, "invalid delivery ID")
		return
	}
	d, err := h.service.Get(r.Context(), tenantID, id)
	if errors.Is(err, delivery.ErrNotFound) {
		writeError(w, 404, "delivery not found")
		return
	}
	if err != nil {
		writeError(w, 500, "failed to get delivery")
		return
	}
	response := map[string]any{
		"id": d.ID, "event_id": d.EventID, "endpoint_id": d.EndpointID,
		"status": d.Status, "attempt_count": d.AttemptCount,
		"next_attempt_at": d.NextAttemptAt, "last_attempt_at": d.LastAttemptAt,
		"last_status_code": d.LastStatusCode, "last_error_code": d.LastErrorCode,
		"last_error_message": d.LastErrorMessage, "replay_of_delivery_id": d.ReplayOfDeliveryID,
		"created_at": d.CreatedAt, "updated_at": d.UpdatedAt, "completed_at": d.CompletedAt,
	}
	writeJSON(w, 200, response)

}
func (h *Handler) ListDeliveries(w http.ResponseWriter, r *http.Request) {
	tenantID, err := tenantID(r)
	if err != nil {
		writeError(w, 401, "invalid tenant")
		return
	}

	filter, err := parseListFilter(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	page, err := h.service.List(r.Context(), tenantID, filter)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, deliveryPageResponse(page))
}
func deliveryPageResponse(page delivery.DeliveryPage) map[string]any {
	items := make([]map[string]any, 0, len(page.Items))
	for i := range page.Items {
		items = append(items, deliveryView(&page.Items[i]))
	}
	return map[string]any{"items": items, "next_cursor": page.NextCursor}
}

func deliveryView(d *delivery.Delivery) map[string]any {
	return map[string]any{
		"id": d.ID, "event_id": d.EventID, "endpoint_id": d.EndpointID,
		"status": d.Status, "attempt_count": d.AttemptCount,
		"next_attempt_at": d.NextAttemptAt, "last_attempt_at": d.LastAttemptAt,
		"last_status_code": d.LastStatusCode, "last_error_code": d.LastErrorCode,
		"replay_of_delivery_id": d.ReplayOfDeliveryID,
		"created_at":            d.CreatedAt, "updated_at": d.UpdatedAt, "completed_at": d.CompletedAt,
	}
}

func (h *Handler) ReplayDelivery(w http.ResponseWriter, r *http.Request) {
	tenantID, err := tenantID(r)
	if err != nil {
		writeError(w, 401, "invalid tenant")
		return
	}
	id, err := uuid.Parse(strings.TrimPrefix(r.URL.Path, "/v1/deliveries/"))
	if err != nil {
		writeError(w, 400, "invalid delivery ID")
		return
	}
	newID, err := h.service.Replay(r.Context(), tenantID, id)
	if errors.Is(err, delivery.ErrNotFound) {
		writeError(w, 404, "delivery not found")
		return
	}
	if err != nil {
		writeError(w, 409, err.Error())
		return
	}
	writeJSON(w, 202, map[string]any{"delivery_id": newID, "status": "queued"})
}

func (h *Handler) Health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, map[string]string{"status": "ok"})
}

func tenantID(r *http.Request) (uuid.UUID, error) {
	return uuid.Parse(r.Header.Get("X-Tenant-ID"))
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func parseListFilter(r *http.Request) (delivery.ListFilter, error) {
	raw := r.URL.Query().Get("limit")

	limit, err := strconv.Atoi(raw)
	if err != nil || limit < 1 || limit > 100 {
		limit = 50
	}

	filter := delivery.ListFilter{
		Limit:  limit,
		Cursor: r.URL.Query().Get("cursor"),
		Status: r.URL.Query().Get("status"),
	}
	if raw := r.URL.Query().Get("endpoint_id"); raw != "" {
		id, parseErr := uuid.Parse(raw)
		if parseErr != nil {
			return filter, errors.New("invalid endpoint_id")
		}
		filter.EndpointID = &id
	}
	if raw := r.URL.Query().Get("event_id"); raw != "" {
		id, parseErr := uuid.Parse(raw)
		if parseErr != nil {
			return filter, errors.New("invalid event_id")
		}
		filter.EventID = &id
	}
	return filter, nil
}
