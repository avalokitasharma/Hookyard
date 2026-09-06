package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/avalokitasharma/HookYard/event-service/internal/event"
	"github.com/google/uuid"
)

type Handler struct {
	service *event.Service
}

func NewHandler(service *event.Service) *Handler {
	return &Handler{
		service: service,
	}
}

func (h *Handler) PublishEvent(w http.ResponseWriter, r *http.Request) {
	tenantID, err := tenantIDFromRequest(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "invalid tenant")
		return
	}

	var req event.PublishRequest

	decoder := json.NewDecoder(r.Body)

	if err := decoder.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return
	}

	if req.IdempotencyKey == "" {
		req.IdempotencyKey = r.Header.Get("Idempotency-Key")
	}

	ctx := r.Context()

	result, err := h.service.Publish(ctx, tenantID, req)
	if err != nil {
		if errors.Is(err, event.ErrInvalidRequest) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		writeError(w, http.StatusInternalServerError, "failed to publish event")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)

	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"event_id": result.ID,
		"status":   "accepted",
	})
}

func (h *Handler) GetEvent(w http.ResponseWriter, r *http.Request) {
	tenantID, err := tenantIDFromRequest(r)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "invalid tenant")
		return
	}

	idString := strings.TrimPrefix(r.URL.Path, "/v1/events/")

	id, err := uuid.Parse(idString)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid event ID")
		return
	}

	result, err := h.service.Get(r.Context(), tenantID, id)
	if err != nil {
		writeError(w, http.StatusNotFound, "event not found")
		return
	}

	w.Header().Set("Content-Type", "application/json")

	_ = json.NewEncoder(w).Encode(result)
}

func tenantIDFromRequest(r *http.Request) (uuid.UUID, error) {
	// The API Gateway will eventually inject this.
	// For now this makes the service independently testable.
	return uuid.Parse(r.Header.Get("X-Tenant-ID"))
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	_ = json.NewEncoder(w).Encode(map[string]string{
		"error": message,
	})
}
