package httpapi

import (
	"errors"
	"net/http"
	"strings"

	"github.com/avalokitasharma/HookYard/endpoint-service/internal/endpoint"
	"github.com/google/uuid"
)

type EndpointHandler struct {
	service *endpoint.Service
	maxBody int64
}

func NewEndpointHandler(service *endpoint.Service) *EndpointHandler {
	return &EndpointHandler{service: service, maxBody: 1 << 20}
}

func (h *EndpointHandler) Create(w http.ResponseWriter, r *http.Request) {
	tenant, err := tenantID(r)
	if err != nil {
		writeError(w, 401, "invalid tenant")
		return
	}
	var req endpoint.CreateRequest
	if err = decodeJSON(w, r, &req, h.maxBody); err != nil {
		return
	}
	endpoint, err := h.service.Create(r.Context(), tenant, req)
	if err != nil {
		writeEndpointError(w, err)
		return
	}
	writeJSON(w, 201, endpoint.PublicView())

}

func (h *EndpointHandler) GetEndpoint(w http.ResponseWriter, r *http.Request) {
	tenant, err := tenantID(r)
	if err != nil {
		writeError(w, 401, "invalid tenant")
		return
	}
	id, err := parseID(r)
	if err != nil {
		writeError(w, 400, "invalid endpoint ID")
		return
	}
	e, p, err := h.service.Get(r.Context(), tenant, id)
	if errors.Is(err, endpoint.ErrNotFound) {
		writeError(w, 404, "endpoint not found")
		return
	}
	if err != nil {
		writeError(w, 500, "failed to get endpoint")
		return
	}
	v := e.PublicView()
	v["retry_policy"] = p
	writeJSON(w, 200, v)
}

func (h *EndpointHandler) Patch(w http.ResponseWriter, r *http.Request) {
	tenant, err := tenantID(r)
	if err != nil {
		writeError(w, 401, "invalid tenant")
		return
	}
	id, err := parseID(r)
	if err != nil {
		writeError(w, 400, "invalid endpoint ID")
		return
	}
	var req endpoint.PatchRequest
	if err = decodeJSON(w, r, &req, h.maxBody); err != nil {
		return
	}
	e, p, err := h.service.Patch(r.Context(), tenant, id, req)
	if err != nil {
		writeEndpointError(w, err)
		return
	}
	v := e.PublicView()
	v["retry_policy"] = p
	writeJSON(w, 200, v)
}

func (h *EndpointHandler) Delete(w http.ResponseWriter, r *http.Request) {
	tenant, err := tenantID(r)
	if err != nil {
		writeError(w, 401, "invalid tenant")
		return
	}
	id, err := parseID(r)
	if err != nil {
		writeError(w, 400, "invalid endpoint ID")
		return
	}
	_, _, err = h.service.Delete(r.Context(), tenant, id)
	if errors.Is(err, endpoint.ErrNotFound) {
		writeError(w, 404, "endpoint not found")
		return
	}
	if err != nil {
		writeEndpointError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *EndpointHandler) GetSubscriptions(w http.ResponseWriter, r *http.Request) {
	tenant, err := tenantID(r)
	if err != nil {
		writeError(w, 401, "invalid tenant")
		return
	}
	eventType := r.PathValue("event_type")
	eventType, err = decodePath(eventType)
	if err != nil {
		writeError(w, 400, "invalid event type")
		return
	}
	eps, err := h.service.Subscriptions(r.Context(), tenant, eventType)
	if err != nil {
		writeEndpointError(w, err)
		return
	}
	out := make([]map[string]any, 0, len(eps))
	for _, e := range eps {
		out = append(out, map[string]any{"endpoint_id": e.ID, "url": e.URL})
	}
	writeJSON(w, 200, out)
}

func tenantID(r *http.Request) (uuid.UUID, error) {
	return uuid.Parse(strings.TrimSpace(r.Header.Get("X-Tenant-ID")))
}

func writeEndpointError(w http.ResponseWriter, err error) {
	if errors.Is(err, endpoint.ErrNotFound) {
		writeError(w, http.StatusNotFound, "endpoint not found")
		return
	}
	if errors.Is(err, endpoint.ErrConflict) {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	if errors.Is(err, endpoint.ErrInvalid) {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeError(w, http.StatusInternalServerError, "internal server error")
}
