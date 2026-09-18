package httpapi

import (
	"errors"
	"net/http"

	"github.com/avalokitasharma/HookYard/endpoint-service/internal/tenant"
	"github.com/google/uuid"
)

type TenantHandler struct {
	service *tenant.Service
}

func NewTenantHandler(service *tenant.Service) *TenantHandler {
	return &TenantHandler{service: service}
}

func (h *TenantHandler) Create(w http.ResponseWriter, r *http.Request) {

	var req tenant.CreateRequest
	if err := decodeJSON(w, r, &req, 1<<20); err != nil {
		return
	}
	t, err := h.service.Create(r.Context(), req)
	if err != nil {
		writeTenantError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, t)
}

func (h *TenantHandler) Get(w http.ResponseWriter, r *http.Request) {

	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid tenant ID")
		return
	}
	tenant, err := h.service.Get(r.Context(), id)
	if err != nil {
		writeTenantError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, tenant)

}

func (h *TenantHandler) Patch(w http.ResponseWriter, r *http.Request) {

	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid tenant ID")
		return
	}
	var req tenant.PatchRequest
	if err = decodeJSON(w, r, &req, 1<<20); err != nil {
		return
	}
	tenant, err := h.service.Patch(r.Context(), id, req)
	if err != nil {
		writeTenantError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, tenant)

}

func (h *TenantHandler) Delete(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid tenant ID")
		return
	}
	_, err = h.service.Delete(r.Context(), id)
	if err != nil {
		writeTenantError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func writeTenantError(w http.ResponseWriter, err error) {
	if errors.Is(err, tenant.ErrNotFound) {
		writeError(w, http.StatusNotFound, "tenant not found")
		return
	}
	if errors.Is(err, tenant.ErrConflict) {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	if errors.Is(err, tenant.ErrInvalid) {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeError(w, http.StatusInternalServerError, "internal server error")
}
