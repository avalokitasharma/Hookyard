package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/avalokitasharma/HookYard/auth-service/internal/model"
	"github.com/avalokitasharma/HookYard/auth-service/internal/repository"
	"github.com/avalokitasharma/HookYard/auth-service/internal/service"
	"github.com/google/uuid"
)

type Handler struct {
	s             *service.Service
	bootstrap     string
	internalToken string
	logger        *slog.Logger
}

func New(s *service.Service, bootstrap, internalToken string, l *slog.Logger) *Handler {
	return &Handler{s: s, bootstrap: bootstrap, internalToken: internalToken, logger: l}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) { write(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("POST /internal/v1/auth/verify", h.verify)
	mux.Handle("POST /internal/v1/tenants", h.admin(http.HandlerFunc(h.createTenant)))
	mux.Handle("GET /internal/v1/tenants", h.admin(http.HandlerFunc(h.listTenants)))
	mux.Handle("GET /internal/v1/tenants/{tenantID}", h.admin(http.HandlerFunc(h.getTenant)))
	// mux.Handle("PATCH /internal/v1/tenants/{tenantID}", h.admin(http.HandlerFunc(h.updateTenant)))
	// mux.Handle("DELETE /internal/v1/tenants/{tenantID}", h.admin(http.HandlerFunc(h.deleteTenant)))
	// mux.Handle("POST /internal/v1/tenants/{tenantID}/keys", h.admin(http.HandlerFunc(h.createKey)))
	// mux.Handle("GET /internal/v1/tenants/{tenantID}/keys", h.admin(http.HandlerFunc(h.listKeys)))
	// mux.Handle("DELETE /internal/v1/tenants/{tenantID}/keys/{keyID}", h.admin(http.HandlerFunc(h.revokeKey)))
}

func (h *Handler) admin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.bootstrap == "" || !constantEqual(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "), h.bootstrap) {
			write(w, 401, map[string]string{"error": "unauthorized"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (h *Handler) verify(w http.ResponseWriter, r *http.Request) {
	if h.internalToken == "" || !constantEqual(r.Header.Get("X-Auth-Internal-Token"), h.internalToken) {
		write(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	var req struct {
		APIKey string `json:"api_key"`
	}
	if decode(r, &req) != nil || req.APIKey == "" {
		write(w, 400, map[string]string{"error": "invalid request"})
		return
	}
	ctx, cancel := contextWithTimeout(r)
	defer cancel()
	result, err := h.s.Verify(ctx, req.APIKey)
	if err != nil {
		h.logger.Error("verify failed", "error", err)
		write(w, 500, map[string]string{"error": "internal error"})
		return
	}
	write(w, 200, result)
}

func (h *Handler) createTenant(w http.ResponseWriter, r *http.Request) {
	var req model.CreateTenantRequest
	if decode(r, &req) != nil {
		write(w, 400, map[string]string{"error": "invalid JSON"})
		return
	}
	ctx, cancel := contextWithTimeout(r)
	defer cancel()
	t, k, err := h.s.CreateTenant(ctx, req, r.Header.Get("X-Admin-Actor"))
	if err != nil {
		status := 400
		if errors.Is(err, repository.ErrConflict) {
			status = 409
		}
		write(w, status, map[string]string{"error": err.Error()})
		return
	}
	write(w, 201, map[string]any{"tenant": t, "key": k})
}

func (h *Handler) listTenants(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := contextWithTimeout(r)
	defer cancel()
	tenants, err := h.s.ListTenants(ctx)
	if err != nil {
		h.logger.Error("list tenants failed", "error", err)
		write(w, 500, map[string]string{"error": "internal error"})
		return
	}
	write(w, 200, map[string]any{"tenants": tenants})
}

func (h *Handler) getTenant(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("tenantID"))
	if err != nil {
		write(w, 400, map[string]string{"error": "invalid tenantID"})
		return
	}
	ctx, cancel := contextWithTimeout(r)
	defer cancel()
	t, err := h.s.GetTenant(ctx, id)
	if errors.Is(err, repository.ErrNotFound) {
		write(w, 404, map[string]string{"error": "tenant not found"})
		return
	}
	if err != nil {
		h.logger.Error("get tenant failed", "error", err)
		write(w, 500, map[string]string{"error": "internal error"})
		return
	}
	write(w, 200, t)
}

func decode(r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(nil, r.Body, 1<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	return d.Decode(v)
}
func write(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func constantEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	var x byte
	for i := range a {
		x |= a[i] ^ b[i]
	}
	return x == 0
}
func contextWithTimeout(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), 3*time.Second)
}
