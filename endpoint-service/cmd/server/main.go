package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/avalokitasharma/HookYard/endpoint-service/internal/endpoint"
	"github.com/avalokitasharma/HookYard/endpoint-service/internal/httpapi"
	"github.com/avalokitasharma/HookYard/endpoint-service/internal/kafka"
	"github.com/avalokitasharma/HookYard/endpoint-service/internal/outbox"
	"github.com/avalokitasharma/HookYard/endpoint-service/internal/retrypolicy"
	"github.com/avalokitasharma/HookYard/endpoint-service/internal/secrets"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := pgxpool.New(ctx, mustEnv("DATABASE_URL"))
	if err != nil {
		logger.Error("database init failed", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	if err = db.Ping(ctx); err != nil {
		logger.Error("database ping failed", "error", err)
		os.Exit(1)
	}

	cipher, err := secrets.NewFromHex(mustEnv("SECRET_ENCRYPTION_KEY_HEX"))
	if err != nil {
		logger.Error("cipher init failed", "error", err)
		os.Exit(1)
	}

	policies := retrypolicy.NewRepository(db)
	repo := endpoint.NewRepository(db, "hookyard.endpoint-events", "hookyard.endpoint-events")
	svc := endpoint.NewService(repo, policies, cipher, os.Getenv("ALLOW_HTTP_ENDPOINTS") == "true")
	eh := httpapi.NewEndpointHandler(svc)

	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("POST /v1/endpoints", eh.Create)
	mux.HandleFunc("GET /v1/endpoints/{id}", eh.GetEndpoint)
	mux.HandleFunc("PATCH /v1/endpoints/{id}", eh.Patch)
	mux.HandleFunc("DELETE /v1/endpoints/{id}", eh.Delete)
	mux.HandleFunc("GET /v1/endpoints/{id}/subscriptions", eh.GetSubscriptions)

	publisher := kafka.NewPublisher(strings.Split(env("KAFKA_BROKERS", "kafka:9092"), ","))
	defer publisher.Close()

	outboxRepo := outbox.NewRepository(db)
	relay := outbox.NewRelay(outboxRepo, publisher, "endpoint-relay-1", 100, 30*time.Second, logger)
	go relay.Run(ctx)

	srv := &http.Server{
		Addr:              env("HTTP_ADDR", ":8080"),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		logger.Info("endpoint service started", "addr", srv.Addr)
		if e := srv.ListenAndServe(); e != nil && e != http.ErrServerClosed {
			logger.Error("server failed", "error", e)
			stop()
		}
	}()

	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdown)
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		panic("missing environment variable: " + k)
	}
	return v
}
func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
