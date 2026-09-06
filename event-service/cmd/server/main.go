package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/avalokitasharma/HookYard/event-service/internal/db"
	"github.com/avalokitasharma/HookYard/event-service/internal/event"
	"github.com/avalokitasharma/HookYard/event-service/internal/httpapi"
	"github.com/avalokitasharma/HookYard/event-service/internal/outbox"
)

func main() {
	logger := slog.New(
		slog.NewJSONHandler(
			os.Stdout,
			nil,
		),
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	dsn := os.Getenv("DATABASE_URL")

	pool, err := db.NewPostgres(ctx, dsn)
	if err != nil {
		logger.Error("database initialization failed", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	eventRepo := event.NewPostgresRepository(pool)
	eventService := event.NewService(eventRepo)

	handler := httpapi.NewHandler(eventService)

	mux := http.NewServeMux()

	mux.HandleFunc("POST /v1/events", handler.PublishEvent)

	mux.HandleFunc("GET /v1/events/{id}", handler.GetEvent)

	server := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// Outbox publisher.
	brokers := []string{
		os.Getenv("KAFKA_BROKER"),
	}

	publisher := outbox.NewPublisher(pool, brokers, "hookyard.events", logger)

	go publisher.Run(ctx)

	go func() {
		logger.Info("event service started", "addr", ":8080")

		if err := server.ListenAndServe(); err != nil &&
			err != http.ErrServerClosed {
			logger.Error("http server failed", "error", err)

			stop()
		}
	}()

	<-ctx.Done()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("server shutdown failed", "error", err)
	}
}
