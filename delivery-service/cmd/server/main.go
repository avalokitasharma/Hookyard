package main

import (
	"context"
	"encoding/hex"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/avalokitasharma/HookYard/delivery-service/internal/delivery"
	"github.com/avalokitasharma/HookYard/delivery-service/internal/endpoint"
	"github.com/avalokitasharma/HookYard/delivery-service/internal/eventClient"
	"github.com/avalokitasharma/HookYard/delivery-service/internal/httpapi"
	hkafka "github.com/avalokitasharma/HookYard/delivery-service/internal/kafka"
	"github.com/avalokitasharma/HookYard/delivery-service/internal/scheduler"
	"github.com/avalokitasharma/HookYard/delivery-service/internal/secrets"
	"github.com/avalokitasharma/HookYard/delivery-service/internal/webhook"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	// logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// connect to DB : new pool
	db, err := pgxpool.New(ctx, mustEnv("DATABASE_URL"))
	if err != nil {
		logger.Error("postgres init failed", "error", err)
		os.Exit(1)
	}
	defer db.Close()
	// ping DB : check conn
	if err := db.Ping(ctx); err != nil {
		logger.Error("postgres ping failed", "error", err)
		os.Exit(1)
	}
	// get private decrytption key
	key, err := hex.DecodeString(mustEnv("SECRET_ENCRYPTION_KEY_HEX"))
	if err != nil || (len(key) != 16 && len(key) != 24 && len(key) != 32) {
		logger.Error("invalid secret key", "error", err)
		os.Exit(1)
	}
	decryptor, err := secrets.New(key)

	endpointRepo := endpoint.NewRepository(db)
	deliveryRepo := delivery.NewRepository(db)

	deliveryService := delivery.NewService(deliveryRepo, endpointRepo, webhook.NewClient(), decryptor)
	eventClient := eventClient.New(mustEnv("EVENT_SERVICE_URL"))

	eventHandler := hkafka.NewEventHandler(deliveryService, eventClient, logger)
	endpointHandler := hkafka.NewEndpointHandler(endpointRepo)

	brokers := splitEnv("KAFKA_BROKERS", "localhost:9092")
	eventConsumer := hkafka.NewConsumer(brokers, "hookyard.events", "delivery-service", eventHandler.Handle, logger)
	endpointConsumer := hkafka.NewConsumer(brokers, "hookyard.endpoint-events", "delivery-service-endpoints", endpointHandler.Handle, logger)
	defer eventConsumer.Close()
	defer endpointConsumer.Close()

	concurrency := envInt("DELIVERY_CONCURRENCY", 50)
	batchSize := envInt("DELIVERY_BATCH_SIZE", 50)
	leaseSeconds := envInt("DELIVERY_LEASE_SECONDS", 30)
	sched := scheduler.New(deliveryRepo, deliveryService, concurrency, batchSize, time.Duration(leaseSeconds)*time.Second, logger)

	go runConsumer(ctx, eventConsumer, logger)
	go runConsumer(ctx, endpointConsumer, logger)
	go sched.Run(ctx)

	h := httpapi.NewHandler(deliveryService)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", h.Health)
	mux.HandleFunc("GET /v1/deliveries/{id}", h.GetDelivery)
	mux.HandleFunc("POST /v1/deliveries/{id}/replay", h.ReplayDelivery)

	server := &http.Server{
		Addr:              envString("HTTP_ADDR", ":8081"),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		logger.Info("delivery service started", "addr", server.Addr)
		err := server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			logger.Error("http server failed", "error", err)
			stop()
		}
	}()
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdown)

}

func runConsumer(ctx context.Context, c *hkafka.Consumer, logger *slog.Logger) {
	if err := c.Run(ctx); err != nil && ctx.Err() == nil {
		logger.Error("Kafka consumer stopped", "error", err)
	}
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		panic("missing environment variable: " + k)
	}
	return v
}
func splitEnv(k, fallback string) []string {
	v := os.Getenv(k)
	if v == "" {
		v = fallback
	}
	return strings.Split(v, ",")
}

func envInt(k string, fallback int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}
func envString(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}
