package cmd

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/avalokitasharma/HookYard/auth-service/internal/httpapi"
	"github.com/avalokitasharma/HookYard/auth-service/internal/repository"
	"github.com/avalokitasharma/HookYard/auth-service/internal/service"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		logger.Error("DATABASE_URL is required")
		os.Exit(1)
	}
	bootstrap := os.Getenv("AUTH_BOOTSTRAP_TOKEN")

	internalToken := os.Getenv("AUTH_INTERNAL_TOKEN")
	if internalToken == "" {
		logger.Error("AUTH_INTERNAL_TOKEN is required")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		logger.Error("database init failed", "error", err)
		os.Exit(1)
	}

	defer db.Close()
	if err = db.Ping(ctx); err != nil {
		logger.Error("database ping failed", "error", err)
		os.Exit(1)
	}

	h := httpapi.New(service.New(repository.New(db)), bootstrap, internalToken, logger)
	mux := http.NewServeMux()
	h.Register(mux)

	srv := &http.Server{
		Addr:              ":8082",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		logger.Info("auth service started", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server failed", "error", err)
			stop()
		}
	}()
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdown)

}
