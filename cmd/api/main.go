package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/novoapex/novoapex-backend-api/internal/auth"
	"github.com/novoapex/novoapex-backend-api/internal/config"
	"github.com/novoapex/novoapex-backend-api/internal/db"
	"github.com/novoapex/novoapex-backend-api/internal/handlers"
	"github.com/novoapex/novoapex-backend-api/internal/httpx"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		logger.Error("load config", slog.Any("error", err))
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	pool, err := db.NewPool(ctx, cfg.DatabaseURL, db.PoolConfig{
		MaxConns:    int32(cfg.DatabasePoolMax),
		IdleTimeout: time.Duration(cfg.DatabasePoolIdleTimeoutMS) * time.Millisecond,
		ConnTimeout: time.Duration(cfg.DatabasePoolConnectionTimeoutMS) * time.Millisecond,
	})
	if err != nil {
		logger.Error("connect database", slog.Any("error", err))
		os.Exit(1)
	}
	defer pool.Close()

	rdb, err := db.NewRedis(db.RedisConfig{
		Host:     cfg.RedisHost,
		Port:     cfg.RedisPort,
		Password: cfg.RedisPassword,
		TLS:      cfg.RedisTLS,
	})
	if err != nil {
		logger.Error("configure redis", slog.Any("error", err))
		os.Exit(1)
	}
	defer func() { _ = rdb.Close() }()

	kernel := httpx.New()
	kernel.MountHealth(httpx.HealthDeps{
		DB:    httpx.NewPGPinger(pool),
		Redis: httpx.NewRedisPinger(rdb),
	})

	kernel.Router.NotFound(func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusNotFound,
			fmt.Sprintf("Cannot %s %s", r.Method, r.URL.Path)))
	})

	vendor := []struct {
		prefix string
		h      func(*pgxpool.Pool) http.Handler
	}{
		{"/customers", handlers.NewCustomers},
		{"/products", handlers.NewProducts},
		{"/orders", handlers.NewOrders},
		{"/inbox", handlers.NewInbox},
		{"/dashboard", handlers.NewDashboard},
		{"/analytics", handlers.NewAnalytics},
		{"/payouts", handlers.NewPayouts},
	}
	kernel.Router.Group(func(v chi.Router) {
		v.Use(auth.Middleware(cfg.JWTSecret, nil))
		v.Handle("/", handlers.NewRoot())
		for _, m := range vendor {
			v.Mount(m.prefix, m.h(pool))
		}
		v.Mount("/conversations", handlers.NewConversations(pool))
	})

	addr := ":" + itoa(cfg.Port)
	srv, err := kernel.Start(addr)
	if err != nil {
		logger.Error("start server", slog.String("addr", addr), slog.Any("error", err))
		os.Exit(1)
	}
	logger.Info("api listening", slog.String("addr", addr))

	<-ctx.Done()
	kernel.BeginShutdown()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpx.Shutdown(shutdownCtx, srv); err != nil {
		logger.Error("graceful shutdown failed", slog.Any("error", err))
	}
}

func itoa(n int) string {
	return strconv.Itoa(n)
}
