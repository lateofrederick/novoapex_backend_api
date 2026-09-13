// Command api serves the HTTP surface: auth, the vendor mobile API, provider
// webhooks, manual message sends, health, metrics, API docs and the queue
// dashboard.
//
// With -mobile it runs like the standalone apps/mobile-api entrypoint instead:
// only the mobile API modules, listening on MOBILE_API_PORT.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5"

	"github.com/novoapex/novoapex-backend-api/internal/config"
	"github.com/novoapex/novoapex-backend-api/internal/db"
	"github.com/novoapex/novoapex-backend-api/internal/httpx"
	"github.com/novoapex/novoapex-backend-api/internal/integrations/paystack"
	"github.com/novoapex/novoapex-backend-api/internal/integrations/whatsapp"
	"github.com/novoapex/novoapex-backend-api/internal/observability"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
	"github.com/novoapex/novoapex-backend-api/internal/storage"
)

func main() {
	mobileOnly := flag.Bool("mobile", false, "serve only the mobile API on MOBILE_API_PORT (standalone apps/mobile-api)")
	flag.Parse()

	cfg, err := config.Load()
	if err != nil {
		slog.New(slog.NewJSONHandler(os.Stdout, nil)).Error("load config", slog.Any("error", err))
		os.Exit(1)
	}

	service := "api"
	if *mobileOnly {
		service = "mobile-api"
	}
	logger, sentryOn, flush := observability.Bootstrap(observability.BootstrapConfig{
		Service: service, NodeEnv: cfg.NodeEnv, LogLevel: cfg.LogLevel,
	})
	defer flush()
	var middleware []func(http.Handler) http.Handler
	var dbTracer pgx.QueryTracer
	if sentryOn {
		middleware = append(middleware, observability.HTTPMiddleware)
		dbTracer = observability.PGXTracer{}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	pool, err := db.NewPool(ctx, cfg.DatabaseURL, db.PoolConfig{
		MaxConns:    int32(cfg.DatabasePoolMax),
		IdleTimeout: time.Duration(cfg.DatabasePoolIdleTimeoutMS) * time.Millisecond,
		ConnTimeout: time.Duration(cfg.DatabasePoolConnectionTimeoutMS) * time.Millisecond,
		Tracer:      dbTracer,
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

	redisAddr := net.JoinHostPort(cfg.RedisHost, strconv.Itoa(cfg.RedisPort))
	qclient := queue.NewClientWithConfig(queue.ClientConfig{
		RedisAddr: redisAddr,
		RedisPass: cfg.RedisPassword,
		RedisTLS:  cfg.RedisTLS,
		Logger:    logger,
	})
	defer func() { _ = qclient.Close() }()

	queueRedis := asynq.RedisClientOpt{Addr: redisAddr, Password: cfg.RedisPassword}
	if cfg.RedisTLS {
		queueRedis.TLSConfig = queue.TLSServerConfig(cfg.RedisHost)
	}
	if !*mobileOnly {
		// QueueProducerModule registers every queue; declare them so the
		// dashboard lists each one before its first job.
		if err := queue.DeclareQueues(ctx, queueRedis); err != nil {
			logger.Warn("declare queues for the dashboard", slog.Any("error", err))
		}
	}

	kernel, closeRouter, err := buildRouter(routerDeps{
		cfg:        cfg,
		pool:       pool,
		rdb:        rdb,
		publisher:  qclient,
		queueRedis: queueRedis,
		storage:    storage.NewDefaultProvider(cfg),
		wa:         whatsapp.New(cfg.WhatsAppAPIVersion, cfg.WhatsAppAccessToken),
		paystack:   paystack.New(paystack.Config{SecretKey: cfg.PaystackSecretKey, BaseURL: cfg.PaystackBaseURL}),
		mobileOnly: *mobileOnly,
		middleware: middleware,
	})
	if err != nil {
		logger.Error("build router", slog.Any("error", err))
		os.Exit(1)
	}
	defer func() { _ = closeRouter() }()

	port := cfg.Port
	if *mobileOnly {
		port = cfg.MobileAPIPort
	}
	addr := ":" + strconv.Itoa(port)
	srv, err := kernel.Start(addr)
	if err != nil {
		logger.Error("start server", slog.String("addr", addr), slog.Any("error", err))
		os.Exit(1)
	}
	logger.Info("Application running on port "+strconv.Itoa(port), slog.String("service", service))

	<-ctx.Done()
	kernel.BeginShutdown()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpx.Shutdown(shutdownCtx, srv); err != nil {
		logger.Error("graceful shutdown failed", slog.Any("error", err))
	}
}
