// Command worker is the headless queue-consumer process (apps/worker): every
// asynq task handler plus the cron sweeps, with graceful SIGTERM shutdown.
// Scale it independently of the API so an LLM burst cannot degrade webhook
// ingestion.
package main

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5"

	"github.com/novoapex/novoapex-backend-api/internal/config"
	"github.com/novoapex/novoapex-backend-api/internal/db"
	"github.com/novoapex/novoapex-backend-api/internal/events"
	"github.com/novoapex/novoapex-backend-api/internal/integrations/google"
	"github.com/novoapex/novoapex-backend-api/internal/integrations/openai"
	"github.com/novoapex/novoapex-backend-api/internal/integrations/paystack"
	"github.com/novoapex/novoapex-backend-api/internal/integrations/whatsapp"
	"github.com/novoapex/novoapex-backend-api/internal/observability"
	"github.com/novoapex/novoapex-backend-api/internal/orchestrator"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
	"github.com/novoapex/novoapex-backend-api/internal/workers"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.New(slog.NewJSONHandler(os.Stdout, nil)).Error("load config", slog.Any("error", err))
		os.Exit(1)
	}

	logger, sentryOn, flush := observability.Bootstrap(observability.BootstrapConfig{
		Service: "worker", NodeEnv: cfg.NodeEnv, LogLevel: cfg.LogLevel,
	})
	defer flush()
	var dbTracer pgx.QueryTracer
	var taskMiddleware func(string, queue.Handler) queue.Handler
	if sentryOn {
		queue.SentryHook = observability.CaptureTaskFailure
		dbTracer = observability.PGXTracer{}
		taskMiddleware = func(taskType string, next queue.Handler) queue.Handler {
			return func(ctx context.Context, payload []byte) error {
				return observability.TraceTask(ctx, taskType, func(ctx context.Context) error { return next(ctx, payload) })
			}
		}
	}
	defer observability.CapturePanic(context.Background())

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

	redisAddr := net.JoinHostPort(cfg.RedisHost, strconv.Itoa(cfg.RedisPort))

	client := queue.NewClientWithConfig(queue.ClientConfig{
		RedisAddr: redisAddr,
		RedisPass: cfg.RedisPassword,
		RedisTLS:  cfg.RedisTLS,
		Logger:    logger,
	})
	defer func() { _ = client.Close() }()

	server := queue.NewServer(queue.ServerConfig{
		RedisAddr:       redisAddr,
		RedisPass:       cfg.RedisPassword,
		RedisTLS:        cfg.RedisTLS,
		Concurrency:     workerConcurrency(),
		GracefulTimeout: queue.DefaultGracefulTimeout,
		Middleware:      taskMiddleware,
		Logger:          logger,
	})
	defer func() { _ = server.Close() }()

	openaiBaseURL := os.Getenv("OPENAI_BASE_URL")
	openaiClient := openai.New(openai.Config{APIKey: cfg.OpenAIAPIKey, BaseURL: openaiBaseURL})
	gemini := google.New(google.Config{APIKey: cfg.GoogleGenerativeAIAPIKey, BaseURL: os.Getenv("GOOGLE_GENERATIVE_AI_BASE_URL")})
	wa := whatsapp.New(cfg.WhatsAppAPIVersion, cfg.WhatsAppAccessToken)
	psc := paystack.New(paystack.Config{SecretKey: cfg.PaystackSecretKey, BaseURL: cfg.PaystackBaseURL})

	// Inbound: webhook-processing persists the message and debounces the
	// orchestrator run.
	workers.RegisterWebhookProcessing(server, workers.WebhookDeps{Pool: pool, Publisher: client})

	// Conversation pipeline.
	workers.RegisterOrchestrator(server, workers.OrchestratorDeps{
		Pool:      pool,
		Publisher: client,
		LLM:       orchestrator.NewResponsesLLM(orchestrator.LLMConfig{APIKey: cfg.OpenAIAPIKey, BaseURL: openaiBaseURL}),
		Catalog: orchestrator.CatalogAdapter(orchestrator.RetrieverDeps{Pool: pool, OpenAI: openaiClient}, func(ctx context.Context, image []byte, mimeType string) ([]float32, error) {
			return gemini.EmbedContent(ctx, workers.ImageEmbeddingModel, workers.ImageEmbeddingDimensions, []google.Part{{InlineData: &google.InlineData{MimeType: mimeType, Data: base64.StdEncoding.EncodeToString(image)}}})
		}),
		Media: workers.NewMediaProcessor(
			orchestrator.NewMediaDownloader(orchestrator.MediaConfig{
				Token:   cfg.WhatsAppAccessToken,
				Version: cfg.WhatsAppAPIVersion,
				BaseURL: os.Getenv("WHATSAPP_BASE_URL"),
			}),
			openaiClient,
		),
	})

	// Outbound: the sole sender of WhatsApp messages.
	workers.RegisterOutbound(server, workers.OutboundDeps{Pool: pool, WA: wa})

	// Checkout: order creation, split out of the CRM materialiser.
	workers.RegisterCheckout(server, workers.CheckoutDeps{Pool: pool})

	// order.created consumers: payment initiation, profile stats, follow-ups.
	workers.RegisterPaymentInit(server, workers.PaymentInitDeps{Pool: pool, Publisher: client, Paystack: psInitiator{c: psc}})
	workers.RegisterProfileStats(server, workers.ProfileStatsDeps{Pool: pool})
	workers.RegisterFollowUpSchedule(server, workers.FollowUpScheduleDeps{Pool: pool})

	// order.cancelled consumers: stock reversal.
	workers.RegisterRestock(server, workers.RestockDeps{Pool: pool})

	// Stealth CRM enrichment, payments, follow-ups, embeddings.
	workers.RegisterCRM(server, workers.CRMDeps{Pool: pool})
	deps := workers.Deps{
		Pool:                            pool,
		Publisher:                       client,
		WA:                              wa,
		ReengagementThresholdMultiplier: cfg.ReengagementThresholdMultiplier,
		ReengagementCooldownDays:        cfg.ReengagementCooldownDays,
		NewArrivalsIntervalDays:         cfg.NewArrivalsIntervalDays,
	}
	workers.RegisterPaymentEvents(server, deps)
	workers.RegisterFollowUp(server, deps)
	if err := workers.RegisterEmbedding(server, workers.EmbeddingDeps{Pool: pool, OpenAI: openaiClient, Google: gemini}); err != nil {
		logger.Error("register embedding handlers", slog.Any("error", err))
		os.Exit(1)
	}

	workers.RegisterExample(server, logger)

	// Cron sweeps: the scheduler enqueues one task per tick onto the default
	// queue; each sweep takes an advisory lock, so replicas never double-fire.
	for _, ce := range workers.CronSpecs() {
		server.Register(ce.TaskType, cronHandler(ce.Spec, deps))
	}
	sched := asynq.NewScheduler(asynq.RedisClientOpt{
		Addr:      redisAddr,
		Password:  cfg.RedisPassword,
		TLSConfig: redisTLS(cfg),
	}, &asynq.SchedulerOpts{Location: time.UTC, Logger: schedulerLogger{logger}})
	for _, ce := range workers.CronSpecs() {
		if _, err := sched.Register(ce.Cron, asynq.NewTask(ce.TaskType, nil),
			asynq.MaxRetry(0), asynq.Timeout(10*time.Minute)); err != nil {
			logger.Error("register cron", slog.String("spec", ce.Spec), slog.Any("error", err))
			os.Exit(1)
		}
	}

	// Start (non-blocking) both runtimes, then wait for the shutdown signal.
	// asynq's Run() variants each block on their own signal wait, which is why
	// they cannot be chained.
	if err := sched.Start(); err != nil {
		logger.Error("start cron scheduler", slog.Any("error", err))
		os.Exit(1)
	}
	if err := server.Start(); err != nil {
		sched.Shutdown()
		logger.Error("start worker runtime", slog.Any("error", err))
		os.Exit(1)
	}
	if err := queue.DeclareQueues(ctx, asynq.RedisClientOpt{Addr: redisAddr, Password: cfg.RedisPassword, TLSConfig: redisTLS(cfg)}); err != nil {
		logger.Warn("declare queues for the dashboard", slog.Any("error", err))
	}
	logger.Info("Worker process started — processing queues and cron sweeps",
		slog.Int("concurrency", workerConcurrency()))

	// Event dispatcher: publish PENDING domain_events (order.created etc.) to
	// their queue subscribers via LISTEN/NOTIFY + a poll backstop.
	events.DeadLetterHook = func(eventType, aggregateID string, err error) {
		slog.Log(context.Background(), observability.LevelFatal,
			"domain event permanently failed (dead letter)",
			slog.String("event", "event_dead_letter"),
			slog.String("eventType", eventType),
			slog.String("aggregateId", aggregateID),
			slog.String("severity", "fatal"),
			slog.Any("error", err))
	}
	dispatcher := &events.Dispatcher{
		Pool:      pool,
		Publisher: client,
		Subscriptions: map[string][]events.Subscription{
			events.TypeOrderCreated: {
				{Queue: queue.QPaymentInit, TaskType: queue.TaskPaymentInit},
				{Queue: queue.QCRMMaterialiser, TaskType: queue.TaskProfileStats},
				{Queue: queue.QFollowUp, TaskType: queue.TaskFollowUpSchedule},
			},
			events.TypeOrderCancelled: {
				{Queue: queue.QCheckout, TaskType: queue.TaskRestock},
			},
		},
	}
	go func() {
		if err := dispatcher.Run(ctx); err != nil && ctx.Err() == nil {
			logger.Error("event dispatcher exited", slog.Any("error", err))
		}
	}()

	<-ctx.Done()
	logger.Info("worker shutting down")
	sched.Shutdown()
	if err := server.Stop(); err != nil {
		logger.Error("worker graceful shutdown", slog.Any("error", err))
	}
}

func cronHandler(spec string, deps workers.Deps) queue.Handler {
	return func(ctx context.Context, _ []byte) error {
		switch spec {
		case "follow-up-scanner":
			return workers.SweepFollowUps(ctx, deps)
		case "outbox-sweep":
			return workers.SweepOutbox(ctx, deps)
		case "retention-scanner":
			return workers.SweepRetention(ctx, deps)
		case "checkout-expiry":
			return workers.SweepCheckoutExpiry(ctx, deps)
		case "new-arrivals-scanner":
			return workers.SweepNewArrivals(ctx, deps)
		default:
			return fmt.Errorf("unknown cron spec %q", spec)
		}
	}
}

func redisTLS(cfg *config.Config) *tls.Config {
	if !cfg.RedisTLS {
		return nil
	}
	return queue.TLSServerConfig(cfg.RedisHost)
}

// workerConcurrency reads WORKER_CONCURRENCY (asynq.Config.Concurrency);
// <=0 falls back to the substrate default.
func workerConcurrency() int {
	n, err := strconv.Atoi(os.Getenv("WORKER_CONCURRENCY"))
	if err != nil || n <= 0 {
		return queue.DefaultConcurrency
	}
	return n
}

// schedulerLogger routes asynq scheduler internals through slog.
type schedulerLogger struct{ log *slog.Logger }

func (l schedulerLogger) Debug(args ...any) { l.log.Debug(fmt.Sprint(args...)) }
func (l schedulerLogger) Info(args ...any)  { l.log.Info(fmt.Sprint(args...)) }
func (l schedulerLogger) Warn(args ...any)  { l.log.Warn(fmt.Sprint(args...)) }
func (l schedulerLogger) Error(args ...any) { l.log.Error(fmt.Sprint(args...)) }
func (l schedulerLogger) Fatal(args ...any) { l.log.Error("fatal: " + fmt.Sprint(args...)) }

// psInitiator adapts the Paystack client to workers.PaystackInitiator.
type psInitiator struct{ c *paystack.Client }

func (p psInitiator) InitiatePayment(ctx context.Context, req workers.PaymentRequest) (workers.PaymentLink, error) {
	res, err := p.c.InitiatePayment(ctx, paystack.InitiatePaymentRequest{
		AmountMajor:   req.Amount,
		Currency:      req.Currency,
		CustomerPhone: req.CustomerPhone,
		Reference:     req.Reference,
		BusinessID:    req.BusinessID,
		CallbackURL:   req.CallbackURL,
	})
	if err != nil {
		return workers.PaymentLink{}, err
	}
	return workers.PaymentLink{ProviderReference: res.ProviderReference, PaymentURL: res.PaymentURL, Status: res.Status}, nil
}
