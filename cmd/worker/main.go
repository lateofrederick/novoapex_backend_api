// Command worker is the Go queue consumer runtime (T6.2). It mirrors
// apps/worker/src/main.ts semantics: a headless process running the queue
// processors and (from Stage 7) cron sweeps, with graceful SIGTERM shutdown.
//
// Registration calls exported by sibling worker packages are added centrally
// here as each stage lands; today it consumes the embedding queue (Stage 6)
// plus a no-op placeholder for orchestrator-queue so that pipeline cannot get
// stuck once its producers move over.
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
	"github.com/novoapex/novoapex-backend-api/internal/config"
	"github.com/novoapex/novoapex-backend-api/internal/db"
	"github.com/novoapex/novoapex-backend-api/internal/integrations/google"
	"github.com/novoapex/novoapex-backend-api/internal/integrations/openai"
	"github.com/novoapex/novoapex-backend-api/internal/integrations/paystack"
	"github.com/novoapex/novoapex-backend-api/internal/integrations/whatsapp"
	"github.com/novoapex/novoapex-backend-api/internal/orchestrator"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
	"github.com/novoapex/novoapex-backend-api/internal/workers"
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

	redisAddr := net.JoinHostPort(cfg.RedisHost, strconv.Itoa(cfg.RedisPort))

	client := queue.NewClientWithLogger(redisAddr, cfg.RedisPassword, logger)
	defer func() { _ = client.Close() }()

	server := queue.NewServer(queue.ServerConfig{
		RedisAddr:       redisAddr,
		RedisPass:       cfg.RedisPassword,
		RedisTLS:        cfg.RedisTLS,
		Concurrency:     workerConcurrency(),
		GracefulTimeout: queue.DefaultGracefulTimeout,
		Logger:          logger,
	})
	defer func() { _ = server.Close() }()

	// Stage 6 consumers.
	if err := workers.RegisterEmbedding(server, workers.EmbeddingDeps{
		Pool:   pool,
		OpenAI: openai.New(openai.Config{APIKey: cfg.OpenAIAPIKey, BaseURL: os.Getenv("OPENAI_BASE_URL")}),
		Google: google.New(google.Config{APIKey: cfg.GoogleGenerativeAIAPIKey, BaseURL: os.Getenv("GOOGLE_GENERATIVE_AI_BASE_URL")}),
	}); err != nil {
		logger.Error("register embedding handlers", slog.Any("error", err))
		os.Exit(1)
	}

	// Stage 7 consumers.
	wa := whatsapp.New(cfg.WhatsAppAPIVersion, cfg.WhatsAppAccessToken)
	psc := paystack.New(paystack.Config{SecretKey: cfg.PaystackSecretKey, BaseURL: cfg.PaystackBaseURL})
	stage7Deps := workers.Deps{
		Pool:                            pool,
		Publisher:                       client,
		WA:                              wa,
		ReengagementThresholdMultiplier: cfg.ReengagementThresholdMultiplier,
		ReengagementCooldownDays:        cfg.ReengagementCooldownDays,
	}
	workers.RegisterPaymentEvents(server, stage7Deps)
	workers.RegisterFollowUp(server, stage7Deps)
	workers.RegisterCRM(server, workers.CRMDeps{Pool: pool, Publisher: client, Paystack: psInitiator{c: psc}})

	cronDeps := stage7Deps
	for _, ce := range workers.CronSpecs() {
		ce := ce
		server.Register(ce.TaskType, func(ctx context.Context, _ []byte) error {
			switch ce.Spec {
			case "follow-up-scanner":
				return workers.SweepFollowUps(ctx, cronDeps)
			case "outbox-sweep":
				return workers.SweepOutbox(ctx, cronDeps)
			case "retention-scanner":
				return workers.SweepRetention(ctx, cronDeps)
			default:
				return fmt.Errorf("unknown cron spec %q", ce.Spec)
			}
		})
	}

	// Stage 8 orchestrator consumer.
	llmClient := orchestrator.NewResponsesLLM(orchestrator.LLMConfig{
		APIKey:  cfg.OpenAIAPIKey,
		BaseURL: os.Getenv("OPENAI_BASE_URL"),
	})
	embedClient := openai.New(openai.Config{APIKey: cfg.OpenAIAPIKey, BaseURL: os.Getenv("OPENAI_BASE_URL")})
	gemini := google.New(google.Config{APIKey: cfg.GoogleGenerativeAIAPIKey, BaseURL: os.Getenv("GOOGLE_GENERATIVE_AI_BASE_URL")})
	workers.RegisterOrchestrator(server, workers.OrchestratorDeps{
		Pool:      pool,
		Publisher: client,
		LLM:       llmClient,
		Catalog: orchestrator.CatalogAdapter(orchestrator.RetrieverDeps{Pool: pool, OpenAI: embedClient}, func(ctx context.Context, image []byte, mimeType string) ([]float32, error) {
			return gemini.EmbedContent(ctx, "gemini-embedding-001", 768, []google.Part{{InlineData: &google.InlineData{MimeType: mimeType, Data: base64.StdEncoding.EncodeToString(image)}}})
		}),
	})

	sched := asynq.NewScheduler(asynq.RedisClientOpt{
		Addr:     redisAddr,
		Password: cfg.RedisPassword,
		TLSConfig: func() *tls.Config {
			if cfg.RedisTLS {
				return queue.TLSServerConfig(cfg.RedisHost)
			}
			return nil
		}(),
	}, &asynq.SchedulerOpts{Location: time.Local})
	for _, ce := range workers.CronSpecs() {
		if _, err := sched.Register(ce.Cron, asynq.NewTask(ce.TaskType, nil)); err != nil {
			logger.Error("register cron", slog.String("spec", ce.Spec), slog.Any("error", err))
			os.Exit(1)
		}
	}
	if err := sched.Run(); err != nil {
		logger.Error("cron scheduler terminated", slog.Any("error", err))
		os.Exit(1)
	}
	defer sched.Shutdown()

	logger.Info("worker process started — processing queues and cron sweeps")
	if err := server.Run(); err != nil {
		logger.Error("worker runtime terminated with error", slog.Any("error", err))
		os.Exit(1)
	}
}

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

// workerConcurrency reads WORKER_CONCURRENCY (asynq.Config.Concurrency);
// <=0 falls back to the substrate default.
func workerConcurrency() int {
	n, err := strconv.Atoi(os.Getenv("WORKER_CONCURRENCY"))
	if err != nil || n <= 0 {
		return 0
	}
	return n
}
