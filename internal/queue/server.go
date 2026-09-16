package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

// DefaultGracefulTimeout bounds how long Stop waits for in-flight tasks after
// SIGTERM (mirrors apps/worker/src/main.ts shutdown hooks; asynq's
// Config.ShutdownTimeout).
const DefaultGracefulTimeout = 30 * time.Second

// maxBackoffDelay guards the exponential backoff against int64 overflow for
// pathological retry counts. BullMQ leaves it uncapped; nothing in this system
// ever gets close (max 5 attempts), so the cap is purely defensive.
const maxBackoffDelay = 24 * time.Hour

// QueuePolicy encodes one row of the plan §B.2 queue table.
type QueuePolicy struct {
	// MaxRetry is retries AFTER the first attempt. BullMQ's `attempts` counts
	// the first try, so attempts:3 maps to MaxRetry:2.
	MaxRetry int
	// BackoffBase is the BullMQ exponential backoff base delay; retry n waits
	// BackoffBase * 2^n. Zero means "no retry backoff configured".
	BackoffBase time.Duration
	// RateLimit/RateWindow encode the per-queue limiter ("100 / 10s"). Zero
	// RateLimit means unlimited (orchestrator-queue, embedding).
	RateLimit  int
	RateWindow time.Duration
	// RetentionTTL is applied at enqueue when EnqueueOpts.RetainTTL is unset.
	// Zero = delete on completion (BullMQ removeOnComplete:true semantics).
	RetentionTTL time.Duration
}

// builtinPolicies is the §B.2 table. orchestrator-queue has no retries
// (terminal). outbound-queue retries transient Meta/network failures twice;
// the consumer marks the row 'failed' once the budget is spent.
var builtinPolicies = map[string]QueuePolicy{
	QWebhookProcessing: {MaxRetry: 2, BackoffBase: 1 * time.Second, RateLimit: 100, RateWindow: 10 * time.Second},
	QOrchestrator:      {MaxRetry: 0},
	QOutbound:          {MaxRetry: 2, BackoffBase: 2 * time.Second, RateLimit: 50, RateWindow: 1 * time.Second},
	QCRMMaterialiser:   {MaxRetry: 2, BackoffBase: 2 * time.Second, RateLimit: 50, RateWindow: 1 * time.Second},
	QPaymentInit:       {MaxRetry: 2, BackoffBase: 2 * time.Second, RateLimit: 20, RateWindow: 1 * time.Second},
	QPaymentEvents:     {MaxRetry: 4, BackoffBase: 3 * time.Second, RateLimit: 20, RateWindow: 1 * time.Second},
	QFollowUp:          {MaxRetry: 2, BackoffBase: 5 * time.Second, RateLimit: 10, RateWindow: 1 * time.Second},
	QEmbedding:         {MaxRetry: 2, BackoffBase: 2 * time.Second},
	QExample:           {MaxRetry: 0}, // registered without defaultJobOptions: BullMQ's single attempt
}

func PolicyFor(queueName string) QueuePolicy {
	if p, ok := builtinPolicies[queueName]; ok {
		return p
	}
	return QueuePolicy{MaxRetry: 0}
}

// QueueOf derives the queue name from a task type ("<queue>:<name>").
func QueueOf(taskType string) string {
	if q, _, found := strings.Cut(taskType, ":"); found {
		return q
	}
	return taskType
}

// ServerConfig configures NewServer.
type ServerConfig struct {
	RedisAddr string
	RedisPass string
	RedisTLS  bool

	// Concurrency caps simultaneously executing tasks across all queues
	// (asynq.Config.Concurrency; <=0 falls back to asynq's default of 10).
	Concurrency int

	// GracefulTimeout bounds shutdown after SIGTERM; 0 => DefaultGracefulTimeout.
	GracefulTimeout time.Duration

	// Policies optionally overrides/extends the §B.2 table (tests use this to
	// shrink backoffs). Unknown queues keep PolicyFor's fallback.
	Policies map[string]QueuePolicy

	// Middleware, when set, wraps every task handler outermost (Sentry task
	// transactions): it sees the task after panics became errors.
	Middleware func(taskType string, next Handler) Handler

	Logger *slog.Logger
}

// DefaultConcurrency mirrors asynq's built-in default.
const DefaultConcurrency = 10

// Server is the asynq worker runtime (T6.2). It implements Registrar so worker
// mains can hand it to module Register* functions.
type Server struct {
	srv         *asynq.Server
	mux         *asynq.ServeMux
	log         *slog.Logger
	graceful    time.Duration
	policies    map[string]QueuePolicy
	rdb         *rateLimiter
	concurrency int
	middleware  func(taskType string, next Handler) Handler
}

// NewServer builds the worker runtime. Run/Start must be called to begin
// processing.
func NewServer(cfg ServerConfig) *Server {
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}

	graceful := cfg.GracefulTimeout
	if graceful <= 0 {
		graceful = DefaultGracefulTimeout
	}

	merged := make(map[string]QueuePolicy, len(builtinPolicies)+len(cfg.Policies))
	for q, p := range builtinPolicies {
		merged[q] = p
	}
	for q, p := range cfg.Policies {
		merged[q] = p
	}

	opt := asynq.RedisClientOpt{Addr: cfg.RedisAddr, Password: cfg.RedisPass}
	if cfg.RedisTLS {
		host, _, _ := splitHostPort(cfg.RedisAddr)
		opt.TLSConfig = tlsConfigFor(host)
	}

	concurrency := cfg.Concurrency
	if concurrency <= 0 {
		concurrency = DefaultConcurrency
	}

	aCfg := asynq.Config{
		Concurrency:     concurrency,
		Queues:          queueWeights(merged),
		ShutdownTimeout: graceful,
		// asynq forwards due scheduled/retry tasks on a poll (default 5s), which
		// would stretch the 3s orchestrator debounce to 3–8s. BullMQ promotes
		// delayed jobs at their due time; a 1s poll keeps replies close to that.
		DelayedTaskCheckInterval: time.Second,
		RetryDelayFunc:           retryDelay(merged),
		ErrorHandler:             asynq.ErrorHandlerFunc(handleProcessingError(log)),
		Logger:                   asynqLogger{log: log},
	}

	s := &Server{
		srv:         asynq.NewServer(opt, aCfg),
		mux:         asynq.NewServeMux(),
		log:         log,
		graceful:    graceful,
		policies:    merged,
		concurrency: concurrency,
		middleware:  cfg.Middleware,
	}
	s.rdb = newRateLimiter(cfg.RedisAddr, cfg.RedisPass, cfg.RedisTLS, log)
	return s
}

// queueWeights distributes the shared concurrency pool over every live queue
// (BullMQ ran one worker per queue; asynq multiplexes). Equal weights preserve
// fairness; per-queue isolation comes from Concurrency + rate limiters.
func queueWeights(policies map[string]QueuePolicy) map[string]int {
	qs := make(map[string]int, len(policies)+1)
	for q := range policies {
		qs[q] = 1
	}
	qs["default"] = 1 // safety net for misrouted tasks
	return qs
}

// Register attaches a handler for taskType (implements Registrar).
//
// Wrapping order (outermost first): middleware → recover → rate limit → user
// handler, so a panic anywhere — including limiter bookkeeping — is converted
// to an error with a stack instead of taking down the worker goroutine (T6.7).
func (s *Server) Register(taskType string, h Handler) {
	queueName := QueueOf(taskType)
	wrapped := h
	if p := s.policyFor(queueName); p.RateLimit > 0 && p.RateWindow > 0 {
		wrapped = s.rateLimited(queueName, p)(wrapped)
	}
	wrapped = Recover(wrapped)
	if s.middleware != nil {
		wrapped = s.middleware(taskType, wrapped)
	}
	s.mux.HandleFunc(taskType, func(ctx context.Context, t *asynq.Task) error {
		return wrapped(context.WithValue(ctx, taskTypeKey{}, t.Type()), t.Payload())
	})
}

type taskTypeKey struct{}

// TaskInfo returns the running task's id and full type ("<queue>:<name>") —
// BullMQ's job.id and job.name — from a handler context.
func TaskInfo(ctx context.Context) (id, taskType string) {
	id, _ = asynq.GetTaskID(ctx)
	taskType, _ = ctx.Value(taskTypeKey{}).(string)
	return id, taskType
}

// DeclareQueues records every queue in asynq's queue index so the dashboard
// lists them all, including ones that have never received a job (Bull Board
// showed every registered queue).
func DeclareQueues(ctx context.Context, opt asynq.RedisConnOpt) error {
	client := opt.MakeRedisClient()
	rdb, ok := client.(redis.UniversalClient)
	if !ok {
		return fmt.Errorf("queue: unsupported redis client %T", client)
	}
	defer func() { _ = rdb.Close() }()
	members := make([]any, 0, len(AllQueues()))
	for _, q := range AllQueues() {
		members = append(members, q)
	}
	return rdb.SAdd(ctx, "asynq:queues", members...).Err()
}

func (s *Server) policyFor(queueName string) QueuePolicy {
	if p, ok := s.policies[queueName]; ok {
		return p
	}
	return QueuePolicy{}
}

// retryDelay returns the RetryDelayFunc implementing the §B.2 exponential
// backoffs. Throttled tasks (RateLimitedError) reschedule flat after their
// window instead of growing exponentially.
func retryDelay(policies map[string]QueuePolicy) asynq.RetryDelayFunc {
	return func(n int, err error, t *asynq.Task) time.Duration {
		var rle *RateLimitedError
		if errors.As(err, &rle) {
			delay := rle.RetryIn
			if delay < minRetryDelay {
				delay = minRetryDelay
			}
			return delay
		}
		p, ok := policies[QueueOf(t.Type())]
		if !ok || p.BackoffBase <= 0 {
			// Fall back to asynq's default for queues without a base.
			return asynq.DefaultRetryDelayFunc(n, err, t)
		}
		delay := p.BackoffBase << min(n, 62) // BullMQ exponential: base * 2^attempt
		if delay > maxBackoffDelay {
			return maxBackoffDelay
		}
		return delay
	}
}

// handleProcessingError is the EXPLICIT failure-event registration (T6.4):
// asynq invokes it synchronously for every failed attempt, and ReportFailure
// escalates to FATAL + Sentry exactly once when retries are exhausted (T6.5).
func handleProcessingError(log *slog.Logger) func(ctx context.Context, t *asynq.Task, err error) {
	return func(ctx context.Context, t *asynq.Task, err error) {
		retried, _ := asynq.GetRetryCount(ctx)
		maxRetry, _ := asynq.GetMaxRetry(ctx)
		ReportFailure(log, FailureInfo{
			TaskType: t.Type(),
			Retried:  retried,
			MaxRetry: maxRetry,
		}, err)
	}
}

// Start begins processing without blocking.
func (s *Server) Start() error { return s.srv.Start(s.mux) }

// Stop gracefully drains in-flight tasks, waiting up to GracefulTimeout.
func (s *Server) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.graceful+5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { s.srv.Shutdown(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("queue: graceful shutdown exceeded %s", s.graceful)
	}
}

// Run starts the server and blocks until SIGTERM/SIGINT, then shuts down
// gracefully. This is what cmd/worker runs in production (T6.2 graceful
// shutdown semantics of apps/worker/src/main.ts).
func (s *Server) Run() error {
	if err := s.Start(); err != nil {
		return fmt.Errorf("queue: start server: %w", err)
	}
	s.log.Info("worker started",
		slog.Int("concurrency", s.concurrency),
		slog.Duration("graceful_timeout", s.graceful))

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	sig := <-sigCh
	s.log.Info("worker shutting down", slog.String("signal", sig.String()))
	return s.Stop()
}

// Close releases the rate-limiter redis connection.
func (s *Server) Close() error { return s.rdb.close() }

// asynqLogger routes asynq internals through slog so worker logs stay JSON.
type asynqLogger struct{ log *slog.Logger }

func (l asynqLogger) Debug(args ...any) { l.log.Debug(fmt.Sprint(args...)) }
func (l asynqLogger) Info(args ...any)  { l.log.Info(fmt.Sprint(args...)) }
func (l asynqLogger) Warn(args ...any)  { l.log.Warn(fmt.Sprint(args...)) }
func (l asynqLogger) Error(args ...any) { l.log.Error(fmt.Sprint(args...)) }
func (l asynqLogger) Fatal(args ...any) { l.log.Error("fatal: " + fmt.Sprint(args...)) }
