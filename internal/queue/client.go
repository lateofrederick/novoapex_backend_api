package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/hibiken/asynq"
)

// AsynqClient is the asynq-backed implementation of Publisher (T6.2).
//
// It is safe for concurrent use: the underlying asynq.Client multiplexes over
// a redis connection pool. Producers (api handlers) must only depend on the
// Publisher interface; this type is what main wires in.
type AsynqClient struct {
	client *asynq.Client
	log    *slog.Logger
}

// NewClient connects to redis at addr (host:port, optionally password-protected)
// and returns a Publisher-compatible client. Close MUST be called on shutdown.
func NewClient(redisAddr, redisPass string) *AsynqClient {
	return &AsynqClient{
		client: asynq.NewClient(asynq.RedisClientOpt{Addr: redisAddr, Password: redisPass}),
		log:    slog.Default(),
	}
}

// NewClientWithLogger is NewClient with an explicit logger.
func NewClientWithLogger(redisAddr, redisPass string, log *slog.Logger) *AsynqClient {
	c := NewClient(redisAddr, redisPass)
	if log != nil {
		c.log = log
	}
	return c
}

// ClientConfig configures NewClientWithConfig.
type ClientConfig struct {
	RedisAddr string
	RedisPass string
	RedisTLS  bool // managed Redis (REDIS_TLS=true)
	Logger    *slog.Logger
}

// NewClientWithConfig is NewClient with TLS support and an explicit logger.
func NewClientWithConfig(cfg ClientConfig) *AsynqClient {
	opt := asynq.RedisClientOpt{Addr: cfg.RedisAddr, Password: cfg.RedisPass}
	if cfg.RedisTLS {
		host, _, _ := splitHostPort(cfg.RedisAddr)
		opt.TLSConfig = tlsConfigFor(host)
	}
	c := &AsynqClient{client: asynq.NewClient(opt), log: slog.Default()}
	if cfg.Logger != nil {
		c.log = cfg.Logger
	}
	return c
}

// Close releases the underlying redis connection pool.
func (c *AsynqClient) Close() error { return c.client.Close() }

// Enqueue publishes one task onto the named queue, mapping EnqueueOpts onto
// asynq options per plan §B.2:
//
//	MaxRetry  -> asynq.MaxRetry   (explicit >0 wins; otherwise the queue policy)
//	ProcessIn -> asynq.ProcessIn  (BullMQ delay)
//	ProcessAt -> asynq.ProcessAt  (BullMQ delayedUntil; ignored if ProcessIn set)
//	TaskID    -> asynq.TaskID     (fixed-id dedup)
//	UniqueTTL -> asynq.Unique
//	RetainTTL -> asynq.Retention  (completed-task retention)
//
// BullMQ parity notes:
//   - a duplicate TaskID or Unique key is silently dropped (nil error), which
//     matches BullMQ's behaviour when `jobId` already exists;
//   - when opts is nil (or fields are zero) the per-queue defaults from the
//     §B.2 policy table apply.
func (c *AsynqClient) Enqueue(ctx context.Context, queueName, taskType string, payload any, opts *EnqueueOpts) error {
	if taskType == "" {
		return errors.New("queue: enqueue: empty task type")
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("queue: marshal %s payload: %w", taskType, err)
	}

	asynqOpts := buildAsynqOptions(queueName, opts)

	_, err = c.client.EnqueueContext(ctx, asynq.NewTask(taskType, body, asynqOpts...))
	switch {
	case err == nil:
		return nil
	case errors.Is(err, asynq.ErrTaskIDConflict), errors.Is(err, asynq.ErrDuplicateTask):
		// BullMQ drops jobs whose fixed id is still queued/active/completed.
		c.log.Debug("queue: duplicate job dropped",
			slog.String("queue", queueName), slog.String("task", taskType))
		return nil
	default:
		return fmt.Errorf("queue: enqueue %s to %s: %w", taskType, queueName, err)
	}
}

// buildAsynqOptions maps EnqueueOpts (+ per-queue policy defaults) onto asynq
// options. Pure function so tests can assert the mapping without redis.
func buildAsynqOptions(queueName string, opts *EnqueueOpts) []asynq.Option {
	policy := PolicyFor(queueName)

	// Always pin the target queue: asynq would otherwise route to "default".
	asynqOpts := []asynq.Option{asynq.Queue(queueName)}

	var eo EnqueueOpts
	if opts != nil {
		eo = *opts
	}

	maxRetry := eo.MaxRetry
	if maxRetry <= 0 {
		maxRetry = policy.MaxRetry
	}
	asynqOpts = append(asynqOpts, asynq.MaxRetry(maxRetry))

	switch {
	case eo.ProcessIn > 0:
		asynqOpts = append(asynqOpts, asynq.ProcessIn(eo.ProcessIn))
	case !eo.ProcessAt.IsZero():
		asynqOpts = append(asynqOpts, asynq.ProcessAt(eo.ProcessAt))
	}

	if eo.TaskID != "" {
		asynqOpts = append(asynqOpts, asynq.TaskID(eo.TaskID))
	}
	if eo.UniqueTTL > 0 {
		asynqOpts = append(asynqOpts, asynq.Unique(eo.UniqueTTL))
	}

	retention := eo.RetainTTL
	if retention <= 0 {
		retention = policy.RetentionTTL
	}
	if retention > 0 {
		asynqOpts = append(asynqOpts, asynq.Retention(retention))
	}
	return asynqOpts
}
