// outbound.go is the outbound-queue consumer (outbound.processor.ts): it
// decodes the job and hands the row to internal/outbound, which owns ordered
// delivery through the WhatsApp Cloud API.
package workers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/hibiken/asynq"

	"github.com/novoapex/novoapex-backend-api/internal/outbound"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
)

// OutboundSender is the WhatsApp surface delivery needs.
type OutboundSender = outbound.Sender

// OutboundDeps carries the outbound consumer collaborators.
type OutboundDeps = outbound.Deps

// RegisterOutbound attaches the outbound-queue handler.
func RegisterOutbound(reg queue.Registrar, deps OutboundDeps) {
	reg.Register(queue.TaskOutboundSend, func(ctx context.Context, payload []byte) error {
		var job queue.OutboundJob
		if err := json.Unmarshal(payload, &job); err != nil || job.OutboundMessageID == "" {
			return fmt.Errorf("outbound: decode job payload: %w", errors.Join(err, asynq.SkipRetry))
		}
		return HandleOutboundSend(ctx, deps, job.OutboundMessageID, isFinalAttempt(ctx))
	})
}

// HandleOutboundSend delivers one outbound row; see outbound.Deliver.
func HandleOutboundSend(ctx context.Context, deps OutboundDeps, outboundMessageID string, finalAttempt bool) error {
	return outbound.Deliver(ctx, deps, outboundMessageID, finalAttempt)
}

// isFinalAttempt reports whether the running task has no retries left. Tasks
// invoked outside asynq (tests, drivers) count as final.
func isFinalAttempt(ctx context.Context) bool {
	retried, ok1 := asynq.GetRetryCount(ctx)
	maxRetry, ok2 := asynq.GetMaxRetry(ctx)
	if !ok1 || !ok2 {
		return true
	}
	return retried >= maxRetry
}
