package workers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/novoapex/novoapex-backend-api/internal/queue"
)

// RegisterExample ports ExampleProcessor (libs/queue/src/processors/
// example.processor.ts): every job on example-queue, whatever its name, is
// logged with its id, name and data.
func RegisterExample(r queue.Registrar, log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	// The process logger already carries "context" (the service name).
	log = log.With(slog.String("processor", "ExampleProcessor"))
	r.Register(queue.TaskExamplePrefix, func(ctx context.Context, payload []byte) error {
		id, taskType := queue.TaskInfo(ctx)
		name := taskType
		if len(taskType) > len(queue.TaskExamplePrefix) {
			name = taskType[len(queue.TaskExamplePrefix):]
		}
		log.InfoContext(ctx, fmt.Sprintf("Processing job %s (%s): %s", id, name, exampleData(payload)))
		return nil
	})
}

// exampleData renders the payload like JSON.stringify(job.data): compact JSON,
// "null" for an empty payload, and a JSON string for non-JSON bytes.
func exampleData(payload []byte) string {
	if len(bytes.TrimSpace(payload)) == 0 {
		return "null"
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, payload); err != nil {
		b, _ := json.Marshal(string(payload))
		return string(b)
	}
	return buf.String()
}
