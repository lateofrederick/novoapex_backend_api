# T8.26/T8.27 — Orchestrator debounce design (BullMQ → asynq)

## 1. What BullMQ does today (source of truth)

Producer side — `libs/queue/src/processors/webhook.processor.ts:147-165`:

```ts
// Fixed jobId is the debounce key: rapid messages in the same
// conversation collapse into ONE orchestrator run (one LLM call, one
// reply). The narrow drop-window — a message arriving while this job is
// *active* — is closed by the completion re-check in OrchestratorProcessor.
const debounceKey = `orchestrator:${recipientPhone}:${senderPhone}`;
await this.orchestratorQueue.add(
  'process-conversation',
  { recipientPhone, senderPhone },
  { jobId: debounceKey, delay: 3000, removeOnComplete: true, removeOnFail: { count: 100 } }
);
```

Consumer side — `libs/queue/src/processors/orchestrator.processor.ts:29-32, 53, 63-89`:

```ts
// Capture the start time BEFORE processing. Any inbound message persisted
// after this point arrived while we were busy — the fixed-jobId debounce
// add for it was rejected (this job was active), so we must catch it below.
const startedAt = new Date();
...
await this.reenqueueIfNewerMessages(recipientPhone, senderPhone, startedAt);
...
// Close the debounce drop-window: if a message arrived during processing,
// enqueue one more orchestrator run so it isn't answered on stale context
// (or ignored entirely). A *fresh* jobId is required — the fixed debounce
// key still points at this (active) job, so reusing it would be rejected,
// re-creating the very drop we're closing.
jobId: `orchestrator:${recipientPhone}:${senderPhone}:${Date.now()}`,
delay: 3000,
```

Mechanics that matter:

| BullMQ behaviour | Effect |
| --- | --- |
| fixed `jobId` + 3 s delay | all inbound messages for one `[recipient,sender]` pair inside a 3 s burst collapse into ONE pending job (dup adds are silently dropped while the old job is waiting/active) |
| `removeOnComplete: true` | the key is freed the instant processing finishes → a brand-new message right after completion starts a fresh debounce |
| `removeOnFail: {count:100}` | failed jobs keep their id (like asynq's archived state) |
| completion re-check (`created_at > startedAt`) | closes the drop-window: anything ingested while the job was active schedules one more pass under a fresh timestamped jobId |

## 2. asynq mapping analysis

asynq offers two dedup knobs:

1. **`asynq.TaskID(id)`** — rejects (`ErrTaskIDConflict`) any enqueue whose id matches a task that is currently *scheduled/pending/active* or retained/completed/archived. `internal/queue/client.go:74-79` already swallows `ErrTaskIDConflict`/`ErrDuplicateTask` and returns `nil`, i.e. drop-newest.
   - BullMQ duplicate-jobId behaviour is **also** drop-newest-keep-old-pending. Same net effect: first message wins, later messages in the burst rely on the winner's run to answer them.
   - Key lifetime parity: with **no retention**, asynq deletes a completed task immediately == BullMQ `removeOnComplete:true`; exhausted tasks (MaxRetry 0) move to *archived*, keeping the id alive exactly like `removeOnFail:{count:100}`.
2. **`asynq.Unique(ttl)`** — payload-hash dedup that survives completion until the TTL expires. This is actively WRONG here: a new message arriving 1 s after a completed run would be dropped again instead of starting a fresh debounce. **Chosen design therefore uses TaskID only, never Unique** (`PublishOrchestratorDebounce`, orchestrator.go).

Chosen mapping (`PublishOrchestratorDebounce`):

```go
EnqueueOpts{
    TaskID:    "orchestrator:" + recipient + ":" + sender, // fixed debounce key
    ProcessIn: 3 * time.Second,                            // BullMQ delay: 3000
}
// MaxRetry comes from §B.2 policy: QOrchestrator {MaxRetry: 0} — terminal,
// matching "Orchestrator jobs aren't retried" (processor onFailed comment).
```

Completion re-check (`reenqueueIfNewerInbound`) ports 1:1: capture `startedAt` before the handler runs; afterwards `SELECT EXISTS(... FROM inbound_messages WHERE sender_phone=$1 AND recipient_phone=$2 AND created_at > $3)`; on hit publish with a **fresh** TaskID `orchestrator:<r>:<s>:<UnixMilli>` + same 3 s delay. Errors in the re-check are logged and swallowed (non-fatal), like source lines 97-105.

## 3. Edge cases

- **Multi-instance safety**: TaskID uniqueness is enforced in Redis (asynq's own keyspace), so two api replicas debouncing the same conversation globally collapse to one job — identical to BullMQ's redis-backed jobId set. No instance-local memory involved.
- **Message arrives while job ACTIVE**: enqueue hits `ErrTaskIDConflict` → dropped (same as BullMQ active-job rejection); the completion re-check re-enqueues it. Covered by `opipeCompletionRecheckReenqueuesOnNewerInbound`.
- **Job fails (terminal)**: task archived, debounce key stays occupied until inspected/culled — BullMQ behaves identically (`removeOnFail count:100`). Next inbound for that pair is suppressed until then; accepted parity, not a regression.
- **Two completions race the re-check**: both may publish fresh timestamped ids → two delayed runs. Node has the exact same race (fresh `Date.now()` ids); harmless because a second run over the same conversation is convergent (idempotent context resolution, newest-state read).
- **Clock**: re-check compares Postgres `created_at` (DB clock) against a Go-captured wall time. Same skew exposure as Node (`new Date()` vs DB). Sub-second skew is masked by the 3 s debounce delay.
