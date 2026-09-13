# novoapex-backend-api

The NovoApex backend in Go: the vendor mobile API, WhatsApp/payment webhook
ingest, the AI conversation orchestrator and every background job. It fully
replaces the TypeScript `novoapex` service and has no runtime, build or test
dependency on it.

## Processes

| Binary        | Role                                                                                     |
| ------------- | ---------------------------------------------------------------------------------------- |
| `cmd/migrate` | Applies pending schema migrations (idempotent, safe to run concurrently)                 |
| `cmd/api`     | HTTP: auth, vendor API, `/webhooks/whatsapp`, `/webhooks/payments/{provider}`, `/messages`, health, metrics, API docs, queue dashboard |
| `cmd/api -mobile` | Only the vendor mobile API, on `MOBILE_API_PORT` (the standalone `apps/mobile-api` mode) |
| `cmd/worker`  | asynq consumers + cron sweeps (no HTTP surface)                                          |

Operational endpoints on `cmd/api`:

- `/api-docs` — Swagger UI (`/api-docs-json`, `/api-docs-yaml`); on outside production, or with `ENABLE_SWAGGER=true`.
  `cmd/api/router_test.go` fails if a route is added without documenting it in `internal/apidocs`.
- `/admin/queues` — queue dashboard (asynqmon) behind basic auth from `BULL_BOARD_USER`/`BULL_BOARD_PASSWORD`; returns 503 until both are set.
- `/metrics` — Prometheus (Go runtime + process collectors).

Every request is logged as a pino-http style JSON line (`request completed`, without auth headers). With
`SENTRY_DSN` set (both processes):

- **Issues** — every exception raised while serving a request (handler errors, 4xx HttpExceptions, auth guard
  and validation rejections, panics; never health probes) with `path`/`method` tags, the request body and the
  `X-User-Id` user; fatal logs; queue tasks whose retries are exhausted.
- **Tracing** (`SENTRY_TRACES_SAMPLE_RATE`, default 0.1) — requests become `http.server` transactions named by
  route (`GET /orders/{id}`, continuing incoming `sentry-trace`/`baggage`), queue tasks become `queue.process`
  transactions, with `db.sql.query` spans for every query and `http.client` spans for provider calls.
- **Profiling** (`SENTRY_PROFILES_SAMPLE_RATE`, default 1.0 of traced transactions) — the transaction's goroutine
  and the goroutines it starts are sampled at 101 Hz and sent as a sample-format profile with the transaction.
- **Logs** — every log line is forwarded to Sentry Logs.

PII (phone numbers, conversation ids) is scrubbed from issues and logs unless `SENTRY_SEND_PII=true`.

`example-queue` is a diagnostic queue: any job enqueued on it (`example-queue:<name>`) is logged by the worker.

Message flow: WhatsApp webhook → `webhook-processing` (persist inbound) →
`orchestrator-queue` (3s per-conversation debounce, LLM turn) →
`outbound-queue` (ordered WhatsApp delivery) + `crm-materialiser` (profile,
orders, invoices) → `payment-events` / `follow-up` / `embedding`.

## Layout

```
cmd/            migrate, api, worker entrypoints
internal/
  config/       env parsing + validation
  httpx/        router kernel, error envelope, throttling, health
  auth/         JWT, OTP, per-request business resolution
  db/
    migrations/ NNNN_name.sql schema migrations (embedded, applied in order)
    queries/    sqlc query files
    gen/        sqlc output (pgx/v5 + shopspring/decimal)
  domain/       conversation state machine, safety guard
  handlers/     HTTP handlers by module
  orchestrator/ LLM client, prompt, catalog retrieval, media download
  workers/      queue consumers and cron sweeps
  queue/        asynq client/server, retry + rate-limit policy
  integrations/ whatsapp, paystack, cloudinary, openai, google, smtp
  observability/ structured logging, Sentry, Prometheus
scripts/
  e2e/          full-stack end-to-end journey (see scripts/e2e/README.md)
  loadtest/     HTTP load generator
```

## Schema and migrations

The schema lives in `internal/db/migrations/`. `0001_baseline.sql` is the
complete fresh-start schema; later files are incremental. `cmd/migrate` applies
pending files in order, each in its own transaction, records them in
`schema_migrations`, holds an advisory lock so concurrent migrators are safe,
and refuses to run against a database migrated by a newer build. Tests use the
same runner (`internal/harness.ApplyBaselineSchema`).

To change the schema: add `internal/db/migrations/NNNN_description.sql` (never
edit an applied file), then run `sqlc generate` — it reads the same directory.

`DATABASE_URL` may carry Prisma's `?schema=public`; it is ignored. Sessions are
pinned to UTC because all timestamps are `TIMESTAMP(3)` UTC wall time.

## Deployment

`deploy/` holds the production stack (caddy, go-api, go-worker, Postgres with
pgvector, Redis):

```
cd deploy
cp .env.example .env      # fill in secrets + DOMAIN
./deploy.sh up            # build + start everything (migrate runs first)
./deploy.sh migrate       # apply pending migrations manually
./deploy.sh roll          # rebuild+restart api and worker only
```

Queue dashboard: `https://$DOMAIN/admin/queues` (basic auth from `BULL_BOARD_*`).
`WORKER_CONCURRENCY` sets worker parallelism (default 10); keep
`WORKER_DATABASE_POOL_MAX` at or above it.

## Development

```
go build ./...
go vet ./...
go test ./...            # needs Docker (testcontainers)
golangci-lint run
go run ./scripts/e2e     # full-stack journey, see scripts/e2e/README.md
```

CI runs build, vet, tests, lint and the end-to-end journey on every push and PR
(`.github/workflows/ci.yml`).
