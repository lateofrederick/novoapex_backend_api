# novoapex-backend-api

Go port of the NovoApex backend. Scope, sequencing, and task IDs reference
`../novoapex/MIGRATION_PLAN.md`; queue and orchestrator decisions live in
`../novoapex/docs/adr/0001-queue-substrate.md` and `.../0002-orchestrator-language.md`.

## Layout

```
cmd/
  api/          HTTP server (Stage 1+)
  worker/       asynq consumers + cron (Stage 6+)
internal/
  config/       env parsing + validation (T0.25)
  httpx/        router, middleware, error envelope
  auth/         JWT, OTP
  db/
    schema/     generated from prisma migrations — do not edit by hand
    queries/    sqlc query files
    gen/        sqlc output (pgx/v5 + shopspring/decimal)
  money/        decimal helpers, serialization
  domain/       business logic by entity
  handlers/     HTTP handlers by module
  workers/      queue consumers
  integrations/ whatsapp, paystack, cloudinary, openai, google, smtp
  observability/ slog, sentry, prometheus
scripts/
  sync-prisma-schema.sh
sqlc.yaml
```

## Schema sync

Prisma remains the migration source of truth until Stage 9 (plan §A.3). After any
`prisma migrate` in the main repo:

```
./scripts/sync-prisma-schema.sh            # default source: ../novoapex/prisma/migrations
./scripts/sync-prisma-schema.sh /path/to/novoapex/prisma/migrations
```

This regenerates `internal/db/schema/schema.sql` for sqlc.

## Development

```
go build ./...
go vet ./...
go test ./...
golangci-lint run
```

CI runs the same checks on every push and PR (`.github/workflows/ci.yml`).
