# e2e

End-to-end journey for the whole backend. It builds `cmd/migrate`, `cmd/api`
and `cmd/worker`, migrates a freshly created database (twice, to prove
idempotency), starts both services against real Postgres + Redis, and serves
every external provider from an in-process mock:

| Provider   | Mocked surface                                                   |
| ---------- | ---------------------------------------------------------------- |
| Meta Graph | message sends (text/image/template), media metadata + download   |
| OpenAI     | `/responses` (scripted LLM turns), embeddings, Whisper           |
| Gemini     | `embedContent` (image vectors)                                   |
| Paystack   | transaction initialize, transfer recipient, transfer             |
| Cloudinary | image upload/destroy                                             |
| Sentry     | envelope ingest (issues, transactions, profiles, logs)           |

The scenario covers: auth/OTP and onboarding with a pre-onboarding token,
catalog + text/image embeddings, signed webhook ingest and dedupe, debounce,
product-image replies, order confirmation → invoice → Paystack webhook →
reconciliation, ordered delivery, vendor reads, fulfillment follow-ups, human
takeover, voice notes, keyword escalation, LLM-outage recovery, outbox
recovery, retention and new-arrivals crons, delivery-failure handling,
payouts, the example queue, what reached Sentry (4xx/5xx issues with bodies
and scrubbed PII, fatal task failures, route-named transactions with DB/LLM/
Meta spans, profiles, trace propagation, logs), the `-mobile` mode and
graceful shutdown. Crons are triggered on demand by enqueuing
their tick instead of waiting for the schedule.

## Run

Needs a pgvector Postgres and a Redis:

```sh
docker run -d --name novo-e2e-pg -e POSTGRES_PASSWORD=postgres -p 55432:5432 pgvector/pgvector:pg15
docker run -d --name novo-e2e-redis -p 56379:6379 redis:7-alpine redis-server --requirepass e2epass

go run ./scripts/e2e \
  -pg "postgresql://postgres:postgres@127.0.0.1:55432/postgres?sslmode=disable" \
  -redis 127.0.0.1:56379 -redis-pass e2epass
```

The database `novoapex_e2e` is dropped and recreated, and all asynq queues in
that Redis are deleted, on every run. Service logs (`api.log`, `worker.log`,
`migrate-*.log`) go to `-logs` (default `$TMPDIR/novoapex-e2e`). The process
exits non-zero if any check fails.
