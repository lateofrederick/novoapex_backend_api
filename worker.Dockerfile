# =============================================================================
# NovOApex Go WORKER image (Stage 6 queue substrate — T6.8)
# =============================================================================
# Build from the deploy repo (how compose does it):
#   docker compose --profile go build go-worker
# or from this repo root:
#   docker build -f worker.Dockerfile .
#
# Mirrors api.Dockerfile (same base images, static binary, non-root). Unlike
# the API the worker is headless: no port, no healthcheck probe — liveness is
# "process alive", readiness is handled by compose depends_on + asynq itself.

# Stage 1: Builder
FROM golang:1.26-alpine AS builder
WORKDIR /src

# Copy module manifests first so Docker caches the dependency download
# independently of source changes.
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build a static binary.
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker

# Stage 2: Runner
FROM alpine:3.20 AS runner

RUN addgroup -S app && adduser -S app -G app
USER app

COPY --from=builder /out/worker /worker
ENTRYPOINT ["/worker"]
