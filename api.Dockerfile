# =============================================================================
# NovOApex Go API image (Stage 1 migration — T1.10)
# =============================================================================
# Build from the deploy repo (how compose does it):
#   docker compose --profile go build go-api
# or from this repo root:
#   docker build -f Dockerfile.go .
#
# Runtime choice: alpine instead of distroless/static because the compose
# service healthcheck runs `wget` INSIDE the container and distroless has no
# shell/wget (a probe binary would mean adding application code). The binary
# is fully static (CGO_ENABLED=0), so switching to
# gcr.io/distroless/static-debian12 later is a two-line change here plus
# dropping the healthcheck's wget for an external probe.

# Stage 1: Builder
FROM golang:1.26-alpine AS builder
WORKDIR /src

# Copy module manifests first so Docker caches the dependency download
# independently of source changes.
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build a static binary.
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api

# Stage 2: Runner
FROM alpine:3.20 AS runner

# Run as non-root; the app binds :3100 (>1024) so no privileges needed.
RUN addgroup -S app && adduser -S app -G app
USER app

# Default service port for the Go API (config.Load reads PORT; compose pins
# it to 3100 explicitly as well).
ENV PORT=3100
EXPOSE 3100

COPY --from=builder /out/api /api
ENTRYPOINT ["/api"]
