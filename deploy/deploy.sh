#!/usr/bin/env bash
set -euo pipefail

# NovOApex Go backend — standalone deployment helper.
# Stack definition: deploy/docker-compose.yml (this directory).

cd "$(dirname "$0")"

ENV_FILE="${ENV_FILE:-.env}"

usage() {
  cat <<EOF
Usage: ./deploy.sh <command>

Commands:
  up        Build and start the full stack (first run: cp .env.example .env first)
  roll      Rebuild + restart go-api and go-worker only (zero-downtime-ish)
  down      Stop the stack (data volumes preserved)
  status    Service states + health
  logs      Follow logs for a service: ./deploy.sh logs go-api
  migrate   Apply schema baseline (no-op when already applied)
  health    Curl the api health endpoints through caddy

Rollback to the Node stack is documented in novoapex/docs/rollback-go-cutover.md.
EOF
}

need_env() {
  if [ ! -f "$ENV_FILE" ]; then
    echo "error: $ENV_FILE missing — cp .env.example $ENV_FILE and fill it in" >&2
    exit 1
  fi
}

compose() {
  docker compose --env-file "$ENV_FILE" "$@"
}

case "${1:-}" in
  up)
    need_env
    compose up -d --build
    ./deploy.sh health
    ;;
  roll)
    need_env
    compose build go-api go-worker
    compose up -d go-api go-worker
    sleep 2
    ./deploy.sh health
    ;;
  down)
    compose down
    ;;
  status)
    compose ps
    ;;
  logs)
    compose logs -f --tail=100 "${2:?service name required}"
    ;;
  migrate)
    need_env
    compose up --build migrate
    ;;
  health)
    echo "go-api /health/live:"
    curl -fsS "http://127.0.0.1:3100/health/live" 2>/dev/null \
      || docker compose --env-file "$ENV_FILE" exec caddy wget -qO- "http://go-api:3100/health/live"
    echo
    ;;
  *)
    usage
    exit 1
    ;;
esac
