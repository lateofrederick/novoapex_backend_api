# Auth-Failure Alerting (T3.12) — wire BEFORE cutover (T3.13)

Stage 3 cuts `/auth/*` from the Node mobile API to the Go port. Because OTP
delivery costs real WhatsApp/SMS money and login is the front door, an
auth-failure alert must exist **before** traffic shifts. This doc defines the
alerts, where each signal comes from on both stacks, and the runbook.

## Signal inventory

| Failure mode | Node source | Go source |
| --- | --- | --- |
| OTP delivery failure (email/WhatsApp) | `AuthService` logs `Failed to send OTP to <phone> via <method>` then throws plain `Error` → 500 | handler logs `Failed to send OTP to <phone> via <method>` via `slog.Error`; response is a bare 500 `Internal server error` envelope (`internal/handlers/auth.go`) |
| Verify failures (wrong/expired/burned code) | `UnauthorizedException('Invalid or expired OTP')` → 401 with `message.message="Invalid or expired OTP"` | identical envelope via `httpx.WriteError` + `auth.Verify` false |
| Guard rejections (bad/missing token on guarded routes) | `JwtAuthGuard` 401 `Invalid or missing session token` | `auth.Middleware` 401, same pinned body (`internal/auth/jwt.go`) |
| Rate-limit trips (abuse / cost-attack signal) | `ThrottlerException: Too Many Requests` 429 on `POST /auth/request-otp` and `POST /auth/verify-otp` | same message/status via `internal/httpx/throttle.go`, plus `Retry-After` + `X-RateLimit-*` headers |
| Redis down (OTP store unavailable) | ioredis errors bubble as 500 | go-redis errors wrapped `auth: redis …` → 500 envelope |

Both stacks log through the shared JSON pipeline (pino on Node, slog on Go)
shipped by promtail/Loki; every error response also carries the
`{statusCode,timestamp,path,message}` envelope written by
AllExceptionsFilter / `httpx.WriteError`.

## Alerts (Loki LogQL, evaluate every 1m)

```yaml
groups:
  - name: auth-failures
    rules:
      # A1 — OTP delivery broken (money leak / blocked logins). Page.
      - alert: AuthOtpDeliveryFailures
        expr: |
          sum by (service) (
            rate({job=~"novoapex.*"}
              | json
              | msg =~ "Failed to send OTP to .* via .*" [5m])
          ) > 0
        for: 2m
        labels:
          severity: critical
          stage: auth-cutover
        annotations:
          summary: "OTP delivery failing on {{ $labels.service }}"
          runbook: docs/alert-auth-failures.md#a1

      # A2 — verify failure spike: brute force or a bad cutover. Page if it
      # dwarfs baseline (healthy traffic sees some wrong-code noise).
      - alert: AuthVerifyFailureSpike
        expr: |
          sum(rate({job=~"novoapex.*"} | json
              | json_logfmt
              | line_format "{{.error}}"
              | =~ " - .*Invalid or expired OTP.*" [5m]))
          /
          clamp_min(sum(rate({job=~"novoapex.*"} | json
              | line_format "{{.msg}}" | =~ "POST /auth/verify-otp" [5m])), 0.001)
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "OTP verify failure ratio above threshold"
          runbook: docs/alert-auth-failures.md#a2

      # A3 — throttler storms: someone hammering OTP endpoints (cost abuse).
      - alert: AuthThrottlerStorm
        expr: |
          sum(rate({job=~"novoapex.*"} | json
            | line_format "{{.msg}}"
            | =~ "ThrottlerException: Too Many Requests" [5m])) > 1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Sustained 429s on /auth/* — possible OTP pumping"
          runbook: docs/alert-auth-failures.md#a3

      # A4 — any 5xx from the auth surface (redis/pg/mailer outages). Page.
      - alert: AuthServerErrorRate
        expr: |
          sum(rate({job=~"novoapex.*"} | json
            | line_format "{{.msg}}"
            | regex "(?P<verb>POST|GET) /auth/.* (?P<status>5[0-9]{2}) -" [5m])) > 0.05
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "5xx rate > 0.05/s on /auth/*"
          runbook: docs/alert-auth-failures.md#a4

      # A5 — guard-rejection explosion right after cutover catches a bad
      # secret/claim regression instantly (tokens valid on both stacks by
      # construction, so this should stay silent).
      - alert: AuthGuardRejectionsPostCutover
        expr: |
          sum(increase({job="novoapex-go", stage="auth-cutover"} |= "Invalid or missing session token" [10m])) > 50
        for: 0m
        labels:
          severity: critical
        annotations:
          summary: "JWT guard rejection burst after /auth cutover — check JWT_SECRET parity"
```

HTTP-status based variants can be substituted where request logs carry status
codes; the message-based matchers above are deliberately identical across both
stacks because the Go port reproduces Node's exact strings.

## Runbook

### A1 — OTP delivery failing

1. Identify channel in the log line (`via email` vs `via whatsapp`).
2. **Email**: check SMTP relay health (`SMTP_HOST`/`SMTP_PORT`); port 465 must
   do implicit TLS, 587 STARTTLS. The Go sender (`internal/integrations/smtp`)
   mirrors nodemailer's `secure: port === 465`.
3. **WhatsApp**: call the graph URL manually with the configured
   `WHATSAPP_ACCESS_TOKEN`; inspect the Meta error envelope embedded in the
   thrown `Meta API error: {...}` message (token expired / WNI misconfigured /
   recipient not in allow-list are the usual three).
4. Rollback path for cutover: revert the Caddy `/auth/*` block to Node while
   investigating; tokens remain valid on both stacks.

### A2 — verify failure spike

1. Compare against signup volume; a matching rise in successful verifies means
   marketing traffic, not attack.
2. Check Redis TTL/attempts keys (`otp:<phone>`, `otp:attempts:<phone>`): mass
   "expired" failures with correct codes point at clock/TTL drift or an
   accidental FLUSH between stacks.
3. If brute force: confirm throttler counters are tripping (A3 firing too);
   consider lowering `MaxAttempts` operationally via deploy config change only
   after parity review.

### A3 — throttler storm

1. Extract top client IPs from `X-RateLimit` hit logs / access logs.
2. Block at Caddy if a single source dominates.
3. Note: budgets are per route × per IP (Nest `generateKey` shape preserved),
   fixed window of 60s, block duration = window.

### A4 — auth 5xx

1. Redis: `GET otp:*` probe; connection-pool exhaustion shows as timeouts.
2. Postgres: business lookup (`ListBusinessesByOwnerPhone`) failures on
   verify/me.
3. If both stacks 5xx simultaneously → infra; if only Go → roll back Caddy
   block, capture `slog` output, file with logs attached.

### A5 — guard rejections post-cutover

1. Confirm `JWT_SECRET` byte-equality between stacks (harness pins
   cross-validation in `TestS3_AuthCrossValidationAgainstNodeStack`).
2. Check for clock skew > exp tolerance on new pods.
3. Roll back immediately; token validation is fail-closed.
