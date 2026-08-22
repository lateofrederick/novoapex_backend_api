# loadtest

Minimal std-lib-only HTTP load generator used to characterize the Go API
kernel (`cmd/api`) and, in Stage 8, the Node Terminus stack it replaces.
No external dependencies; build/run from anywhere with `go run`.

## Usage

```sh
go run ./scripts/loadtest -n 20000 -c 50 -target http://127.0.0.1:3977/health/live
```

## Flags

| Flag       | Default                            | Meaning                                   |
| ---------- | ---------------------------------- | ----------------------------------------- |
| `-n`       | `1000`                             | Total requests to issue                   |
| `-c`       | `10`                               | Concurrent workers                        |
| `-target`  | `http://127.0.0.1:3977/health/live` | Absolute http(s) URL                      |
| `-method`  | `GET`                              | HTTP method                               |
| `-timeout` | `10s`                              | Per-request timeout                       |
| `-warmup`  | `100`                              | Warmup requests discarded before measuring |

## Method

- One shared `http.Client`; keep-alive on (`MaxIdleConnsPerHost = -c`),
  responses fully drained so pooled connections are reused.
- Requests are split evenly across `-c` goroutines; each records its own
  durations (no shared locks on the hot path), merged and sorted at the end.
- Percentiles use nearest-rank on the sorted sample; `req/s` is
  `-n` / wall-clock of the measured phase (warmup excluded).
- Transport failures are counted as `failed` and excluded from latency
  stats; non-2xx responses are valid results counted under their status.
- Exit code is non-zero only if every request failed (server unreachable)
  or arguments/URL are invalid.

## Baseline runs

The Stage-1 Go-kernel baseline produced with this tool (scenarios,
numbers, caveats, exact re-run commands) lives at
`docs/go-loadtest-baseline.md` in the `novoapex` monorepo docs tree —
Stage 8's T8.1 Node-vs-Go comparison must re-run those commands verbatim.
