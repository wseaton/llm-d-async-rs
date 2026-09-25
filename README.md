# llm-d-async (Rust)

An asynchronous dispatch processor for llm-d. Producers submit requests over
HTTP. They wait in durable queues in an embedded [redb](https://github.com/cberner/redb)
store, pass dispatch gates that watch system capacity, and go to an inference
gateway (`llm-d-router` or any OpenAI-compatible endpoint). Results come back
through the same API.

This is a rewrite of the Go processor. The Redis and Pub/Sub transports are
replaced by the embedded store, and the processor owns its queues. Gates,
merge policies, retries, deadlines, metrics, and tracing match the Go version
except where noted under [Differences from the Go processor](#differences-from-the-go-processor).

```text
 producers ──HTTP──► store (redb + blob files)
                        │ per queue: peek → queue gate → claim
                        ▼
                 merge policy (per pool) ──► workers ──pool gate──► inference gateway
                        ▲                                   │
                        └──────── outcomes (result / retry / release)
 consumers ◄──HTTP── results
```

## Running

```sh
cargo run --release -- \
  --data-dir /var/lib/llm-d-async \
  --transport-config-file transport.json \
  --pool-config-file pools.json \
  --request-merge-policy-config-file merge.json
```

One process owns a data directory. A claim is a row in the store, not a lease.
When the store opens, every claim the previous process held goes back to its
queue, so delivery is at-least-once across crashes. Graceful shutdown (SIGTERM)
stops claiming, lets in-flight requests finish for `--drain-timeout`, and puts
the rest back in their queues.

### Flags

| flag | default | |
|---|---|---|
| `--data-dir` | `data` | store and blob files |
| `--api-addr` | `0.0.0.0:8080` | producer/consumer API |
| `--health-port` / `--metrics-port` | 8081 / 9090 | `/healthz`, `/readyz` / `/metrics` |
| `--concurrency` | 64 | workers in the default pool when no pool file is given |
| `--request-timeout` | 5m | one inference attempt |
| `--gate-wait-timeout` | 5m | longest wait at a pool gate before requeueing (0: until the deadline) |
| `--drain-timeout` | 2m | in-flight grace period on SIGTERM |
| `--transport-config` / `--transport-config-file` | | queues (exactly one) |
| `--transport-config-watch-interval` | 0 | hot-reload the file's `queues` |
| `--pool-config-file` | | worker pools |
| `--request-merge-policy-config-file` | random-robin | merge policy |
| `--metrics-backlog-poll-interval` | 15s | queue depth and deadline-proximity metrics |
| `--prometheus-url` / `--prometheus-cache-ttl` | / 5s | metric gates |
| `--inline-payload-limit` | 64 KiB | larger request bodies go to blob files |
| `--max-payload-bytes` | 1 GiB | largest request body |
| `--max-json-body-bytes` | 64 MiB | largest JSON submission (buffered in memory) |
| `--result-blob-retention` | 24h | how long a result body stays readable after a destructive pop |
| `--tls-ca-cert`, `--tls-cert`, `--tls-key`, `--tls-insecure-skip-verify` | | gateway TLS (native TLS) |
| `-v` | 2 | 2 info, 3–4 debug, 5+ trace; `RUST_LOG` overrides |

Durations use Go syntax (`90s`, `1h30m`, `250ms`), so existing values carry over.

### Transport config

```json
{
  "result_queue_name": "result-list",
  "poll_interval_ms": 1000,
  "batch_size": 10,
  "queues": [
    {
      "id": "chat", "queue_name": "chat",
      "igw_base_url": "http://gateway:8000", "request_path_url": "/v1/chat/completions",
      "worker_pool_id": "default", "inference_objective": "batch",
      "result_queue_name": "", "result_ttl_seconds": 0,
      "labels": {"tier": "batch"},
      "gate_type": "quota", "gate_params": {"mode": "concurrency", "limit": 8}
    }
  ]
}
```

Unknown fields are rejected. Redis-only fields (`url`, `retry_queue_name`,
`claim_*`, `enable_tracing`) must be removed. Each poll, a queue dispatches at
most `batch_size × budget` requests, earliest deadline first.

Pools (`pools.json`) and merge policies (`merge.json`) keep the Go format:
`[{"id","workers","gate_type","gate_params"}]` and
`{"type": "random-robin" | "tier-priority", "parameters": {...}}`.

## API

Request bodies are opaque bytes. They are never parsed after submission and
are streamed to the gateway exactly as submitted.

| | |
|---|---|
| `POST /v1/requests` | submit. JSON: `{"id","deadline","payload",...}` with an inline JSON payload. Multipart: a `request` part (the same JSON without `payload`) and a `payload` part with any bytes and any content type, streamed to disk. Returns `202 {"id","request_token"}` |
| `POST /v1/requests/batch` | JSON array, all-or-nothing |
| `POST /v1/requests/cancel` | `{"ids": [...]}`; best effort before dispatch |
| `GET /v1/queues` | queues with depth |
| `POST /v1/results/{route}/claims?wait_ms=&lease_ms=` | lease the oldest result: `{"claim_id","owner_token","lease_ms","result"}`, or 204 |
| `POST /v1/results/{route}/claims/{id}/renew` | `{"owner_token","lease_ms"}`; 409 once ownership is lost |
| `POST /v1/results/{route}/claims/{id}/ack` | `{"owner_token"}`; idempotent; deletes the result's body blob |
| `POST /v1/results/{route}/pop?wait_ms=` | destructive take (no ack) |
| `GET /v1/results/{route}/depth` | |
| `GET /v1/blobs/results/{token}` | a result body stored by reference |
| `PUT/GET/DELETE /v1/admin/budgets/{key}` | budget for `budget-key` gates (JSON number) |
| `PUT/GET/DELETE /v1/admin/dispatch-rates/{key}` | leased `DispatchRateLimit` for `leased-rate` gates |

Results use the Go `ResultMessage` wire format. A 2xx response whose media
type is not JSON (audio, images, anything binary) is streamed into a blob, and
the result carries `payload_ref`, `content_type`, `payload_size` and
`payload_sha256` instead of an inline `payload`. The fields match
`feat/result-payload-ref` in the Go repo.

## Gates

`constant`, `composite`, `wait-on-refuse`, `tier-priority-admission`,
`local-max-concurrency`, `quota` (alias `redis-quota`), `budget-key` (alias
`redis`), `leased-rate` (alias `redis-leased-rate`), `prometheus-saturation`,
`prometheus-budget`, `prometheus-query`, `endpoint-scrape`. Params match the
Go gates. With the aliases, a Redis `address` is ignored. Quota gates that
share a `prefix` share per-tenant counters, as they shared Redis keys before.

## Differences from the Go processor

- **No Redis, no Pub/Sub.** One process per data directory. There are no
  multiple replicas sharing a queue; scale by giving each replica its own
  queues.
- **Unknown `gate_type` is a startup error.** Go silently used an open gate,
  so a typo disabled flow control.
- **Queue-level `Wait` keeps the request queued.** Go dispatched it.
- **Deadline expiry uses the deadline's own instant** everywhere. Go compared
  whole seconds, so a request could be sent during its final second and
  aborted at once.
- **Result TTL expires each result** `result_ttl_seconds` after it is written.
  Go expired the whole result list after its last write.
- **Budget-limited polls still count `gate_closed`**, as in Go. Polling is
  paced by `poll_interval_ms`, as in Go.
- **Not ported:** GCP Pub/Sub and its GCS multipart transform, Redis Pub/Sub,
  Kubernetes authn/authz on `/metrics` (bind it to a private interface), and
  the deprecated per-backend flags.

## Tracing and metrics

Metric names match the Go processor (`llm_d_async_async_*`). Spans follow Go's
`process-request` attributes. A `traceparent` in request metadata continues the
producer's trace and is propagated to the gateway. Set
`OTEL_EXPORTER_OTLP_ENDPOINT` (gRPC) to export.

## Development

```sh
cargo fmt
cargo clippy --all --benches --tests --examples --all-features
cargo test                  # unit tests
cargo test --test e2e       # the binary against a stand-in gateway
```

The e2e suite starts the real binary on real ports with a real store. It
covers the JSON and multipart paths, results by reference, lease redelivery,
retries and shedding, cancellation and deadlines, every gate family,
tier-priority lane order, SIGKILL recovery, graceful drain, drain timeout, and
hot reload. The gateway, the Prometheus query API, and the scraped `/metrics`
page are a real HTTP server standing in for vLLM and Prometheus. Set
`E2E_LOG=debug` for processor logs.
