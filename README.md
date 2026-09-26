# llm-d-async (Rust)

> [!IMPORTANT]
> This Rust processor is experimental. It is a place to prove designs (resume
> from saved tokens, results by request, the Postgres store) before they go to [llm-d-async](https://github.com/llm-d/llm-d-async).
> What proves out is meant to be upstreamed and, where the project decides it
> belongs, refactored back into the Go processor. It is not a supported
> replacement for it.

An asynchronous dispatch processor for llm-d. Producers submit requests over
HTTP. They wait in durable queues, pass dispatch gates that watch system
capacity, and go to an inference gateway (`llm-d-router` or any
OpenAI-compatible endpoint). Results come back through the same API.

Queues live in one of two stores:

- **embedded**: [redb](https://github.com/cberner/redb) in a local directory,
  owned by one process. No other service.
- **postgres**: shared by any number of replicas, which split each queue
  between them and take over each other's work.

This is a rewrite of the Go processor. The Redis and Pub/Sub transports are
replaced by these stores, and the processor owns its queues. Gates, merge
policies, retries, deadlines, metrics, and tracing match the Go version
except where noted under [Differences from the Go processor](#differences-from-the-go-processor).

```text
 producers ──HTTP──► store (redb or Postgres, + blob store)
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

With Postgres, run as many replicas as you like against one database:

```sh
cargo run --release -- \
  --store postgres --database-url postgres://user@db/llm_d_async \
  --blob-store s3://bucket/llm-d-async \
  --transport-config-file transport.json
```

Delivery is at-least-once across crashes on either store. Graceful shutdown
(SIGTERM) stops claiming, lets in-flight requests finish for
`--drain-timeout`, puts the rest back in their queues, and (on Postgres)
hands the process's partitions to the other replicas at once.

## Stores and replicas

**Embedded.** One process owns the data directory. A claim is a row, not a
lease: whoever opens the store is its only owner, so every claim the previous
process held goes back to its queue on open. To scale out, give each replica
its own directory and its own queues.

**Postgres.** Each queue hashes its requests into 64 partitions. Every
replica consuming a queue heartbeats a membership row and leases its share,
`ceil(64 / live replicas)`, of the partitions; it only claims requests in
partitions it holds. When a replica joins, the others release partitions
down to their new share at their next heartbeat, within a second.

A claim belongs to the replica that made it, not to the partition: a
partition handed over keeps its requests in flight with the replica that
dispatched them, which finishes them while the new owner dispatches the
rest, so a handoff never waits for a long generation. Each replica
heartbeats a process row; when a replica dies, its row lapses after
`--partition-lease-ttl`, its claims go back to pending, and the survivors
take its partitions and redeliver what it held.

Every claim is fenced by the replica that holds it and by an attempt number
drawn for each dispatch. An outcome from a replica whose claims were
returned, or from an earlier dispatch of the same request, changes nothing.
Claims whose reply was lost are reconciled as in the Go SQL transport
(llm-d-async#452).

Quota gates (`quota`), leased-rate buckets (`leased-rate`), and budget keys
count in the database, so limits hold across replicas. Concurrency slots are
counted per heartbeated holder: a dead replica's slots free themselves when
its holder lapses. Every replica reports the whole queue's depth and
deadline proximity, so aggregate those metrics with `max`, not `sum`.

**Blob stores.** Request bodies over `--inline-payload-limit` and binary
results are stored apart from the queue:

| `--blob-store` | where | with |
|---|---|---|
| `local` (embedded default) | files under `--data-dir/blobs` | embedded only |
| `s3://bucket/prefix`, `gs://…`, `az://…`, `file:///…` | an object store | Postgres (required) |
| `postgres` | 1 MiB rows in the same database | Postgres, development only |

The Postgres store has no default blob store: audio and long prompts would
otherwise go through Postgres's WAL and memory. Bodies up to
`--inline-payload-limit` (64 KiB) stay in the queue row either way. Results
stored in an object store carry `payload_location`, the object's URL, so a
consumer with access to the bucket (the batch gateway's files store) can copy
it server-side instead of reading it through the processor.

Object store credentials come from the provider's usual environment variables
(`AWS_*`, `GOOGLE_*`, `AZURE_*`). The store deletes blobs nothing references
(uploads whose submission failed, deletes that failed) an hour after they
were written. Set a lifecycle rule to abort incomplete multipart uploads.

### Flags

| flag | default | |
|---|---|---|
| `--store` | `embedded` | `embedded` or `postgres` |
| `--data-dir` | `data` | embedded store and local blobs |
| `--database-url` / `DATABASE_URL` | | Postgres URL; TLS follows its `sslmode` |
| `--database-max-connections` | 32 | per replica |
| `--database-ca-cert` | | extra CA for Postgres (native TLS) |
| `--partition-lease-ttl` | 30s | how long a dead replica's partitions, claims and quota slots stay held |
| `--blob-store` / `BLOB_STORE` | `local` (embedded) | required with Postgres; see [Blob stores](#stores-and-replicas) |
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
| `--request-result-ttl` | 1h | how long a result [delivered by request](#results-by-request) stays when its queue sets no `result_ttl_seconds` |
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
      "gate_type": "quota", "gate_params": {"mode": "concurrency", "limit": 8},
      "render_url": null
    }
  ]
}
```

Unknown fields are rejected. Redis-only fields (`url`, `retry_queue_name`,
`claim_*`, `enable_tracing`) must be removed. Result routes starting with `@`
are the processor's own and cannot be configured. Each poll, a queue dispatches at
most `batch_size × budget` requests, earliest deadline first. The queue's
`inference_objective` is sent as `x-llm-d-inference-objective`; a request's own
header replaces it. A `render_url` makes the queue
[resumable](#resumable-queues).

Pools (`pools.json`) and merge policies (`merge.json`) keep the Go format:
`[{"id","workers","gate_type","gate_params"}]` and
`{"type": "random-robin" | "tier-priority", "parameters": {...}}`.

## API

Request bodies are opaque bytes, streamed to the gateway exactly as
submitted, except on [resumable queues](#resumable-queues).

| | |
|---|---|
| `POST /v1/requests` | submit. JSON: `{"id","deadline","payload",...}` with an inline JSON payload. Multipart: a `request` part (the same JSON without `payload`) and a `payload` part with any bytes and any content type, streamed to disk. Returns `202 {"id","request_token"}` |
| `POST /v1/requests/batch` | JSON array, all-or-nothing |
| `POST /v1/requests/cancel` | `{"ids": [...]}`; best effort before dispatch |
| `GET /v1/requests/{id}?request_token=` | `{"id","status"}`: `queued`, `in_progress` (claimed by a worker) or `done`; without `request_token`, the live submission of `id` |
| `GET /v1/queues` | queues with depth |
| `POST /v1/results/{route}/claims?wait_ms=&lease_ms=` | lease the oldest result: `{"claim_id","owner_token","lease_ms","result"}`, or 204 |
| `POST /v1/results/{route}/claims/{id}/renew` | `{"owner_token","lease_ms"}`; 409 once ownership is lost |
| `POST /v1/results/{route}/claims/{id}/ack` | `{"owner_token"}`; idempotent; deletes the result's body blob |
| `POST /v1/results/{route}/pop?wait_ms=` | destructive take (no ack) |
| `GET /v1/results/{route}/depth` | |
| `GET /v1/blobs/results/{name}` | a result body stored by reference; `name` is the tail of its `payload_ref`, `<token>-<attempt>` |
| `PUT/GET/DELETE /v1/admin/budgets/{key}` | budget for `budget-key` gates (JSON number) |
| `PUT/GET/DELETE /v1/admin/dispatch-rates/{key}` | leased `DispatchRateLimit` for `leased-rate` gates |

A Go client lives in [`clients/go`](clients/go)
(`github.com/wseaton/llm-d-async-rs/clients/go`). Its `Client` satisfies
`producer.Producer` from the Go repo, so code written against the Redis
producer switches by construction alone, and adds batch and streamed
submission, leased delivery (`ReceiveResult`/`RenewResult`/`AckResult`), and
`OpenResultBody` for results stored by reference. Its tests run the real
binary on both stores.

### Results by request

A submission with `"result_delivery": "request"` gets a result route of its
own, `@request-<id>`, named in the reply as `result_route`. Its consumer claims,
renews and acknowledges it with the result endpoints above (URL-encode the
route), so a result is delivered at least once to whoever asked for it, and
the queue's `result_queue_name` does not apply. Such requests need IDs no other
submission uses. The result expires after the queue's `result_ttl_seconds`, or
`--request-result-ttl` when the queue sets none. Result writes wake only the
long polls on their route, across replicas on Postgres.

Results use the Go `ResultMessage` wire format. Response bodies are inline
in `payload`, as in Go, unless they are binary: a 2xx response with a binary
media type (`audio/*`, `image/*`, `video/*`, `font/*`, `model/*`,
`application/octet-stream`, `application/pdf`, archives, protobuf, msgpack,
CBOR) is streamed into a blob, and any other successful response whose body
is not UTF-8 is stored the same way. The result then carries `payload_ref`,
`content_type`, `payload_size` and `payload_sha256` instead of an inline
`payload`. JSON, `text/*` (including `text/event-stream`), and untyped
responses stay inline. The fields match `feat/result-payload-ref` in the Go
repo; its rule (anything not JSON goes to a blob) does not.

## Resumable queues

A queue with a `render_url`, a vLLM serving the queue's model with
`--enable-scale-out`, sends eligible `/v1/completions` and
`/v1/chat/completions` requests through vLLM's token layer:

```text
 caller's request ──► render_url /v1/…/render ──► prompt token IDs + sampling params
                      igw_base_url /inference/v1/generate (streamed) ──► output token IDs
                      render_url /v1/…/derender ──► the caller's response
```

The result is the response the non-streamed request would have got, with the
same content, reasoning, tool calls (vLLM's own parser runs in derender),
finish reason and usage. `usage.prompt_tokens_details` carries the cached
tokens the generate stream reports (vLLM with `--enable-prompt-tokens-details`),
capped at the caller's prompt after a resume. Derender does not report
`stop_reason`, `system_fingerprint`, `usage.completion_tokens_details` or
per-request `metrics`; those are null. IDs keep vLLM 0.30's form.

An interrupted generation (a stream cut before `[DONE]`, an `abort` finish, a
drain) keeps its output: the retry re-renders the request, checks the prompt is
unchanged, and generates from the prompt plus the saved tokens with
`max_tokens` and `min_tokens` reduced. When the saved output used the whole
budget, the request finishes with `length` without generating. A generation
that finished but failed to derender keeps its tokens, and the retry only
derenders. Progress travels with the request, so a draining replica saves it
and another continues, and a shed continuation is sent again. The generate
request carries token IDs, so the gateway's prefix scorers see the same prompt
on every attempt.

Only an inline JSON payload is eligible, and only when derender reproduces its
response and a continuation samples the same way. These are sent as submitted:
a `stream: true` request, `n` or `best_of` above 1, beam search, `seed`,
structured output (`response_format` other than text, `structured_outputs`),
`tool_choice` of `required` or a named function, non-zero presence or
frequency penalty, `logprobs` or `prompt_logprobs`, `echo`,
`kv_transfer_params`, stop strings, `stop_token_ids`, `truncate_prompt_tokens`,
a completions `prompt` that is not one text or one list of token IDs, and chat
requests with `return_prompt_text: true`.

`tests/fixtures/vllm` holds responses recorded from Python vLLM 0.30, streamed
and not, with the render, generate stream and derender of each; the unit tests
check that the token layer reproduces every recorded response and resumes from
every cut. `tests/e2e/resumable.rs` runs the binary against a real vLLM (see
[Development](#development)).

## Praxis AI

[`bench/praxis-agents`](bench/praxis-agents/README.md) runs agents through
[Praxis AI](https://github.com/praxis-proxy/ai) in front of the processor:
background responses through results by request and resumption under
eviction, with the Praxis AI patches it needs.

## Gates

`constant`, `composite`, `wait-on-refuse`, `tier-priority-admission`,
`local-max-concurrency`, `quota` (alias `redis-quota`), `budget-key` (alias
`redis`), `leased-rate` (alias `redis-leased-rate`), `prometheus-saturation`,
`prometheus-budget`, `prometheus-query`, `endpoint-scrape`. Params match the
Go gates. With the aliases, a Redis `address` is ignored. Quota gates that
share a `prefix` and `attribute` share per-tenant counters, keyed
`<prefix><attribute>:<value>` as the Redis keys were.

## Differences from the Go processor

- **No Redis, no Pub/Sub.** Queues live in the embedded store (one process)
  or Postgres (many replicas, split by partition leases instead of Redis
  claim leases).
- **A connection that fails before response headers is retried.** Go failed
  the request, so a model pod dying during a non-streamed generation lost it.
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

The resumable and Praxis e2e tests run against a real vLLM
0.30 frontend over a GPU-free engine that replays scripted outputs
(`tests/fixtures/vllm/v0.30.0/resume`), with a gateway in between that cuts,
stalls or sheds generate streams. They are skipped unless `VLLM_BIN` (the vLLM
0.30.0 CLI) and `VCR_BIN` (`vllm-vcr` built for the 0.30 protocol, see
`tests/fixtures/vllm/README.md`) are set, and the Praxis test also needs
`PRAXIS_AI_BIN`. `REQUIRE_VLLM=1` and `REQUIRE_PRAXIS=1` make skipping a
failure. The OpenAI SDK client runs through `uv`.

Postgres tests need `TEST_DATABASE_URL`, a database where they may create
schemas (each test gets its own). Without it they are skipped; CI sets
`REQUIRE_POSTGRES=1` so they cannot be. A throwaway cluster:

```sh
initdb -D /tmp/pg -U postgres --auth=trust
pg_ctl -D /tmp/pg -o "-p 55432 -k '' -c listen_addresses=127.0.0.1" start
createdb -h 127.0.0.1 -p 55432 -U postgres lda_test
export TEST_DATABASE_URL=postgres://postgres@127.0.0.1:55432/lda_test
```

Every store backend runs the same conformance suite
(`src/store/conformance.rs`), as does every counter backend
(`src/gate/admission/counters/conformance.rs`).

The e2e suite starts the real binary on real ports with a real store. It
covers the JSON and multipart paths, results by reference, lease redelivery,
retries and shedding, cancellation and deadlines, every gate family,
tier-priority lane order, SIGKILL recovery, graceful drain, drain timeout, and
hot reload; and, on Postgres, replicas splitting a queue, SIGKILL failover,
SIGTERM handoff, a tenant quota across replicas, and large bodies crossing
replicas through both shared blob stores. The gateway, the Prometheus query API, and the scraped `/metrics`
page are a real HTTP server standing in for vLLM and Prometheus. Set
`E2E_LOG=debug` for processor logs.
