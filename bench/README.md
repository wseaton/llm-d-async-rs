# Dispatch benchmark

Compares this processor with the Go llm-d-async processor's `sql` transport
(Postgres). Both run against the same Postgres, send to the same stand-in
gateway, and share one configuration: queues, replicas, batch size, poll
interval, workers, and request and response sizes.

The gateway lives in the benchmark process. It answers at once (or after
`-gateway-delay`) and records when each request arrived, so both processors
are timed by the same clock outside them. Each run gets a database of its
own, which makes `pg_stat_database` and `pg_stat_statements` report that run
alone.

## Reproducing

```sh
./setup.sh      # Go processor at GO_COMMIT, Rust release build, bench binary
./postgres.sh   # Postgres 17 with pg_stat_statements on port 55433
export BENCH_DATABASE_URL='postgres://postgres:postgres@127.0.0.1:55433/postgres?sslmode=disable'
./matrix.sh     # every case REPEATS (default 3) times, alternating implementations
uv run charts.py runs/matrix-<time>/results.jsonl charts
./profile.sh    # a production traffic profile: TOKENS_PER_S over ISL:OSL SHAPES
```

`setup.sh` checks the Go processor out under `.go-async` at `GO_COMMIT`
(default: the head of the sql transport PR, llm-d-async#452); `go.mod` builds
the Go producer from that checkout. `matrix.sh` writes `env.md` (commits,
Postgres version and settings, host), `results.jsonl` (one line per run) and
`summary.md` (medians, from `.bin/bench summarize`) under
`runs/matrix-<time>/`.

Single runs:

```sh
# Drain: preload 50k requests, then time cold processors dispatching them.
.bin/bench -impl rust -mode drain -n 50000 -batch-size 1000 -poll-interval-ms 10
# Rate: submit 1000/s for 30s; record dispatch lag and result latency.
.bin/bench -impl go -mode rate -rate 1000 -duration 30s -batch-size 1000 -poll-interval-ms 10
```

`-queues`, `-replicas`, `-concurrency`, `-isl` and `-osl` (prompt and
completion tokens, 4 bytes each) and `-gateway-delay` vary the rest; `-keep`
keeps the run's database for inspection. Processor logs and configs stay
under `runs/<impl>-<mode>-<time>/`.

## Reading the numbers

- **Neither processor can dispatch faster than `queues × replicas × batch /
  poll interval`.** Each queue polls on a fixed tick and takes at most one
  batch per tick; with the defaults (batch 10, every second) that is 10/s per
  queue per replica. The matrix uses batch 1000 every 10 ms so the ceiling is
  far above either implementation. Size production settings against it.
- **Drain** isolates dispatch. Requests are loaded before the clock starts:
  the Go producer writes its tables directly, and the Rust processor takes
  them over its API with every queue held shut by a zero `budget-key` gate.
  Dispatch rate covers first to last gateway arrival; last result runs from
  starting the processors to the last result written, startup included.
- **Rate** includes ingestion. Latency runs from building a request in the
  producer: dispatch lag to its arrival at the gateway, result latency to a
  reader taking its result. The Rust producer submits over HTTP through the
  processor; the Go producer inserts into Postgres directly and polls for
  results every 10 ms.
- **Per request** database work (transactions, WAL, statement time) and
  processor CPU and peak RSS are what each dispatch costs a shared Postgres
  and a replica. At low rates they are dominated by idle polling.
- Generated text is seeded random three-letter words, so Postgres cannot
  compress bodies away.
- Each run starts with a `CHECKPOINT`, and `postgres.sh` raises the WAL limits
  so none lands mid-run; a checkpoint during a run costs it seconds at random.
- These are laptop numbers, Postgres in a VM next to the processors and the
  load generator. They compare cost; they are not capacity. For numbers to
  quote, run the processors and Postgres on separate, dedicated hosts.

## Future optimizations

Found while benchmarking and not yet pursued:

- **Small-request memory.** A 200k small-request drain peaks at 300-670 MiB,
  against about 95 MiB for Go. Large responses are bounded by the outcome
  budget (a 128k-token-output drain peaks below Go), so the cost is per
  outcome, not per byte: outcome weight counts only the result body, not the
  envelope and result fields each outcome carries, and peak RSS includes
  allocator retention. Profile the heap over a run before changing anything.
- **Replica contention.** Postgres time per request rises with replicas
  sharing a queue. Every replica polls every queue on its tick; candidates
  are the pending index head, partition heartbeats, and the poll count.
- **Adaptive polling.** Poll again at once when a batch comes back full, and
  back off when a queue is empty. The first removes the `batch / interval`
  ceiling; the second removes idle polling, which dominates per-request cost
  at low rates.
- **Large text results in the database.** A 128k-token response writes about
  570 KB of WAL. Results over a size threshold could go to the blob store
  by reference, as binary results already do.
- **Result reads.** The result pop and claim statements have not been
  checked against stale statistics the way the dispatch path has.
- **One-statement claim.** Go picks and claims in one statement; this
  processor peeks, gates, then claims.
