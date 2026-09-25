# Dispatch benchmark

Compares this processor with the Go llm-d-async processor's `sql` transport
(Postgres). Both run against the same Postgres, send to the same stand-in
gateway, and share one configuration: queues, replicas, batch size, poll
interval, workers, payload size.

The gateway lives in the benchmark process. It answers at once (or after
`-gateway-delay`) and records when each request arrived, so both processors
are timed by the same clock outside them. Each run gets a database of its
own, which makes `pg_stat_database` and `pg_stat_statements` report that run
alone.

## Setup

```sh
./setup.sh      # Go processor at GO_COMMIT, Rust release build, bench binary
./postgres.sh   # Postgres 17 with pg_stat_statements on port 55433
export BENCH_DATABASE_URL='postgres://postgres:postgres@127.0.0.1:55433/postgres?sslmode=disable'
```

`setup.sh` checks the Go processor out under `.go-async` at `GO_COMMIT`
(default: the head of the sql transport PR, llm-d-async#452). `go.mod` builds
the Go producer from that checkout.

## Runs

```sh
# Drain: preload 50k requests, then time cold processors dispatching them.
.bin/bench -impl rust -mode drain -n 50000 -batch-size 1000 -poll-interval-ms 10
.bin/bench -impl go   -mode drain -n 50000 -batch-size 1000 -poll-interval-ms 10

# Rate: submit 5000/s for 30s and record dispatch lag.
.bin/bench -impl rust -mode rate -rate 5000 -duration 30s -batch-size 1000 -poll-interval-ms 10
```

Every run prints a summary and appends a JSON line to `runs/results.jsonl`,
with a per-second dispatch series. Processor logs and configs are kept under
`runs/<impl>-<mode>-<time>/`. `-replicas`, `-queues`, `-concurrency`,
`-payload-bytes` and `-gateway-delay` vary the rest; `-keep` keeps the run's
database for inspection.

## Reading the numbers

- **Neither processor can dispatch faster than `queues × replicas × batch /
  poll interval`.** Each queue polls on a fixed tick and takes at most one
  batch per tick. The report prints this as the ceiling. Measure the
  implementations with a ceiling well above what they reach, then measure
  what your production settings allow.
- **Drain** isolates dispatch. The requests are loaded before the clock
  starts: the Go producer writes its tables directly, and the Rust processor
  takes them over its API with every queue held shut by a zero
  `budget-key` gate. `dispatch_per_s` covers first to last gateway arrival;
  `total_s` runs from starting the processors to the last result written.
- **Rate** includes ingestion. Dispatch lag runs from building the request in
  the producer to its arrival at the gateway, so the Rust number includes an
  HTTP round trip through the processor that the Go producer, which inserts
  directly, does not make.
- **Per request database work** (transactions, rows written, WAL bytes,
  statements, execution time) and **processor CPU and peak RSS** are what a
  shared Postgres pays for each dispatch. At scale they matter as much as the
  peak rate.
- Numbers from a laptop with Postgres in a VM show relative cost, not
  capacity. For numbers to quote, run the processors and Postgres on
  separate, dedicated hosts.
