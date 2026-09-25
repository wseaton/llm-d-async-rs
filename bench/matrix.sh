#!/usr/bin/env bash
# Runs the benchmark matrix against BENCH_DATABASE_URL: every case REPEATS
# times, alternating implementations, then writes env.md, results.jsonl and
# summary.md under runs/matrix-<time>.
set -euo pipefail

: "${BENCH_DATABASE_URL:?set BENCH_DATABASE_URL (see postgres.sh)}"
REPEATS=${REPEATS:-3}
here=$(cd "$(dirname "$0")" && pwd)
bin="$here/.bin/bench"
out="$here/runs/matrix-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$out"
results="$out/results.jsonl"
common=(-batch-size 1000 -poll-interval-ms 10 -concurrency 256 -timeout 10m -out "$results")

run() {
  for _ in $(seq "$REPEATS"); do
    for impl in rust go; do
      echo "$impl $*" >&2
      "$bin" -impl "$impl" "${common[@]}" "$@" >/dev/null 2>>"$out/errors.log" ||
        echo "FAILED: $impl $*" | tee -a "$out/errors.log" >&2
    done
  done
}

{
  echo "- rust: $(git -C "$here/.." log -1 --format='%h %s')"
  echo "- go: $(git -C "$here/.go-async" log -1 --format='%h %s')"
  echo "- postgres: $(psql "$BENCH_DATABASE_URL" -Atc 'SELECT version()')"
  echo "- postgres settings: $(psql "$BENCH_DATABASE_URL" -Atc "SELECT string_agg(name || '=' || setting, ', ' ORDER BY name) FROM pg_settings WHERE name IN ('shared_buffers', 'max_wal_size', 'checkpoint_timeout', 'synchronous_commit', 'fsync')")"
  echo "- host: $(uname -srm), $(getconf _NPROCESSORS_ONLN) CPUs"
  echo "- repeats: $REPEATS; figures are medians"
} >"$out/env.md"

# Dispatch scaling: queues x replicas, small requests.
for q in 1 4; do
  for r in 1 2 4; do
    run -mode drain -n 100000 -queues "$q" -replicas "$r" -isl 256 -osl 16
  done
done

# Input and output sizes, one queue and replica. Fewer requests as bodies
# grow, so each case writes a similar volume.
run -mode drain -n 50000 -isl 1024 -osl 1024
run -mode drain -n 20000 -isl 8192 -osl 1024
run -mode drain -n 20000 -isl 1024 -osl 8192
run -mode drain -n 2000 -isl 1024 -osl 131072

# Latency from light to heavy load.
for rate in 10 100 1000 5000; do
  run -mode rate -rate "$rate" -duration 20s -isl 256 -osl 16
done

"$bin" summarize "$results" >"$out/summary.md"
cat "$out/env.md" "$out/summary.md"
