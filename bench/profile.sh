#!/usr/bin/env bash
# Runs a production traffic profile against BENCH_DATABASE_URL: TOKENS_PER_S
# prompt plus completion tokens a second, for each ISL/OSL shape in SHAPES.
# Each shape's request rate is TOKENS_PER_S / (ISL + OSL); the gateway answers
# after OSL / DECODE_TPS seconds, and each replica gets enough workers for the
# requests that keeps in flight. Writes env.md, results.jsonl and summary.md
# under runs/profile-<time>.
set -euo pipefail

: "${BENCH_DATABASE_URL:?set BENCH_DATABASE_URL (see postgres.sh)}"
TOKENS_PER_S=${TOKENS_PER_S:-500000}
SHAPES=${SHAPES:-"2048:512 8192:1024"}
DECODE_TPS=${DECODE_TPS:-50}
QUEUES=${QUEUES:-4}
REPLICAS=${REPLICAS:-3}
DURATION=${DURATION:-120s}
REPEATS=${REPEATS:-1}
IMPLS=${IMPLS:-"rust go"}
here=$(cd "$(dirname "$0")" && pwd)
bin="$here/.bin/bench"
out="$here/runs/profile-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$out"
results="$out/results.jsonl"

{
  echo "- rust: $(git -C "$here/.." log -1 --format='%h %s')"
  echo "- go: $(git -C "$here/.go-async" log -1 --format='%h %s')"
  echo "- postgres: $(psql "$BENCH_DATABASE_URL" -Atc 'SELECT version()')"
  echo "- host: $(uname -srm), $(getconf _NPROCESSORS_ONLN) CPUs"
  echo "- profile: $TOKENS_PER_S tokens/s, shapes $SHAPES, decode $DECODE_TPS tokens/s,"
  echo "  $QUEUES queues, $REPLICAS replicas, $DURATION per run, repeats $REPEATS"
} >"$out/env.md"

for shape in $SHAPES; do
  isl=${shape%%:*}
  osl=${shape##*:}
  rate=$((TOKENS_PER_S / (isl + osl)))
  delay_ms=$((osl * 1000 / DECODE_TPS))
  in_flight=$((rate * delay_ms / 1000))
  concurrency=$(((in_flight * 3 / 2 + REPLICAS - 1) / REPLICAS))
  for _ in $(seq "$REPEATS"); do
    for impl in $IMPLS; do
      echo "$impl isl=$isl osl=$osl rate=$rate delay=${delay_ms}ms concurrency=$concurrency" >&2
      "$bin" -impl "$impl" -mode rate -rate "$rate" -duration "$DURATION" \
        -isl "$isl" -osl "$osl" -gateway-delay "${delay_ms}ms" \
        -queues "$QUEUES" -replicas "$REPLICAS" -concurrency "$concurrency" \
        -batch-size 100 -poll-interval-ms 100 -timeout 10m -out "$results" \
        >/dev/null 2>>"$out/errors.log" ||
        echo "FAILED: $impl $shape" | tee -a "$out/errors.log" >&2
    done
  done
done

"$bin" summarize "$results" >"$out/summary.md"
cat "$out/env.md" "$out/summary.md"
