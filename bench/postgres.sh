#!/usr/bin/env bash
# A Postgres 17 for benchmarking, with pg_stat_statements loaded. Durability
# settings are the defaults: numbers reflect a real fsync'ing database. WAL
# limits are raised so that a run does not straddle a checkpoint; the bench
# checkpoints before each run.
set -euo pipefail

PORT=${PORT:-55433}
NAME=${NAME:-lda-bench-pg}
ENGINE=${ENGINE:-podman}

"$ENGINE" rm -f "$NAME" >/dev/null 2>&1 || true
"$ENGINE" run -d --name "$NAME" -p "$PORT:5432" \
  -e POSTGRES_PASSWORD=postgres \
  --shm-size=1g \
  postgres:17 \
  -c shared_preload_libraries=pg_stat_statements \
  -c pg_stat_statements.track=all \
  -c max_connections=500 \
  -c shared_buffers=1GB \
  -c max_wal_size=16GB \
  -c checkpoint_timeout=30min >/dev/null

until pg_isready -h 127.0.0.1 -p "$PORT" -U postgres >/dev/null 2>&1; do sleep 0.5; done
echo "postgres://postgres:postgres@127.0.0.1:$PORT/postgres?sslmode=disable"
