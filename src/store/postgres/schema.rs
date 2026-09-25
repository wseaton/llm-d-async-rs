//! Tables and functions of the Postgres store, created on first open.
//!
//! Queue tables churn their whole contents, so autovacuum runs on a fixed row
//! count without cost throttling.

use deadpool_postgres::Pool;

use crate::store::error::StoreError;

const VERSION: i32 = 3;

/// Serializes concurrent migrations: "lda-mig" in ASCII.
const MIGRATE_LOCK: i64 = 0x006c_6461_2d6d_6967;

const CHURN: &str = "fillfactor = 70,
    autovacuum_vacuum_scale_factor = 0,
    autovacuum_vacuum_threshold = 10000,
    autovacuum_vacuum_insert_scale_factor = 0,
    autovacuum_vacuum_insert_threshold = 10000,
    autovacuum_analyze_scale_factor = 0.02,
    autovacuum_vacuum_cost_delay = 0";

fn ddl() -> Vec<String> {
    vec![
        format!(
            "CREATE TABLE IF NOT EXISTS lda_requests (
                seq              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                id               TEXT     NOT NULL,
                request_token    TEXT     NOT NULL,
                queue            TEXT     NOT NULL,
                partition_id     INTEGER  NOT NULL,
                deadline         BIGINT   NOT NULL,
                not_before_ms    BIGINT   NOT NULL DEFAULT 0,
                dispatch_epoch   BIGINT   NOT NULL DEFAULT 0,
                dispatch_attempt BIGINT   NOT NULL DEFAULT 0,
                cancelled        BOOLEAN  NOT NULL DEFAULT false,
                envelope         TEXT     NOT NULL,
                payload          BYTEA,
                UNIQUE (id, request_token)
            ) WITH ({CHURN})"
        ),
        "CREATE INDEX IF NOT EXISTS lda_requests_pending
            ON lda_requests (queue, deadline, seq) WHERE dispatch_epoch = 0"
            .into(),
        "CREATE INDEX IF NOT EXISTS lda_requests_inflight
            ON lda_requests (queue, partition_id) WHERE dispatch_epoch > 0"
            .into(),
        "CREATE TABLE IF NOT EXISTS lda_partitions (
            queue            TEXT    NOT NULL,
            partition_id     INTEGER NOT NULL,
            owner            TEXT    NOT NULL DEFAULT '',
            epoch            BIGINT  NOT NULL DEFAULT 0,
            draining         BOOLEAN NOT NULL DEFAULT false,
            lease_expires_ms BIGINT  NOT NULL DEFAULT 0,
            PRIMARY KEY (queue, partition_id)
        )"
        .into(),
        "CREATE TABLE IF NOT EXISTS lda_dispatchers (
            queue      TEXT   NOT NULL,
            owner      TEXT   NOT NULL,
            expires_ms BIGINT NOT NULL,
            PRIMARY KEY (queue, owner)
        )"
        .into(),
        "CREATE SEQUENCE IF NOT EXISTS lda_dispatch_attempts".into(),
        format!(
            "CREATE TABLE IF NOT EXISTS lda_results (
                seq            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                route          TEXT   NOT NULL,
                body           TEXT   NOT NULL,
                blob           TEXT,
                expires_at_ms  BIGINT,
                lease_owner    TEXT,
                lease_until_ms BIGINT NOT NULL DEFAULT 0
            ) WITH ({CHURN})"
        ),
        "CREATE INDEX IF NOT EXISTS lda_results_route ON lda_results (route, seq)".into(),
        "CREATE INDEX IF NOT EXISTS lda_results_expiry
            ON lda_results (expires_at_ms) WHERE expires_at_ms IS NOT NULL"
            .into(),
        "CREATE TABLE IF NOT EXISTS lda_result_acks (
            route         TEXT   NOT NULL,
            seq           BIGINT NOT NULL,
            expires_at_ms BIGINT NOT NULL,
            PRIMARY KEY (route, seq)
        )"
        .into(),
        "CREATE TABLE IF NOT EXISTS lda_blobs (
            key           TEXT PRIMARY KEY,
            content_type  TEXT NOT NULL,
            expires_at_ms BIGINT
        )"
        .into(),
        "CREATE INDEX IF NOT EXISTS lda_blobs_expiry
            ON lda_blobs (expires_at_ms) WHERE expires_at_ms IS NOT NULL"
            .into(),
        "CREATE TABLE IF NOT EXISTS lda_blob_chunks (
            key        TEXT    NOT NULL,
            idx        INTEGER NOT NULL,
            data       BYTEA   NOT NULL,
            written_ms BIGINT  NOT NULL,
            PRIMARY KEY (key, idx)
        )"
        .into(),
        "ALTER TABLE lda_blob_chunks ALTER COLUMN data SET STORAGE EXTERNAL".into(),
        "CREATE TABLE IF NOT EXISTS lda_kv (
            key   TEXT PRIMARY KEY,
            value BYTEA NOT NULL
        )"
        .into(),
        "CREATE TABLE IF NOT EXISTS lda_quota_keys (key TEXT PRIMARY KEY)".into(),
        "CREATE TABLE IF NOT EXISTS lda_quota_holders (
            holder     TEXT   PRIMARY KEY,
            expires_ms BIGINT NOT NULL
        )"
        .into(),
        "CREATE TABLE IF NOT EXISTS lda_quota_slots (
            key    TEXT    NOT NULL,
            holder TEXT    NOT NULL,
            used   INTEGER NOT NULL,
            PRIMARY KEY (key, holder)
        )"
        .into(),
        "CREATE INDEX IF NOT EXISTS lda_quota_slots_holder ON lda_quota_slots (holder)".into(),
        "CREATE TABLE IF NOT EXISTS lda_quota_windows (
            key       TEXT   NOT NULL,
            window_ms BIGINT NOT NULL,
            used      BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY (key, window_ms)
        )"
        .into(),
        "CREATE TABLE IF NOT EXISTS lda_quota_admits (
            key       TEXT    NOT NULL,
            window_ms BIGINT  NOT NULL,
            at_ms     BIGINT  NOT NULL,
            n         INTEGER NOT NULL
        )"
        .into(),
        "CREATE INDEX IF NOT EXISTS lda_quota_admits_key
            ON lda_quota_admits (key, window_ms, at_ms)"
            .into(),
        "CREATE TABLE IF NOT EXISTS lda_rate_buckets (
            key        TEXT             PRIMARY KEY,
            tokens     DOUBLE PRECISION NOT NULL,
            last_ms    BIGINT           NOT NULL,
            expires_ms BIGINT           NOT NULL
        )"
        .into(),
        PEEK.into(),
        QUOTA_ACQUIRE.into(),
        QUOTA_ADMIT.into(),
        RATE_TAKE.into(),
        "CREATE TABLE IF NOT EXISTS lda_schema (version INTEGER NOT NULL)".into(),
    ]
}

/// The first `p_limit` requests of `p_queue` due by `p_now`, by deadline, in
/// partitions `p_owner` holds and is not draining. It walks the pending index
/// in order and stops at the limit.
const PEEK: &str = "
CREATE OR REPLACE FUNCTION lda_peek(p_queue TEXT, p_owner TEXT, p_now BIGINT, p_limit BIGINT)
RETURNS TABLE (seq BIGINT, deadline BIGINT, envelope TEXT, cancelled BOOLEAN)
LANGUAGE sql STABLE SET enable_seqscan = off SET enable_bitmapscan = off AS $$
    SELECT r.seq, r.deadline, r.envelope, r.cancelled
    FROM lda_requests r
    WHERE r.queue = p_queue AND r.dispatch_epoch = 0 AND r.not_before_ms <= p_now
      AND r.partition_id = ANY(ARRAY(
          SELECT partition_id FROM lda_partitions
          WHERE queue = p_queue AND owner = p_owner AND NOT draining))
    ORDER BY r.deadline, r.seq
    LIMIT p_limit
$$";

/// Grants up to `p_n` of `p_limit` concurrent slots of `p_key` to a live
/// holder; -1 when the holder's lease has lapsed. Locking the key row first
/// gives every later statement on the key a snapshot that includes this one.
/// The sweep deletes idle key rows, so the lock is retried until it holds a
/// row. The window and bucket functions do the same.
const QUOTA_ACQUIRE: &str = "
CREATE OR REPLACE FUNCTION lda_quota_acquire(p_key TEXT, p_holder TEXT, p_n INTEGER, p_limit INTEGER)
RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE
    v_now   BIGINT;
    v_used  BIGINT;
    v_grant INTEGER;
BEGIN
    LOOP
        INSERT INTO lda_quota_keys (key) VALUES (p_key) ON CONFLICT (key) DO NOTHING;
        PERFORM 1 FROM lda_quota_keys WHERE key = p_key FOR UPDATE;
        EXIT WHEN FOUND;
    END LOOP;
    v_now := (extract(epoch FROM clock_timestamp()) * 1000)::bigint;
    PERFORM 1 FROM lda_quota_holders WHERE holder = p_holder AND expires_ms > v_now;
    IF NOT FOUND THEN
        RETURN -1;
    END IF;
    SELECT coalesce(sum(s.used), 0) INTO v_used
    FROM lda_quota_slots s JOIN lda_quota_holders h ON h.holder = s.holder
    WHERE s.key = p_key AND h.expires_ms > v_now;
    v_grant := least(p_n, greatest(p_limit - v_used, 0));
    IF v_grant > 0 THEN
        INSERT INTO lda_quota_slots (key, holder, used) VALUES (p_key, p_holder, v_grant)
        ON CONFLICT (key, holder) DO UPDATE SET used = lda_quota_slots.used + EXCLUDED.used;
    END IF;
    RETURN v_grant;
END $$";

/// Admits up to `p_n` requests for `p_key` while fewer than `p_limit` were
/// admitted in the trailing `p_window_ms`. Each window length keeps its own
/// log, so gates with different windows on one key count only their own.
const QUOTA_ADMIT: &str = "
CREATE OR REPLACE FUNCTION lda_quota_admit(p_key TEXT, p_n INTEGER, p_limit INTEGER, p_window_ms BIGINT)
RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE
    v_now     BIGINT;
    v_used    BIGINT;
    v_expired BIGINT;
    v_grant   INTEGER;
BEGIN
    LOOP
        INSERT INTO lda_quota_windows (key, window_ms) VALUES (p_key, p_window_ms)
        ON CONFLICT (key, window_ms) DO NOTHING;
        SELECT used INTO v_used FROM lda_quota_windows
        WHERE key = p_key AND window_ms = p_window_ms FOR UPDATE;
        EXIT WHEN FOUND;
    END LOOP;
    v_now := (extract(epoch FROM clock_timestamp()) * 1000)::bigint;
    WITH gone AS (
        DELETE FROM lda_quota_admits
        WHERE key = p_key AND window_ms = p_window_ms AND at_ms <= v_now - p_window_ms
        RETURNING n
    )
    SELECT coalesce(sum(n), 0) INTO v_expired FROM gone;
    v_used := v_used - v_expired;
    v_grant := least(p_n, greatest(p_limit - v_used, 0));
    IF v_grant > 0 THEN
        INSERT INTO lda_quota_admits (key, window_ms, at_ms, n) VALUES (p_key, p_window_ms, v_now, v_grant);
    END IF;
    IF v_grant > 0 OR v_expired > 0 THEN
        UPDATE lda_quota_windows SET used = v_used + v_grant
        WHERE key = p_key AND window_ms = p_window_ms;
    END IF;
    RETURN v_grant;
END $$";

/// Takes up to `p_n` tokens from bucket `p_key`, refilled at `p_rate` per
/// second up to `p_capacity`. The bucket starts full again once the expiry
/// stored by the previous take has passed.
const RATE_TAKE: &str = "
CREATE OR REPLACE FUNCTION lda_rate_take(p_key TEXT, p_n INTEGER, p_rate DOUBLE PRECISION,
    p_capacity DOUBLE PRECISION, p_expires_ms BIGINT)
RETURNS INTEGER LANGUAGE plpgsql AS $$
DECLARE
    v_now    BIGINT;
    v_tokens DOUBLE PRECISION;
    v_last   BIGINT;
    v_exp    BIGINT;
    v_grant  INTEGER;
BEGIN
    LOOP
        v_now := (extract(epoch FROM clock_timestamp()) * 1000)::bigint;
        INSERT INTO lda_rate_buckets (key, tokens, last_ms, expires_ms)
        VALUES (p_key, p_capacity, v_now, 0) ON CONFLICT (key) DO NOTHING;
        SELECT tokens, last_ms, expires_ms INTO v_tokens, v_last, v_exp
        FROM lda_rate_buckets WHERE key = p_key FOR UPDATE;
        EXIT WHEN FOUND;
    END LOOP;
    IF v_now >= v_exp THEN
        v_tokens := p_capacity;
        v_last := v_now;
    END IF;
    v_now := greatest(v_now, v_last);
    v_tokens := least(v_tokens + (v_now - v_last) / 1000.0 * p_rate, p_capacity);
    v_grant := least(p_n, floor(v_tokens))::integer;
    v_grant := greatest(v_grant, 0);
    UPDATE lda_rate_buckets
    SET tokens = v_tokens - v_grant, last_ms = v_now, expires_ms = p_expires_ms
    WHERE key = p_key;
    RETURN v_grant;
END $$";

/// Creates the schema. Safe to run from many replicas at once; a current
/// schema takes no locks.
pub async fn migrate(pool: &Pool) -> Result<(), StoreError> {
    let mut client = pool.get().await?;
    let current: bool = client
        .query_one("SELECT to_regclass('lda_schema') IS NOT NULL", &[])
        .await?
        .get(0);
    if current {
        let version: Option<i32> = client
            .query_opt("SELECT max(version) FROM lda_schema", &[])
            .await?
            .and_then(|row| row.get(0));
        if version == Some(VERSION) {
            return Ok(());
        }
    }
    let txn = client.transaction().await?;
    txn.execute("SELECT pg_advisory_xact_lock($1)", &[&MIGRATE_LOCK])
        .await?;
    for statement in ddl() {
        txn.batch_execute(&statement).await?;
    }
    let version: Option<i32> = txn
        .query_one("SELECT max(version) FROM lda_schema", &[])
        .await?
        .get(0);
    match version {
        Some(v) if v > VERSION => {
            return Err(StoreError::Config(format!(
                "database schema version {v} is newer than this build ({VERSION})"
            )));
        }
        Some(v) if v == VERSION => {}
        _ => {
            txn.execute("DELETE FROM lda_schema", &[]).await?;
            txn.execute("INSERT INTO lda_schema (version) VALUES ($1)", &[&VERSION])
                .await?;
        }
    }
    txn.commit().await?;
    Ok(())
}
