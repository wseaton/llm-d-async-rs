use std::collections::HashSet;

use crate::store::blob::BlobBody;
use crate::store::blob::key::BlobKey;
use crate::store::error::StoreError;
use crate::store::postgres::{DB_NOW_MS, Inner};
use crate::store::queue::{ACK_TOMBSTONE_TTL_MS, AckOutcome, ResultClaim};

fn claim_id(seq: i64) -> u64 {
    u64::try_from(seq).unwrap_or(0)
}

fn seq_of(claim_id: u64) -> i64 {
    i64::try_from(claim_id).unwrap_or(i64::MAX)
}

/// Deletes counters that no longer count anything: quota keys without slots,
/// rate windows whose admissions all aged out, and buckets past their
/// expiry, which would start full again anyway. Rows a counter statement
/// holds are skipped.
async fn sweep_counters(txn: &tokio_postgres::Transaction<'_>) -> Result<u64, StoreError> {
    let keys = txn
        .execute(
            "DELETE FROM lda_quota_keys WHERE key IN (
                SELECT k.key FROM lda_quota_keys k
                WHERE NOT EXISTS (SELECT 1 FROM lda_quota_slots s WHERE s.key = k.key AND s.used > 0)
                FOR UPDATE SKIP LOCKED
             )",
            &[],
        )
        .await?;
    txn.execute("DELETE FROM lda_quota_slots s WHERE s.used <= 0", &[])
        .await?;
    let windows = txn
        .execute(
            &format!(
                "WITH idle AS (
                    SELECT w.key, w.window_ms FROM lda_quota_windows w
                    WHERE NOT EXISTS (
                        SELECT 1 FROM lda_quota_admits a
                        WHERE a.key = w.key AND a.window_ms = w.window_ms
                          AND a.at_ms > {DB_NOW_MS} - w.window_ms
                    )
                    FOR UPDATE SKIP LOCKED
                 ), admits AS (
                    DELETE FROM lda_quota_admits a USING idle
                    WHERE a.key = idle.key AND a.window_ms = idle.window_ms
                 )
                 DELETE FROM lda_quota_windows w USING idle
                 WHERE w.key = idle.key AND w.window_ms = idle.window_ms"
            ),
            &[],
        )
        .await?;
    let buckets = txn
        .execute(
            &format!(
                "DELETE FROM lda_rate_buckets WHERE key IN (
                    SELECT key FROM lda_rate_buckets WHERE expires_ms < {DB_NOW_MS}
                    FOR UPDATE SKIP LOCKED
                 )"
            ),
            &[],
        )
        .await?;
    Ok(keys + windows + buckets)
}

impl Inner {
    pub(crate) async fn pop_result(
        &self,
        route: String,
        now_ms: i64,
    ) -> Result<Option<String>, StoreError> {
        let retained = now_ms.saturating_add(self.result_blob_retention_ms);
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let row = txn
            .query_opt(
                "WITH picked AS MATERIALIZED (
                    SELECT seq FROM lda_results
                    WHERE route = $1 AND lease_until_ms <= $2
                      AND (expires_at_ms IS NULL OR expires_at_ms > $2)
                    ORDER BY seq LIMIT 1
                    FOR UPDATE SKIP LOCKED
                 )
                 DELETE FROM lda_results r USING picked WHERE r.seq = picked.seq
                 RETURNING r.body, r.blob",
                &[&route, &now_ms],
            )
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let body: String = row.get(0);
        let blob: Option<String> = row.get(1);
        if let Some(blob) = blob {
            txn.execute(
                "UPDATE lda_blobs SET expires_at_ms = least(coalesce(expires_at_ms, $2), $2)
                 WHERE key = $1",
                &[&blob, &retained],
            )
            .await?;
        }
        txn.commit().await?;
        Ok(Some(body))
    }

    pub(crate) async fn claim_result(
        &self,
        route: String,
        owner: String,
        lease_ms: i64,
        now_ms: i64,
    ) -> Result<Option<ResultClaim>, StoreError> {
        let client = self.pool.get().await?;
        let row = client
            .query_opt(
                "WITH picked AS MATERIALIZED (
                    SELECT seq FROM lda_results
                    WHERE route = $1 AND lease_until_ms <= $2
                      AND (expires_at_ms IS NULL OR expires_at_ms > $2)
                    ORDER BY seq LIMIT 1
                    FOR UPDATE SKIP LOCKED
                 )
                 UPDATE lda_results r SET lease_owner = $3, lease_until_ms = $2 + $4
                 FROM picked WHERE r.seq = picked.seq
                 RETURNING r.seq, r.body",
                &[&route, &now_ms, &owner, &lease_ms],
            )
            .await?;
        Ok(row.map(|row| ResultClaim {
            claim_id: claim_id(row.get(0)),
            body: row.get(1),
        }))
    }

    pub(crate) async fn renew_result(
        &self,
        route: String,
        claim: u64,
        owner: String,
        lease_ms: i64,
        now_ms: i64,
    ) -> Result<bool, StoreError> {
        let client = self.pool.get().await?;
        let n = client
            .execute(
                "UPDATE lda_results SET lease_until_ms = $4 + $5
                 WHERE route = $1 AND seq = $2 AND lease_owner = $3 AND lease_until_ms > $4",
                &[&route, &seq_of(claim), &owner, &now_ms, &lease_ms],
            )
            .await?;
        Ok(n == 1)
    }

    pub(crate) async fn ack_result(
        &self,
        route: String,
        claim: u64,
        owner: String,
        now_ms: i64,
    ) -> Result<AckOutcome, StoreError> {
        let seq = seq_of(claim);
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let acked = txn
            .query_opt(
                "DELETE FROM lda_results
                 WHERE route = $1 AND seq = $2 AND lease_owner = $3 AND lease_until_ms > $4
                 RETURNING blob",
                &[&route, &seq, &owner, &now_ms],
            )
            .await?;
        let mut dropped = Vec::new();
        let outcome = match acked {
            Some(row) => {
                txn.execute(
                    "INSERT INTO lda_result_acks (route, seq, expires_at_ms) VALUES ($1, $2, $3)
                     ON CONFLICT (route, seq) DO UPDATE SET expires_at_ms = EXCLUDED.expires_at_ms",
                    &[&route, &seq, &now_ms.saturating_add(ACK_TOMBSTONE_TTL_MS)],
                )
                .await?;
                if let Some(blob) = row.get::<_, Option<String>>(0) {
                    txn.execute("DELETE FROM lda_blobs WHERE key = $1", &[&blob])
                        .await?;
                    dropped.extend(BlobKey::parse(&blob));
                }
                AckOutcome::Acked
            }
            None => {
                let tombstone = txn
                    .query_opt(
                        "SELECT 1 FROM lda_result_acks
                         WHERE route = $1 AND seq = $2 AND expires_at_ms > $3",
                        &[&route, &seq, &now_ms],
                    )
                    .await?;
                if tombstone.is_some() {
                    AckOutcome::AlreadyAcked
                } else {
                    AckOutcome::OwnershipLost
                }
            }
        };
        txn.commit().await?;
        drop(client);
        self.blobs.remove_dropped(&dropped).await;
        Ok(outcome)
    }

    pub(crate) async fn open_result_blob(
        &self,
        key: BlobKey,
        now_ms: i64,
    ) -> Result<Option<(BlobBody, String)>, StoreError> {
        let client = self.pool.get().await?;
        let row = client
            .query_opt(
                "SELECT content_type FROM lda_blobs
                 WHERE key = $1 AND (expires_at_ms IS NULL OR expires_at_ms > $2)",
                &[&key.to_string(), &now_ms],
            )
            .await?;
        drop(client);
        let Some(row) = row else {
            return Ok(None);
        };
        let content_type: String = row.get(0);
        Ok(self
            .blobs
            .open(&key)
            .await?
            .map(|body| (body, content_type)))
    }

    pub(crate) async fn result_depth(&self, route: String, now_ms: i64) -> Result<u64, StoreError> {
        let client = self.pool.get().await?;
        let n: i64 = client
            .query_one(
                "SELECT count(*) FROM lda_results
                 WHERE route = $1 AND lease_until_ms <= $2
                   AND (expires_at_ms IS NULL OR expires_at_ms > $2)",
                &[&route, &now_ms],
            )
            .await?
            .get(0);
        Ok(u64::try_from(n).unwrap_or(0))
    }

    pub(crate) async fn kv_get(&self, key: String) -> Result<Option<Vec<u8>>, StoreError> {
        let client = self.pool.get().await?;
        Ok(client
            .query_opt("SELECT value FROM lda_kv WHERE key = $1", &[&key])
            .await?
            .map(|row| row.get(0)))
    }

    pub(crate) async fn kv_put(
        &self,
        key: String,
        value: Option<Vec<u8>>,
    ) -> Result<(), StoreError> {
        let client = self.pool.get().await?;
        match value {
            Some(value) => {
                client
                    .execute(
                        "INSERT INTO lda_kv (key, value) VALUES ($1, $2)
                         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
                        &[&key, &value],
                    )
                    .await?;
            }
            None => {
                client
                    .execute("DELETE FROM lda_kv WHERE key = $1", &[&key])
                    .await?;
            }
        }
        Ok(())
    }

    /// Deletes expired results, old ack tombstones, and blobs past their
    /// retention.
    pub(crate) async fn sweep(&self, now_ms: i64) -> Result<u64, StoreError> {
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let results = txn
            .execute(
                "DELETE FROM lda_results WHERE expires_at_ms IS NOT NULL AND expires_at_ms <= $1",
                &[&now_ms],
            )
            .await?;
        let acks = txn
            .execute(
                "DELETE FROM lda_result_acks WHERE expires_at_ms <= $1",
                &[&now_ms],
            )
            .await?;
        let blobs = txn
            .query(
                "DELETE FROM lda_blobs WHERE expires_at_ms IS NOT NULL AND expires_at_ms <= $1
                 RETURNING key",
                &[&now_ms],
            )
            .await?;
        let counters = sweep_counters(&txn).await?;
        txn.commit().await?;
        drop(client);
        let dropped: Vec<BlobKey> = blobs
            .iter()
            .filter_map(|row| BlobKey::parse(row.get::<_, &str>(0)))
            .collect();
        self.blobs.remove_dropped(&dropped).await;
        Ok(results + acks + counters + u64::try_from(blobs.len()).unwrap_or(0))
    }

    pub(crate) async fn collect_orphans(&self, cutoff_ms: i64) -> Result<usize, StoreError> {
        let listed = self.blobs.list(cutoff_ms).await?;
        if listed.is_empty() {
            return Ok(0);
        }
        let names: Vec<String> = listed.iter().map(|l| l.key.to_string()).collect();
        let client = self.pool.get().await?;
        let referenced: HashSet<String> = client
            .query("SELECT key FROM lda_blobs WHERE key = ANY($1)", &[&names])
            .await?
            .iter()
            .map(|row| row.get(0))
            .collect();
        drop(client);
        let mut removed = 0;
        for l in listed {
            if !referenced.contains(&l.key.to_string()) {
                self.blobs.remove(&l.key).await?;
                removed += 1;
            }
        }
        Ok(removed)
    }
}
