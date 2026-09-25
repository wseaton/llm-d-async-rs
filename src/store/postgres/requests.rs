use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use tokio_postgres::Transaction;

use crate::api::payload::PayloadStorage;
use crate::api::request::InternalRequest;
use crate::api::result::ResultMessage;
use crate::store::blob::key::BlobKey;
use crate::store::error::StoreError;
use crate::store::postgres::partitions::{Tracked, partition_of};
use crate::store::postgres::{DB_NOW_MS, Inner, RESULTS_CHANNEL};
use crate::store::queue::{
    Admission, Admitted, Applied, Backlog, ClaimRef, NewRequest, Outcome, PayloadBody, Peeked,
    PendingKey,
};
use crate::store::staging::StagedPayload;

/// Largest number of rows one statement inserts.
const INSERT_CHUNK: usize = 1000;

fn seq_of(key: PendingKey) -> i64 {
    i64::try_from(key.seq).unwrap_or(i64::MAX)
}

fn split_generation(generation: &str) -> Option<(&str, &str)> {
    generation.split_once('\0')
}

/// A result about to be written, and the blob it keeps alive.
struct NewResult {
    route: String,
    body: String,
    expires_at_ms: Option<i64>,
    blob: Option<(BlobKey, String)>,
}

impl NewResult {
    fn of(
        envelope: &InternalRequest,
        result: &ResultMessage,
        now_ms: i64,
    ) -> Result<Self, StoreError> {
        let ttl_ms = i64::try_from(envelope.routing.result_ttl_seconds.saturating_mul(1000))
            .unwrap_or(i64::MAX);
        Ok(Self {
            route: envelope.routing.result_queue_name.clone(),
            body: serde_json::to_string(result)?,
            expires_at_ms: (ttl_ms > 0).then(|| now_ms.saturating_add(ttl_ms)),
            blob: BlobKey::from_ref(&result.payload_ref).map(|k| (k, result.content_type.clone())),
        })
    }
}

/// Writes results and registers the blobs they reference, which live as
/// long as their result.
async fn insert_results(txn: &Transaction<'_>, results: &[NewResult]) -> Result<(), StoreError> {
    if results.is_empty() {
        return Ok(());
    }
    let routes: Vec<&str> = results.iter().map(|r| r.route.as_str()).collect();
    let bodies: Vec<&str> = results.iter().map(|r| r.body.as_str()).collect();
    let blobs: Vec<Option<String>> = results
        .iter()
        .map(|r| r.blob.as_ref().map(|(k, _)| k.to_string()))
        .collect();
    let expiries: Vec<Option<i64>> = results.iter().map(|r| r.expires_at_ms).collect();
    txn.execute(
        "INSERT INTO lda_results (route, body, blob, expires_at_ms)
         SELECT route, body, blob, expires_at_ms
         FROM unnest($1::text[], $2::text[], $3::text[], $4::bigint[])
              WITH ORDINALITY AS t(route, body, blob, expires_at_ms, ord)
         ORDER BY ord",
        &[&routes, &bodies, &blobs, &expiries],
    )
    .await?;
    let registered: Vec<(String, &str, Option<i64>)> = results
        .iter()
        .filter_map(|r| {
            r.blob
                .as_ref()
                .map(|(k, ct)| (k.to_string(), ct.as_str(), r.expires_at_ms))
        })
        .collect();
    if !registered.is_empty() {
        let keys: Vec<&str> = registered.iter().map(|(k, _, _)| k.as_str()).collect();
        let types: Vec<&str> = registered.iter().map(|(_, t, _)| *t).collect();
        let expiries: Vec<Option<i64>> = registered.iter().map(|(_, _, e)| *e).collect();
        txn.execute(
            "INSERT INTO lda_blobs (key, content_type, expires_at_ms)
             SELECT * FROM unnest($1::text[], $2::text[], $3::bigint[])
             ON CONFLICT (key) DO UPDATE
             SET content_type = EXCLUDED.content_type, expires_at_ms = EXCLUDED.expires_at_ms",
            &[&keys, &types, &expiries],
        )
        .await?;
    }
    txn.execute("SELECT pg_notify($1, '')", &[&RESULTS_CHANNEL])
        .await?;
    Ok(())
}

/// Unregisters request body blobs of rows just deleted and returns them for
/// deletion once the transaction commits.
async fn drop_request_blobs(
    txn: &Transaction<'_>,
    tokens: &[String],
) -> Result<Vec<BlobKey>, StoreError> {
    let keys: Vec<BlobKey> = tokens.iter().filter_map(|t| BlobKey::request(t)).collect();
    if keys.is_empty() {
        return Ok(keys);
    }
    let names: Vec<String> = keys.iter().map(ToString::to_string).collect();
    txn.execute("DELETE FROM lda_blobs WHERE key = ANY($1)", &[&names])
        .await?;
    Ok(keys)
}

impl Inner {
    pub(crate) async fn submit(&self, requests: Vec<NewRequest>) -> Result<(), StoreError> {
        if requests.is_empty() {
            return Ok(());
        }
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        for chunk in requests.chunks(INSERT_CHUNK) {
            let mut ids = Vec::with_capacity(chunk.len());
            let mut tokens = Vec::with_capacity(chunk.len());
            let mut queues = Vec::with_capacity(chunk.len());
            let mut partitions = Vec::with_capacity(chunk.len());
            let mut deadlines = Vec::with_capacity(chunk.len());
            let mut envelopes = Vec::with_capacity(chunk.len());
            let mut payloads: Vec<Option<&[u8]>> = Vec::with_capacity(chunk.len());
            let mut blob_keys = Vec::new();
            let mut blob_types = Vec::new();
            for r in chunk {
                let env = &r.envelope;
                ids.push(env.request.id.as_str());
                tokens.push(env.routing.request_token.as_str());
                queues.push(env.routing.request_queue_name.as_str());
                partitions.push(partition_of(&env.request.id));
                deadlines.push(env.request.deadline);
                envelopes.push(serde_json::to_string(env)?);
                match &r.payload {
                    StagedPayload::Inline(bytes) => payloads.push(Some(bytes.as_slice())),
                    StagedPayload::Blob { key, .. } => {
                        payloads.push(None);
                        blob_keys.push(key.to_string());
                        blob_types.push(env.payload.content_type.as_str());
                    }
                }
            }
            txn.execute(
                "INSERT INTO lda_requests
                    (id, request_token, queue, partition_id, deadline, envelope, payload)
                 SELECT id, request_token, queue, partition_id, deadline, envelope, payload
                 FROM unnest($1::text[], $2::text[], $3::text[], $4::int[], $5::bigint[],
                             $6::text[], $7::bytea[])
                      WITH ORDINALITY AS t(id, request_token, queue, partition_id, deadline,
                                           envelope, payload, ord)
                 ORDER BY ord",
                &[
                    &ids,
                    &tokens,
                    &queues,
                    &partitions,
                    &deadlines,
                    &envelopes,
                    &payloads,
                ],
            )
            .await?;
            if !blob_keys.is_empty() {
                txn.execute(
                    "INSERT INTO lda_blobs (key, content_type)
                     SELECT * FROM unnest($1::text[], $2::text[])
                     ON CONFLICT (key) DO UPDATE
                     SET content_type = EXCLUDED.content_type, expires_at_ms = NULL",
                    &[&blob_keys, &blob_types],
                )
                .await?;
            }
        }
        txn.commit().await?;
        Ok(())
    }

    pub(crate) async fn peek(
        &self,
        queue: String,
        limit: usize,
        now_ms: i64,
    ) -> Result<Vec<Peeked>, StoreError> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "SELECT r.seq, r.deadline, r.envelope, r.cancelled
                 FROM lda_requests r
                 JOIN lda_partitions p ON p.queue = r.queue AND p.partition_id = r.partition_id
                 WHERE r.queue = $1 AND r.dispatch_epoch = 0 AND r.not_before_ms <= $3
                   AND p.owner = $2 AND NOT p.draining
                 ORDER BY r.deadline, r.seq
                 LIMIT $4",
                &[&queue, &self.owner, &now_ms, &limit],
            )
            .await?;
        Ok(rows
            .iter()
            .map(|row| {
                let seq: i64 = row.get(0);
                let envelope: &str = row.get(2);
                Peeked {
                    key: PendingKey {
                        deadline: row.get(1),
                        seq: u64::try_from(seq).unwrap_or(0),
                    },
                    envelope: serde_json::from_str(envelope).map_err(|e| e.to_string()),
                    cancelled: row.get(3),
                }
            })
            .collect())
    }

    pub(crate) async fn has_pending(&self, queue: String, now_ms: i64) -> Result<bool, StoreError> {
        let client = self.pool.get().await?;
        Ok(client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM lda_requests
                                WHERE queue = $1 AND dispatch_epoch = 0 AND not_before_ms <= $2)",
                &[&queue, &now_ms],
            )
            .await?
            .get(0))
    }

    pub(crate) async fn admit(
        &self,
        queue: String,
        admissions: Vec<Admission>,
        now_ms: i64,
    ) -> Result<Vec<Admitted>, StoreError> {
        let mut client = self.pool.get().await?;
        let cycle = self.cycle(&queue)?;
        let _cycle = cycle.lock().await;
        let written = self
            .admit_locked(&mut client, &queue, &admissions, now_ms)
            .await;
        drop(client);
        let (out, claimed, dropped) = match written {
            Ok(done) => done,
            Err(e) => {
                if let Ok(mut state) = self.state.lock()
                    && let Some(q) = state.queues.get_mut(&queue)
                {
                    q.reconcile = true;
                }
                return Err(e);
            }
        };
        if let Ok(mut state) = self.state.lock() {
            for t in claimed {
                state
                    .claims
                    .insert(crate::api::request::generation_key(&t.id, &t.token), t);
            }
        }
        drop(_cycle);
        self.blobs.remove_dropped(&dropped).await;
        if out.contains(&Admitted::Finished) {
            self.results.notify_waiters();
        }
        Ok(out)
    }

    async fn admit_locked(
        &self,
        client: &mut deadpool_postgres::Object,
        queue: &str,
        admissions: &[Admission],
        now_ms: i64,
    ) -> Result<(Vec<Admitted>, Vec<Tracked>, Vec<BlobKey>), StoreError> {
        let mut seen = HashSet::new();
        let mut claims = Vec::new();
        let mut removals = Vec::new();
        for (i, a) in admissions.iter().enumerate() {
            let seq = seq_of(a.key());
            if !seen.insert(seq) {
                continue;
            }
            match a {
                Admission::Claim { .. } => claims.push(seq),
                Admission::Finish { .. } | Admission::Discard { .. } => removals.push((i, seq)),
            }
        }
        let mut out: Vec<Admitted> = admissions.iter().map(|_| Admitted::Gone).collect();
        let by_seq: HashMap<i64, usize> = admissions
            .iter()
            .enumerate()
            .map(|(i, a)| (seq_of(a.key()), i))
            .rev()
            .collect();

        let txn = client.transaction().await?;
        let mut tracked = Vec::new();
        if !claims.is_empty() {
            let rows = txn
                .query(
                    &format!(
                        "UPDATE lda_requests r
                         SET dispatch_epoch = p.epoch,
                             dispatch_attempt = nextval('lda_dispatch_attempts')
                         FROM lda_partitions p
                         WHERE r.seq = ANY($1) AND r.queue = $2 AND r.dispatch_epoch = 0
                           AND p.queue = r.queue AND p.partition_id = r.partition_id
                           AND p.owner = $3 AND NOT p.draining
                           AND p.lease_expires_ms > {DB_NOW_MS}
                         RETURNING r.seq, r.id, r.request_token, r.partition_id,
                                   r.dispatch_epoch, r.dispatch_attempt"
                    ),
                    &[&claims, &queue, &self.owner],
                )
                .await?;
            for row in rows {
                let seq: i64 = row.get(0);
                let Some(&i) = by_seq.get(&seq) else {
                    continue;
                };
                let t = Tracked {
                    queue: queue.to_owned(),
                    id: row.get(1),
                    token: row.get(2),
                    partition: row.get(3),
                    epoch: row.get(4),
                    attempt: row.get(5),
                };
                if let Some(slot) = out.get_mut(i) {
                    *slot = Admitted::Claimed(ClaimRef {
                        generation: crate::api::request::generation_key(&t.id, &t.token),
                        claim_id: u64::try_from(t.attempt).unwrap_or(0),
                    });
                }
                tracked.push(t);
            }
        }
        let mut dropped = Vec::new();
        if !removals.is_empty() {
            let seqs: Vec<i64> = removals.iter().map(|(_, s)| *s).collect();
            let rows = txn
                .query(
                    "DELETE FROM lda_requests r USING lda_partitions p
                     WHERE r.seq = ANY($1) AND r.queue = $2 AND r.dispatch_epoch = 0
                       AND p.queue = r.queue AND p.partition_id = r.partition_id
                       AND p.owner = $3
                     RETURNING r.seq, r.request_token, r.payload IS NULL",
                    &[&seqs, &queue, &self.owner],
                )
                .await?;
            let mut blob_tokens = Vec::new();
            let mut results = Vec::new();
            let mut finished: Vec<usize> = Vec::new();
            for row in rows {
                let seq: i64 = row.get(0);
                if row.get::<_, bool>(2) {
                    blob_tokens.push(row.get::<_, String>(1));
                }
                if let Some(&i) = by_seq.get(&seq) {
                    finished.push(i);
                }
            }
            finished.sort_unstable();
            for i in finished {
                if let Some(Admission::Finish {
                    envelope, result, ..
                }) = admissions.get(i)
                {
                    results.push(NewResult::of(envelope, result, now_ms)?);
                }
                if let Some(slot) = out.get_mut(i) {
                    *slot = Admitted::Finished;
                }
            }
            dropped = drop_request_blobs(&txn, &blob_tokens).await?;
            insert_results(&txn, &results).await?;
        }
        txn.commit().await?;
        Ok((out, tracked, dropped))
    }

    pub(crate) async fn apply_outcomes(
        &self,
        outcomes: Vec<Outcome>,
        now_ms: i64,
    ) -> Result<Applied, StoreError> {
        let mut finishes = Vec::new();
        let mut retries = Vec::new();
        let mut releases = Vec::new();
        let mut fenced_blobs = Vec::new();
        let mut applied = Applied::default();
        for outcome in &outcomes {
            let claim = outcome.claim();
            let Some((id, token)) = split_generation(&claim.generation) else {
                applied.fenced += 1;
                continue;
            };
            let attempt = i64::try_from(claim.claim_id).unwrap_or(i64::MAX);
            match outcome {
                Outcome::Finish {
                    envelope, result, ..
                } => finishes.push((id, token, attempt, envelope, result)),
                Outcome::Retry {
                    envelope, due_ms, ..
                } => retries.push((
                    id,
                    token,
                    attempt,
                    *due_ms,
                    serde_json::to_string(envelope)?,
                )),
                Outcome::Release { .. } => releases.push((id, token, attempt)),
            }
        }

        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let mut dropped = Vec::new();
        if !finishes.is_empty() {
            let ids: Vec<&str> = finishes.iter().map(|f| f.0).collect();
            let tokens: Vec<&str> = finishes.iter().map(|f| f.1).collect();
            let attempts: Vec<i64> = finishes.iter().map(|f| f.2).collect();
            let rows = txn
                .query(
                    "DELETE FROM lda_requests r
                     USING unnest($2::text[], $3::text[], $4::bigint[])
                           AS k(id, request_token, attempt),
                           lda_partitions p
                     WHERE r.id = k.id AND r.request_token = k.request_token
                       AND r.dispatch_epoch > 0 AND r.dispatch_attempt = k.attempt
                       AND p.queue = r.queue AND p.partition_id = r.partition_id
                       AND p.owner = $1 AND p.epoch = r.dispatch_epoch
                     RETURNING r.id, r.request_token, r.dispatch_attempt, r.payload IS NULL",
                    &[&self.owner, &ids, &tokens, &attempts],
                )
                .await?;
            let mut done = HashSet::new();
            let mut blob_tokens = Vec::new();
            for row in rows {
                let id: String = row.get(0);
                let token: String = row.get(1);
                let attempt: i64 = row.get(2);
                if row.get::<_, bool>(3) {
                    blob_tokens.push(token.clone());
                }
                done.insert((id, token, attempt));
            }
            let mut results = Vec::new();
            for (id, token, attempt, envelope, result) in &finishes {
                if done.contains(&((*id).to_owned(), (*token).to_owned(), *attempt)) {
                    results.push(NewResult::of(envelope, result, now_ms)?);
                } else {
                    applied.fenced += 1;
                    if let Some(key) = BlobKey::from_ref(&result.payload_ref) {
                        fenced_blobs.push(key);
                    }
                }
            }
            applied.results_written = results.len();
            dropped = drop_request_blobs(&txn, &blob_tokens).await?;
            insert_results(&txn, &results).await?;
        }
        if !retries.is_empty() {
            let ids: Vec<&str> = retries.iter().map(|r| r.0).collect();
            let tokens: Vec<&str> = retries.iter().map(|r| r.1).collect();
            let attempts: Vec<i64> = retries.iter().map(|r| r.2).collect();
            let due: Vec<i64> = retries.iter().map(|r| r.3).collect();
            let envelopes: Vec<&str> = retries.iter().map(|r| r.4.as_str()).collect();
            let n = txn
                .execute(
                    "UPDATE lda_requests r
                     SET dispatch_epoch = 0, not_before_ms = k.due, envelope = k.envelope
                     FROM unnest($2::text[], $3::text[], $4::bigint[], $5::bigint[], $6::text[])
                          AS k(id, request_token, attempt, due, envelope),
                          lda_partitions p
                     WHERE r.id = k.id AND r.request_token = k.request_token
                       AND r.dispatch_epoch > 0 AND r.dispatch_attempt = k.attempt
                       AND p.queue = r.queue AND p.partition_id = r.partition_id
                       AND p.owner = $1",
                    &[&self.owner, &ids, &tokens, &attempts, &due, &envelopes],
                )
                .await?;
            applied.fenced += retries
                .len()
                .saturating_sub(usize::try_from(n).unwrap_or(0));
        }
        if !releases.is_empty() {
            let ids: Vec<&str> = releases.iter().map(|r| r.0).collect();
            let tokens: Vec<&str> = releases.iter().map(|r| r.1).collect();
            let attempts: Vec<i64> = releases.iter().map(|r| r.2).collect();
            let n = txn
                .execute(
                    "UPDATE lda_requests r SET dispatch_epoch = 0
                     FROM unnest($2::text[], $3::text[], $4::bigint[])
                          AS k(id, request_token, attempt),
                          lda_partitions p
                     WHERE r.id = k.id AND r.request_token = k.request_token
                       AND r.dispatch_epoch > 0 AND r.dispatch_attempt = k.attempt
                       AND p.queue = r.queue AND p.partition_id = r.partition_id
                       AND p.owner = $1",
                    &[&self.owner, &ids, &tokens, &attempts],
                )
                .await?;
            applied.fenced += releases
                .len()
                .saturating_sub(usize::try_from(n).unwrap_or(0));
        }
        if !fenced_blobs.is_empty() {
            let names: Vec<String> = fenced_blobs.iter().map(ToString::to_string).collect();
            let registered: HashSet<String> = txn
                .query("SELECT key FROM lda_blobs WHERE key = ANY($1)", &[&names])
                .await?
                .iter()
                .map(|row| row.get(0))
                .collect();
            dropped.extend(
                fenced_blobs
                    .into_iter()
                    .filter(|k| !registered.contains(&k.to_string())),
            );
        }
        txn.commit().await?;
        drop(client);

        if let Ok(mut state) = self.state.lock() {
            for outcome in &outcomes {
                let claim = outcome.claim();
                let attempt = i64::try_from(claim.claim_id).unwrap_or(i64::MAX);
                if state
                    .claims
                    .get(&claim.generation)
                    .is_some_and(|t| t.attempt == attempt)
                {
                    state.claims.remove(&claim.generation);
                }
            }
        }
        self.blobs.remove_dropped(&dropped).await;
        if applied.results_written > 0 {
            self.results.notify_waiters();
        }
        Ok(applied)
    }

    pub(crate) async fn open_payload(
        &self,
        envelope: &InternalRequest,
    ) -> Result<Option<PayloadBody>, StoreError> {
        match envelope.payload.storage {
            PayloadStorage::Inline => {
                let client = self.pool.get().await?;
                let row = client
                    .query_opt(
                        "SELECT payload FROM lda_requests WHERE id = $1 AND request_token = $2",
                        &[&envelope.request.id, &envelope.routing.request_token],
                    )
                    .await?;
                Ok(row
                    .and_then(|r| r.get::<_, Option<Vec<u8>>>(0))
                    .map(|bytes| PayloadBody::Inline(Bytes::from(bytes))))
            }
            PayloadStorage::Blob => {
                let Some(key) = BlobKey::request(&envelope.routing.request_token) else {
                    return Ok(None);
                };
                Ok(self.blobs.open(&key).await?.map(PayloadBody::Blob))
            }
        }
    }

    pub(crate) async fn cancel(&self, ids: Vec<String>, now_ms: i64) -> Result<usize, StoreError> {
        let ids: Vec<String> = ids.into_iter().filter(|id| !id.is_empty()).collect();
        if ids.is_empty() {
            return Ok(0);
        }
        let client = self.pool.get().await?;
        let row = client
            .query_one(
                "WITH live AS (
                    SELECT max(seq) AS seq FROM lda_requests
                    WHERE id = ANY($1) GROUP BY id
                 ), marked AS (
                    UPDATE lda_requests r SET cancelled = true
                    FROM live
                    WHERE r.seq = live.seq AND r.deadline > $2::bigint / 1000
                    RETURNING r.id
                 )
                 SELECT count(DISTINCT id) FROM marked",
                &[&ids, &now_ms],
            )
            .await?;
        let marked: i64 = row.get(0);
        Ok(usize::try_from(marked).unwrap_or(0))
    }

    pub(crate) async fn backlog(
        &self,
        queue: String,
        now_ms: i64,
        bounds_s: Vec<i64>,
    ) -> Result<Backlog, StoreError> {
        let now_s = now_ms.div_euclid(1000);
        let client = self.pool.get().await?;
        let row = client
            .query_one(
                "WITH base AS MATERIALIZED (
                    SELECT deadline FROM lda_requests
                    WHERE queue = $1 AND dispatch_epoch = 0 AND not_before_ms <= $2
                 )
                 SELECT (SELECT count(*) FROM base),
                        coalesce((SELECT array_agg(c ORDER BY i) FROM (
                            SELECT i, (SELECT count(*) FROM base WHERE deadline <= $3 + b) AS c
                            FROM unnest($4::bigint[]) WITH ORDINALITY AS t(b, i)
                        ) x), '{}')",
                &[&queue, &now_ms, &now_s, &bounds_s],
            )
            .await?;
        let depth: i64 = row.get(0);
        let cumulative: Vec<i64> = row.get(1);
        Ok(Backlog {
            depth: u64::try_from(depth).unwrap_or(0),
            cumulative: cumulative
                .into_iter()
                .map(|c| u64::try_from(c).unwrap_or(0))
                .collect(),
        })
    }
}

/// Of `keys` (id, token), those marked cancelled. A key without a row is not
/// cancelled.
pub(crate) async fn cancelled_keys(
    pool: &deadpool_postgres::Pool,
    keys: &[(String, String)],
) -> Result<HashSet<(String, String)>, StoreError> {
    let ids: Vec<&str> = keys.iter().map(|(i, _)| i.as_str()).collect();
    let tokens: Vec<&str> = keys.iter().map(|(_, t)| t.as_str()).collect();
    let client = pool.get().await?;
    let rows = client
        .query(
            "SELECT r.id, r.request_token FROM lda_requests r
             JOIN unnest($1::text[], $2::text[]) AS k(id, request_token)
               ON r.id = k.id AND r.request_token = k.request_token
             WHERE r.cancelled",
            &[&ids, &tokens],
        )
        .await?;
    Ok(rows.iter().map(|r| (r.get(0), r.get(1))).collect())
}
