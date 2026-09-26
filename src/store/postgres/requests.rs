use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use deadpool_postgres::Transaction;

use crate::api::payload::PayloadStorage;
use crate::api::request::InternalRequest;
use crate::api::result::ResultMessage;
use crate::store::blob::key::BlobKey;
use crate::store::error::StoreError;
use crate::store::postgres::cached::Cached;
use crate::store::postgres::partitions::{Tracked, partition_of};
use crate::store::postgres::{Inner, RESULTS_CHANNEL};
use crate::store::queue::{
    Admission, Admitted, Applied, Backlog, ClaimRef, NewRequest, Outcome, PayloadBody, Peeked,
    PendingKey, RequestStatus,
};
use crate::store::staging::StagedPayload;

/// Largest number of rows one statement inserts.
const INSERT_CHUNK: usize = 1000;

/// Deletes the claims `(id, request_token, attempt)` of `$2..$4` that process
/// `$1` still holds, with their inline bodies. Each key is one probe of the
/// unique index, checked against the claim after the probe so no other index
/// can serve it, and the delete goes straight to the rows found.
pub(crate) const FINISH: &str = "
    WITH gone AS (
    DELETE FROM lda_requests r
    WHERE r.ctid = ANY(ARRAY(
        SELECT x.tid
        FROM unnest($2::text[], $3::text[], $4::bigint[]) AS k(id, request_token, attempt)
        CROSS JOIN LATERAL (
            SELECT h.ctid AS tid, h.claimed_by, h.dispatch_attempt FROM lda_requests h
            WHERE h.id = k.id AND h.request_token = k.request_token
            LIMIT 1
        ) x
        WHERE x.claimed_by = $1 AND x.dispatch_attempt = k.attempt))
    RETURNING r.seq, r.id, r.request_token, r.dispatch_attempt, r.inline_payload
    ), bodies AS (
        DELETE FROM lda_payloads
        WHERE seq = ANY(ARRAY(SELECT seq FROM gone WHERE inline_payload))
    )
    SELECT id, request_token, dispatch_attempt, NOT inline_payload FROM gone";

/// Returns the claims of `$2..$4` that process `$1` holds to their queue, due
/// at `$5` with envelopes `$6`.
pub(crate) const RETRY: &str = "
    WITH m AS MATERIALIZED (
        SELECT x.tid, k.due, k.envelope
        FROM unnest($2::text[], $3::text[], $4::bigint[], $5::bigint[], $6::text[])
             AS k(id, request_token, attempt, due, envelope)
        CROSS JOIN LATERAL (
            SELECT h.ctid AS tid, h.claimed_by, h.dispatch_attempt FROM lda_requests h
            WHERE h.id = k.id AND h.request_token = k.request_token
            LIMIT 1
        ) x
        WHERE x.claimed_by = $1 AND x.dispatch_attempt = k.attempt
    )
    UPDATE lda_requests r
    SET claimed_by = 0, not_before_ms = m.due, envelope = m.envelope
    FROM m
    WHERE r.ctid = ANY(ARRAY(SELECT tid FROM m)) AND r.ctid = m.tid";

/// Returns the claims of `$2..$4` that process `$1` holds to their queue
/// unchanged.
pub(crate) const RELEASE: &str = "
    UPDATE lda_requests r SET claimed_by = 0
    WHERE r.ctid = ANY(ARRAY(
        SELECT x.tid
        FROM unnest($2::text[], $3::text[], $4::bigint[]) AS k(id, request_token, attempt)
        CROSS JOIN LATERAL (
            SELECT h.ctid AS tid, h.claimed_by, h.dispatch_attempt FROM lda_requests h
            WHERE h.id = k.id AND h.request_token = k.request_token
            LIMIT 1
        ) x
        WHERE x.claimed_by = $1 AND x.dispatch_attempt = k.attempt))";

/// Claims for process `$4` the pending requests `$1` of queue `$2` in
/// partitions `$3` holds, with their inline bodies.
pub(crate) const CLAIM: &str = concat!(
    "WITH claimed AS (
     UPDATE lda_requests r
     SET claimed_by = $4,
         dispatch_attempt = nextval('lda_dispatch_attempts')
     FROM lda_partitions p
     WHERE r.seq = ANY($1) AND r.queue = $2 AND r.claimed_by = 0
       AND p.queue = r.queue AND p.partition_id = r.partition_id
       AND p.owner = $3 AND p.lease_expires_ms > ",
    db_now_ms!(),
    "
     RETURNING r.seq, r.id, r.request_token, r.dispatch_attempt
     )
     SELECT c.seq, c.id, c.request_token, c.dispatch_attempt, b.data
     FROM claimed c
     LEFT JOIN (SELECT seq, data FROM lda_payloads WHERE seq = ANY($1)) b ON b.seq = c.seq"
);

/// Queues requests: `$1..$6` are their columns and `$7` their inline bodies,
/// NULL for a body in a blob. Each request takes its `seq` from the identity
/// sequence in array order, which its body is keyed by.
pub(crate) const SUBMIT: &str = "
    WITH t AS MATERIALIZED (
        SELECT nextval('lda_requests_seq_seq') AS seq, r.*
        FROM unnest($1::text[], $2::text[], $3::text[], $4::int[], $5::bigint[],
                    $6::text[], $7::bytea[])
             AS r(id, request_token, queue, partition_id, deadline, envelope, payload)
    ), bodies AS (
        INSERT INTO lda_payloads (seq, data)
        SELECT seq, payload FROM t WHERE payload IS NOT NULL
    )
    INSERT INTO lda_requests
        (seq, id, request_token, queue, partition_id, deadline, envelope, inline_payload)
    OVERRIDING SYSTEM VALUE
    SELECT seq, id, request_token, queue, partition_id, deadline, envelope,
           payload IS NOT NULL
    FROM t";

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

/// Postgres caps a notification payload below 8000 bytes.
const MAX_NOTIFICATION: usize = 7900;

/// The routes written, one per line, or empty (any route) when they do not
/// fit a notification or one could be mistaken for two.
fn notification<'a>(routes: impl Iterator<Item = &'a str>) -> String {
    let routes: std::collections::BTreeSet<&str> = routes.collect();
    let payload = routes.iter().copied().collect::<Vec<_>>().join("\n");
    let ambiguous = routes
        .iter()
        .any(|r| r.is_empty() || r.contains(['\n', '\r']));
    if ambiguous || payload.len() > MAX_NOTIFICATION {
        String::new()
    } else {
        payload
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
    txn.execute_cached(
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
        txn.execute_cached(
            "INSERT INTO lda_blobs (key, content_type, expires_at_ms)
             SELECT * FROM unnest($1::text[], $2::text[], $3::bigint[])
             ON CONFLICT (key) DO UPDATE
             SET content_type = EXCLUDED.content_type, expires_at_ms = EXCLUDED.expires_at_ms",
            &[&keys, &types, &expiries],
        )
        .await?;
    }
    let payload = notification(results.iter().map(|r| r.route.as_str()));
    txn.execute_cached("SELECT pg_notify($1, $2)", &[&RESULTS_CHANNEL, &payload])
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
    txn.execute_cached("DELETE FROM lda_blobs WHERE key = ANY($1)", &[&names])
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
            txn.execute_cached(
                SUBMIT,
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
                txn.execute_cached(
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
            .query_cached(
                "SELECT seq, deadline, envelope, cancelled FROM lda_peek($1, $2, $3, $4)",
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
            .query_one_cached(
                "SELECT EXISTS (SELECT 1 FROM lda_requests
                                WHERE queue = $1 AND claimed_by = 0 AND not_before_ms <= $2)",
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
            self.results.all();
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
                .query_cached(CLAIM, &[&claims, &queue, &self.owner, &self.process()])
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
                    attempt: row.get(3),
                };
                if let Some(slot) = out.get_mut(i) {
                    *slot = Admitted::Claimed {
                        claim: ClaimRef {
                            generation: crate::api::request::generation_key(&t.id, &t.token),
                            claim_id: u64::try_from(t.attempt).unwrap_or(0),
                        },
                        payload: row.get::<_, Option<Vec<u8>>>(4).map(Bytes::from),
                    };
                }
                tracked.push(t);
            }
        }
        let mut dropped = Vec::new();
        if !removals.is_empty() {
            let seqs: Vec<i64> = removals.iter().map(|(_, s)| *s).collect();
            let rows = txn
                .query_cached(
                    "WITH gone AS (
                        DELETE FROM lda_requests r USING lda_partitions p
                        WHERE r.seq = ANY($1) AND r.queue = $2 AND r.claimed_by = 0
                          AND p.queue = r.queue AND p.partition_id = r.partition_id
                          AND p.owner = $3
                        RETURNING r.seq, r.request_token, r.inline_payload
                     ), bodies AS (
                        DELETE FROM lda_payloads
                        WHERE seq = ANY(ARRAY(SELECT seq FROM gone WHERE inline_payload))
                     )
                     SELECT seq, request_token, NOT inline_payload FROM gone",
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
        outcomes: Arc<[Outcome]>,
        now_ms: i64,
    ) -> Result<Applied, StoreError> {
        let mut finishes = Vec::new();
        let mut retries = Vec::new();
        let mut releases = Vec::new();
        let mut fenced_blobs = Vec::new();
        let mut applied = Applied::default();
        for outcome in outcomes.iter() {
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

        let process = self.process();
        let mut client = self.pool.get().await?;
        let txn = client.transaction().await?;
        let mut dropped = Vec::new();
        if !finishes.is_empty() {
            let ids: Vec<&str> = finishes.iter().map(|f| f.0).collect();
            let tokens: Vec<&str> = finishes.iter().map(|f| f.1).collect();
            let attempts: Vec<i64> = finishes.iter().map(|f| f.2).collect();
            let rows = txn
                .query_cached(FINISH, &[&process, &ids, &tokens, &attempts])
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
            applied.result_routes = results.iter().map(|r| r.route.clone()).collect();
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
                .execute_cached(
                    RETRY,
                    &[&process, &ids, &tokens, &attempts, &due, &envelopes],
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
                .execute_cached(RELEASE, &[&process, &ids, &tokens, &attempts])
                .await?;
            applied.fenced += releases
                .len()
                .saturating_sub(usize::try_from(n).unwrap_or(0));
        }
        if !fenced_blobs.is_empty() {
            let names: Vec<String> = fenced_blobs.iter().map(ToString::to_string).collect();
            let registered: HashSet<String> = txn
                .query_cached("SELECT key FROM lda_blobs WHERE key = ANY($1)", &[&names])
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
            for outcome in outcomes.iter() {
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
        self.results
            .written(applied.result_routes.iter().map(String::as_str));
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
                    .query_opt_cached(
                        "SELECT b.data FROM lda_requests r
                         JOIN lda_payloads b ON b.seq = r.seq
                         WHERE r.id = $1 AND r.request_token = $2",
                        &[&envelope.request.id, &envelope.routing.request_token],
                    )
                    .await?;
                Ok(row.map(|r| PayloadBody::Inline(Bytes::from(r.get::<_, Vec<u8>>(0)))))
            }
            PayloadStorage::Blob => {
                let Some(key) = BlobKey::request(&envelope.routing.request_token) else {
                    return Ok(None);
                };
                Ok(self.blobs.open(&key).await?.map(PayloadBody::Blob))
            }
        }
    }

    pub(crate) async fn status(
        &self,
        id: String,
        token: Option<String>,
    ) -> Result<RequestStatus, StoreError> {
        let client = self.pool.get().await?;
        let row = client
            .query_opt(
                "SELECT claimed_by FROM lda_requests
                 WHERE id = $1 AND ($2::text IS NULL OR request_token = $2)
                 ORDER BY claimed_by DESC LIMIT 1",
                &[&id, &token],
            )
            .await?;
        Ok(match row.map(|r| r.get::<_, i64>(0)) {
            None => RequestStatus::Done,
            Some(0) => RequestStatus::Queued,
            Some(_) => RequestStatus::InProgress,
        })
    }

    pub(crate) async fn cancel(&self, ids: Vec<String>, now_ms: i64) -> Result<usize, StoreError> {
        let ids: Vec<String> = ids.into_iter().filter(|id| !id.is_empty()).collect();
        if ids.is_empty() {
            return Ok(0);
        }
        let client = self.pool.get().await?;
        let row = client
            .query_one_cached(
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
            .query_one_cached(
                "WITH base AS MATERIALIZED (
                    SELECT deadline FROM lda_requests
                    WHERE queue = $1 AND claimed_by = 0 AND not_before_ms <= $2
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
        .query_cached(
            "SELECT r.id, r.request_token FROM lda_requests r
             JOIN unnest($1::text[], $2::text[]) AS k(id, request_token)
               ON r.id = k.id AND r.request_token = k.request_token
             WHERE r.cancelled",
            &[&ids, &tokens],
        )
        .await?;
    Ok(rows.iter().map(|r| (r.get(0), r.get(1))).collect())
}

#[cfg(test)]
mod tests {
    use deadpool_postgres::Object;
    use tokio_postgres::types::{FromSql, ToSql, Type};

    use crate::store::postgres::connect::Database;
    use crate::store::postgres::partitions::RETURN_LAPSED;
    use crate::store::postgres::requests::{CLAIM, FINISH, RELEASE, RETRY};
    use crate::store::postgres::schema::migrate;
    use crate::store::postgres::test_support::schema_url;

    const ROWS: i64 = 100_000;
    /// The writer's largest batch.
    const KEYS: i64 = 1024;
    const PARTITIONS: i64 = 64;
    /// Most rows a batch may read and discard: each key may check every
    /// partition, but nothing may scale with the queue.
    const DISCARD_LIMIT: f64 = (KEYS * PARTITIONS) as f64;

    /// A `json` column, as text.
    struct Json(String);

    impl<'a> FromSql<'a> for Json {
        fn from_sql(
            _: &Type,
            raw: &'a [u8],
        ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
            Ok(Self(std::str::from_utf8(raw)?.to_owned()))
        }

        fn accepts(ty: &Type) -> bool {
            *ty == Type::JSON
        }
    }

    /// What finding a statement's rows cost: shared buffers read (not the
    /// writes of an insert, update or delete) and rows read then discarded
    /// by a filter, join filter or index recheck.
    #[derive(Debug, Default)]
    struct Profile {
        blocks: i64,
        discarded: f64,
    }

    fn blocks(node: &serde_json::Value) -> i64 {
        node["Shared Hit Blocks"].as_i64().unwrap_or(0)
            + node["Shared Read Blocks"].as_i64().unwrap_or(0)
    }

    /// Adds up what each node read itself, less its children, leaving out
    /// the writes of every insert, update or delete. A CTE's reads are
    /// counted under its own plan, not under the scans of it. A
    /// data-modifying CTE nothing reads runs after the statement, outside
    /// its parent's counts, so a parent never counts below zero.
    fn walk(node: &serde_json::Value, profile: &mut Profile) {
        let loops = node["Actual Loops"].as_f64().unwrap_or(1.0);
        for key in [
            "Rows Removed by Filter",
            "Rows Removed by Join Filter",
            "Rows Removed by Index Recheck",
        ] {
            profile.discarded += node[key].as_f64().unwrap_or(0.0) * loops;
        }
        let children = node["Plans"].as_array().cloned().unwrap_or_default();
        if !matches!(node["Node Type"].as_str(), Some("ModifyTable" | "CTE Scan")) {
            let own = blocks(node) - children.iter().map(blocks).sum::<i64>();
            profile.blocks += own.max(0);
        }
        for child in &children {
            walk(child, profile);
        }
    }

    /// Runs `statement` once under a generic plan and rolls it back.
    async fn profile(
        client: &mut Object,
        statement: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Profile {
        let txn = client.transaction().await.unwrap();
        txn.batch_execute("SET LOCAL plan_cache_mode = force_generic_plan")
            .await
            .unwrap();
        let row = txn
            .query_one(
                &format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {statement}"),
                params,
            )
            .await
            .unwrap();
        txn.rollback().await.unwrap();
        let plan: serde_json::Value = serde_json::from_str(&row.get::<_, Json>(0).0).unwrap();
        let mut profile = Profile::default();
        walk(&plan[0]["Plan"], &mut profile);
        profile
    }

    /// Queue tables go from empty to full faster than autovacuum analyzes
    /// them. Each hot statement must read rows in proportion to its batch
    /// whatever the statistics say: no scan of the queue, no join that
    /// compares every key with every row. The queue is larger than
    /// [`DISCARD_LIMIT`], so either shows up as discarded rows. Autovacuum
    /// is off so that the statistics are the stale ones set up here.
    #[tokio::test]
    async fn hot_statements_stay_proportional_to_their_batch_under_stale_statistics() {
        let Some(url) = schema_url().await else {
            return;
        };
        let db = Database::connect(&url, 2, None).await.unwrap();
        migrate(&db.pool).await.unwrap();
        let mut client = db.pool.get().await.unwrap();
        client
            .batch_execute(&format!(
                "ALTER TABLE lda_requests SET (autovacuum_enabled = false);
                 ALTER TABLE lda_payloads SET (autovacuum_enabled = false);
                 ALTER TABLE lda_partitions SET (autovacuum_enabled = false);
                 INSERT INTO lda_partitions (queue, partition_id, owner, lease_expires_ms)
                     SELECT 'q', g, 'me', 9223372036854775807 FROM generate_series(0, {PARTITIONS} - 1) g;
                 INSERT INTO lda_processes (expires_ms) VALUES (9223372036854775807), (0);
                 INSERT INTO lda_requests
                     (id, request_token, queue, partition_id, deadline, envelope, inline_payload)
                     SELECT 'early' || g, 'early' || g, 'q', g, g, '{{}}', true
                     FROM generate_series(1, 7) g;
                 INSERT INTO lda_payloads SELECT seq, '\\x00' FROM lda_requests;
                 ANALYZE lda_requests;
                 ANALYZE lda_payloads;
                 INSERT INTO lda_requests
                     (id, request_token, queue, partition_id, deadline, envelope, inline_payload)
                     SELECT 'r' || g, 't' || g, 'q', g % {PARTITIONS}, g, '{{}}', true
                     FROM generate_series(1, {ROWS}) g;
                 INSERT INTO lda_payloads
                     SELECT seq, convert_to(repeat('x', 512), 'UTF8') FROM lda_requests
                     WHERE id LIKE 'r%';"
            ))
            .await
            .unwrap();

        let peek = profile(
            &mut client,
            "SELECT * FROM lda_peek('q', 'me', 9223372036854775807, 10)",
            &[],
        )
        .await;
        assert!(peek.blocks < 2000, "peek of 10: {peek:?}");
        assert!(peek.discarded < 100.0, "peek of 10: {peek:?}");

        let seqs: Vec<i64> = client
            .query(
                "SELECT seq FROM lda_requests WHERE id LIKE 'r%' ORDER BY seq LIMIT $1",
                &[&KEYS],
            )
            .await
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect();
        let queue = "q";
        let owner = "me";
        let process = 1_i64;
        let claim = profile(&mut client, CLAIM, &[&seqs, &queue, &owner, &process]).await;
        assert!(
            claim.discarded < DISCARD_LIMIT,
            "claim of {KEYS}: {claim:?}"
        );
        assert!(claim.blocks < 20 * KEYS, "claim of {KEYS}: {claim:?}");

        client
            .batch_execute(
                "ANALYZE lda_requests;
                 UPDATE lda_requests SET claimed_by = 1, dispatch_attempt = seq;",
            )
            .await
            .unwrap();
        let keys = 1000..1000 + KEYS;
        let ids: Vec<String> = keys.clone().map(|g| format!("r{g}")).collect();
        let tokens: Vec<String> = keys.map(|g| format!("t{g}")).collect();
        let attempts: Vec<i64> = client
            .query(
                "SELECT dispatch_attempt FROM lda_requests r
                 JOIN unnest($1::text[]) WITH ORDINALITY AS k(id, n) ON k.id = r.id
                 ORDER BY k.n",
                &[&ids],
            )
            .await
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect();
        assert_eq!(attempts.len(), ids.len());
        let due: Vec<i64> = vec![0; ids.len()];
        let envelopes: Vec<String> = vec!["{}".into(); ids.len()];
        for (name, statement, params) in [
            (
                "finish",
                FINISH,
                vec![&process as &(dyn ToSql + Sync), &ids, &tokens, &attempts],
            ),
            (
                "retry",
                RETRY,
                vec![&process, &ids, &tokens, &attempts, &due, &envelopes],
            ),
            ("release", RELEASE, vec![&process, &ids, &tokens, &attempts]),
        ] {
            let p = profile(&mut client, statement, &params).await;
            assert!(
                p.discarded < DISCARD_LIMIT,
                "{name} of {KEYS} among {ROWS} in flight: {p:?}"
            );
            assert!(
                p.blocks < 20 * KEYS,
                "{name} of {KEYS} among {ROWS} in flight: {p:?}"
            );
        }

        let lapsed = profile(&mut client, RETURN_LAPSED, &[]).await;
        assert!(
            lapsed.blocks < 100 && lapsed.discarded < 100.0,
            "a lapsed process without claims among {ROWS} in flight: {lapsed:?}"
        );
    }
}
