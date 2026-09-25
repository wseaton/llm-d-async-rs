use std::sync::atomic::Ordering;

use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, Table, WriteTransaction};

use crate::api::payload::PayloadStorage;
use crate::api::request::InternalRequest;
use crate::api::result::ResultMessage;
use crate::store::Store;
use crate::store::blob::BlobKey;
use crate::store::error::StoreError;
use crate::store::staging::StagedPayload;
use crate::store::tables::{
    ACTIVE, BLOBS, BlobRecord, CANCELLED, CLAIMED, ClaimedRecord, META, META_SEQ, PAYLOADS,
    PENDING, RESULTS, RETRY, ResultRecord, RetryRecord, TokenRecord,
};

/// Cancellation markers outlive their request so a late cancel is idempotent.
const CANCEL_MARKER_TTL_MS: i64 = 7 * 24 * 3600 * 1000;

pub struct NewRequest {
    /// Must carry `request_token`, `request_queue_name`, and the `payload`
    /// info of `payload`.
    pub envelope: InternalRequest,
    pub payload: StagedPayload,
}

/// Position of a request within its queue: earliest deadline first.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PendingKey {
    pub deadline: i64,
    pub seq: u64,
}

pub struct Peeked {
    pub key: PendingKey,
    /// `Err` holds the decode error of a row that can never be dispatched.
    pub envelope: Result<InternalRequest, String>,
}

/// Proof that this process holds a request. Outcomes for a claim that is no
/// longer current are discarded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimRef {
    pub generation: String,
    pub claim_id: u64,
}

/// What a queue consumer decided for one peeked request.
pub enum Admission {
    /// Take the request for dispatch.
    Claim { key: PendingKey, generation: String },
    /// Finish the request without dispatching it.
    Finish {
        key: PendingKey,
        envelope: Box<InternalRequest>,
        result: Box<ResultMessage>,
    },
    /// Remove an undecodable row.
    Discard { key: PendingKey },
}

#[derive(Debug, PartialEq, Eq)]
pub enum Admitted {
    Claimed(ClaimRef),
    Finished,
    /// The row left the queue before this admission ran.
    Gone,
}

/// The end of one claim.
#[derive(Clone)]
pub enum Outcome {
    Finish {
        claim: ClaimRef,
        envelope: InternalRequest,
        result: ResultMessage,
    },
    /// Park the request until `due_ms`, then return it to its queue.
    Retry {
        claim: ClaimRef,
        envelope: InternalRequest,
        due_ms: i64,
    },
    /// Return the request to its queue unchanged.
    Release { claim: ClaimRef },
}

impl Outcome {
    fn claim(&self) -> &ClaimRef {
        match self {
            Self::Finish { claim, .. } | Self::Retry { claim, .. } | Self::Release { claim } => {
                claim
            }
        }
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Applied {
    pub results_written: usize,
    /// Outcomes dropped because their claim was no longer current.
    pub fenced: usize,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Backlog {
    pub depth: u64,
    /// Requests with deadline <= now + bound, one per bound.
    pub cumulative: Vec<u64>,
}

type Str = &'static str;

struct Tables<'t> {
    pending: Table<'t, (Str, i64, u64), Str>,
    payloads: Table<'t, Str, &'static [u8]>,
    blobs: Table<'t, Str, Str>,
    claimed: Table<'t, Str, Str>,
    retry: Table<'t, (i64, u64), Str>,
    active: Table<'t, Str, Str>,
    cancelled: Table<'t, Str, Str>,
    results: Table<'t, (Str, u64), Str>,
    meta: Table<'t, Str, u64>,
    /// Blob files to delete once the transaction commits.
    dropped: Vec<BlobKey>,
}

impl<'t> Tables<'t> {
    fn open(txn: &'t WriteTransaction) -> Result<Self, StoreError> {
        Ok(Self {
            pending: txn.open_table(PENDING)?,
            payloads: txn.open_table(PAYLOADS)?,
            blobs: txn.open_table(BLOBS)?,
            claimed: txn.open_table(CLAIMED)?,
            retry: txn.open_table(RETRY)?,
            active: txn.open_table(ACTIVE)?,
            cancelled: txn.open_table(CANCELLED)?,
            results: txn.open_table(RESULTS)?,
            meta: txn.open_table(META)?,
            dropped: Vec::new(),
        })
    }

    fn next_seq(&mut self) -> Result<u64, StoreError> {
        let seq = self.meta.get(META_SEQ)?.map(|v| v.value()).unwrap_or(1);
        self.meta.insert(META_SEQ, seq + 1)?;
        Ok(seq)
    }

    fn drop_payload(&mut self, envelope: &InternalRequest) -> Result<(), StoreError> {
        match envelope.payload.storage {
            PayloadStorage::Inline => {
                self.payloads.remove(envelope.generation_key().as_str())?;
            }
            PayloadStorage::Blob => {
                if let Some(key) = BlobKey::request(&envelope.routing.request_token) {
                    self.blobs.remove(key.as_str())?;
                    self.dropped.push(key);
                }
            }
        }
        Ok(())
    }

    /// Records the terminal result of a generation and drops its state.
    fn finish(
        &mut self,
        envelope: &InternalRequest,
        result: &ResultMessage,
        now_ms: i64,
    ) -> Result<(), StoreError> {
        self.drop_payload(envelope)?;
        let id = envelope.request.id.as_str();
        let token = envelope.routing.request_token.as_str();
        clear_token(&mut self.active, id, token)?;
        clear_token(&mut self.cancelled, id, token)?;
        let ttl_ms = i64::try_from(envelope.routing.result_ttl_seconds.saturating_mul(1000))
            .unwrap_or(i64::MAX);
        let expires_at_ms = (ttl_ms > 0).then(|| now_ms.saturating_add(ttl_ms));
        let blob = BlobKey::from_ref(&result.payload_ref);
        if let Some(key) = &blob {
            // The blob lives as long as its result. A destructive pop
            // starts its retention clock; an ack deletes it.
            let record = BlobRecord {
                content_type: result.content_type.clone(),
                expires_at_ms,
            };
            self.blobs
                .insert(key.as_str(), serde_json::to_string(&record)?.as_str())?;
        }
        let record = ResultRecord {
            expires_at_ms,
            blob: blob.map(|k| k.as_str().to_owned()),
            body: serde_json::to_string(result)?,
        };
        let seq = self.next_seq()?;
        self.results.insert(
            (envelope.routing.result_queue_name.as_str(), seq),
            serde_json::to_string(&record)?.as_str(),
        )?;
        Ok(())
    }

    /// Drops `key` unless a result references it. A body written by an
    /// attempt that then lost a race (fenced, drained, timed out) is garbage.
    fn discard_unreferenced(&mut self, key: Option<BlobKey>) -> Result<(), StoreError> {
        if let Some(key) = key
            && self.blobs.get(key.as_str())?.is_none()
        {
            self.dropped.push(key);
        }
        Ok(())
    }
}

/// Removes `id`'s marker if it belongs to `token`. An undecodable marker is
/// removed too; it could never match anything.
fn clear_token(table: &mut Table<'_, Str, Str>, id: &str, token: &str) -> Result<(), StoreError> {
    let current = table
        .get(id)?
        .map(|v| serde_json::from_str::<TokenRecord>(v.value()));
    match current {
        Some(Ok(record)) if record.token != token => {}
        Some(_) => {
            table.remove(id)?;
        }
        None => {}
    }
    Ok(())
}

fn queue_range(queue: &str) -> std::ops::RangeInclusive<(&str, i64, u64)> {
    (queue, i64::MIN, 0)..=(queue, i64::MAX, u64::MAX)
}

impl Store {
    /// Enqueues requests whose payloads are already staged. On failure,
    /// staged blob files are deleted.
    pub async fn submit(&self, requests: Vec<NewRequest>) -> Result<(), StoreError> {
        let blobs = self.blobs.clone();
        self.run(move |db| {
            let write = || -> Result<(), StoreError> {
                let txn = db.begin_write()?;
                {
                    let mut t = Tables::open(&txn)?;
                    for r in &requests {
                        let id = r.envelope.request.id.as_str();
                        let deadline = r.envelope.request.deadline;
                        t.cancelled.remove(id)?;
                        let active = TokenRecord {
                            token: r.envelope.routing.request_token.clone(),
                            expires_at_ms: deadline.saturating_mul(1000),
                        };
                        t.active
                            .insert(id, serde_json::to_string(&active)?.as_str())?;
                        match &r.payload {
                            StagedPayload::Inline(bytes) => {
                                t.payloads.insert(
                                    r.envelope.generation_key().as_str(),
                                    bytes.as_slice(),
                                )?;
                            }
                            StagedPayload::Blob { key, .. } => {
                                let record = BlobRecord {
                                    content_type: r.envelope.payload.content_type.clone(),
                                    expires_at_ms: None,
                                };
                                t.blobs.insert(
                                    key.as_str(),
                                    serde_json::to_string(&record)?.as_str(),
                                )?;
                            }
                        }
                        let seq = t.next_seq()?;
                        t.pending.insert(
                            (
                                r.envelope.routing.request_queue_name.as_str(),
                                deadline,
                                seq,
                            ),
                            serde_json::to_string(&r.envelope)?.as_str(),
                        )?;
                    }
                }
                txn.commit()?;
                Ok(())
            };
            let written = write();
            if written.is_err() {
                let staged: Vec<BlobKey> = requests
                    .iter()
                    .filter_map(|r| match &r.payload {
                        StagedPayload::Blob { key, .. } => Some(key.clone()),
                        StagedPayload::Inline(_) => None,
                    })
                    .collect();
                blobs.remove_dropped(&staged);
            }
            written
        })
        .await
    }

    /// Returns up to `limit` requests at the head of `queue` without claiming
    /// them. Never reads payloads.
    pub async fn peek(&self, queue: String, limit: usize) -> Result<Vec<Peeked>, StoreError> {
        self.run(move |db| {
            let txn = db.begin_read()?;
            let pending = txn.open_table(PENDING)?;
            let mut out = Vec::new();
            for entry in pending.range(queue_range(&queue))?.take(limit) {
                let (key, value) = entry?;
                let (_, deadline, seq) = key.value();
                out.push(Peeked {
                    key: PendingKey { deadline, seq },
                    envelope: serde_json::from_str(value.value()).map_err(|e| e.to_string()),
                });
            }
            Ok(out)
        })
        .await
    }

    pub async fn has_pending(&self, queue: String) -> Result<bool, StoreError> {
        self.run(move |db| {
            let txn = db.begin_read()?;
            let pending = txn.open_table(PENDING)?;
            Ok(pending.range(queue_range(&queue))?.next().is_some())
        })
        .await
    }

    pub async fn is_cancelled(
        &self,
        id: String,
        token: String,
        now_ms: i64,
    ) -> Result<bool, StoreError> {
        self.run(move |db| {
            let txn = db.begin_read()?;
            let cancelled = txn.open_table(CANCELLED)?;
            let Some(value) = cancelled.get(id.as_str())? else {
                return Ok(false);
            };
            let record: TokenRecord = serde_json::from_str(value.value())?;
            Ok(record.token == token && record.expires_at_ms > now_ms)
        })
        .await
    }

    /// Marks the live generation of each ID cancelled. Unknown, finished and
    /// expired IDs are a no-op. Returns how many were marked.
    pub async fn cancel(&self, ids: Vec<String>, now_ms: i64) -> Result<usize, StoreError> {
        self.run(move |db| {
            let txn = db.begin_write()?;
            let mut marked = 0;
            {
                let active = txn.open_table(ACTIVE)?;
                let mut cancelled = txn.open_table(CANCELLED)?;
                for id in ids.iter().filter(|id| !id.is_empty()) {
                    let Some(value) = active.get(id.as_str())? else {
                        continue;
                    };
                    let live: TokenRecord = serde_json::from_str(value.value())?;
                    if live.expires_at_ms <= now_ms {
                        continue;
                    }
                    let marker = TokenRecord {
                        token: live.token,
                        expires_at_ms: now_ms.saturating_add(CANCEL_MARKER_TTL_MS),
                    };
                    cancelled.insert(id.as_str(), serde_json::to_string(&marker)?.as_str())?;
                    marked += 1;
                }
            }
            txn.commit()?;
            Ok(marked)
        })
        .await
    }

    /// Applies a consumer's decisions for rows of `queue` in one transaction.
    pub async fn admit(
        &self,
        queue: String,
        admissions: Vec<Admission>,
        now_ms: i64,
    ) -> Result<Vec<Admitted>, StoreError> {
        let claim_ids = std::sync::Arc::clone(&self.next_claim_id);
        let blobs = self.blobs.clone();
        self.run(move |db| {
            let txn = db.begin_write()?;
            let mut out = Vec::with_capacity(admissions.len());
            let dropped;
            {
                let mut t = Tables::open(&txn)?;
                for admission in &admissions {
                    let key = match admission {
                        Admission::Claim { key, .. }
                        | Admission::Finish { key, .. }
                        | Admission::Discard { key } => *key,
                    };
                    let removed = t
                        .pending
                        .remove((queue.as_str(), key.deadline, key.seq))?
                        .map(|v| v.value().to_owned());
                    let Some(envelope_json) = removed else {
                        out.push(Admitted::Gone);
                        continue;
                    };
                    match admission {
                        Admission::Claim { generation, .. } => {
                            let claim = ClaimRef {
                                generation: generation.clone(),
                                claim_id: claim_ids.fetch_add(1, Ordering::Relaxed),
                            };
                            let record = ClaimedRecord {
                                queue: queue.clone(),
                                deadline: key.deadline,
                                seq: key.seq,
                                claim_id: claim.claim_id,
                                envelope: envelope_json,
                            };
                            t.claimed.insert(
                                generation.as_str(),
                                serde_json::to_string(&record)?.as_str(),
                            )?;
                            out.push(Admitted::Claimed(claim));
                        }
                        Admission::Finish {
                            envelope, result, ..
                        } => {
                            t.finish(envelope, result, now_ms)?;
                            out.push(Admitted::Finished);
                        }
                        Admission::Discard { .. } => out.push(Admitted::Finished),
                    }
                }
                dropped = std::mem::take(&mut t.dropped);
            }
            txn.commit()?;
            blobs.remove_dropped(&dropped);
            Ok(out)
        })
        .await
    }

    /// Ends claims. Outcomes whose claim is not current are fenced.
    pub async fn apply_outcomes(
        &self,
        outcomes: Vec<Outcome>,
        now_ms: i64,
    ) -> Result<Applied, StoreError> {
        let blobs = self.blobs.clone();
        self.run(move |db| {
            let txn = db.begin_write()?;
            let mut applied = Applied::default();
            let dropped;
            {
                let mut t = Tables::open(&txn)?;
                for outcome in &outcomes {
                    let claim = outcome.claim();
                    let current = t
                        .claimed
                        .get(claim.generation.as_str())?
                        .map(|v| serde_json::from_str::<ClaimedRecord>(v.value()));
                    let current = match current {
                        Some(Err(e)) => {
                            tracing::error!(generation = %claim.generation, error = %e, "removing undecodable claim");
                            t.claimed.remove(claim.generation.as_str())?;
                            None
                        }
                        other => other.and_then(Result::ok),
                    };
                    let Some(record) = current.filter(|r| r.claim_id == claim.claim_id) else {
                        applied.fenced += 1;
                        if let Outcome::Finish { result, .. } = outcome {
                            t.discard_unreferenced(BlobKey::from_ref(&result.payload_ref))?;
                        }
                        continue;
                    };
                    t.claimed.remove(claim.generation.as_str())?;
                    match outcome {
                        Outcome::Finish {
                            envelope, result, ..
                        } => {
                            t.finish(envelope, result, now_ms)?;
                            applied.results_written += 1;
                        }
                        Outcome::Retry {
                            envelope, due_ms, ..
                        } => {
                            let retry = RetryRecord {
                                queue: record.queue,
                                deadline: record.deadline,
                                envelope: serde_json::to_string(envelope)?,
                            };
                            let seq = t.next_seq()?;
                            t.retry
                                .insert((*due_ms, seq), serde_json::to_string(&retry)?.as_str())?;
                        }
                        Outcome::Release { .. } => {
                            t.pending.insert(
                                (record.queue.as_str(), record.deadline, record.seq),
                                record.envelope.as_str(),
                            )?;
                        }
                    }
                    let token = claim.generation.split_once('\0').map(|(_, token)| token);
                    t.discard_unreferenced(token.and_then(BlobKey::result))?;
                }
                dropped = std::mem::take(&mut t.dropped);
            }
            txn.commit()?;
            blobs.remove_dropped(&dropped);
            Ok(applied)
        })
        .await
    }

    /// Moves up to `limit` retries due at `now_ms` back to their queues.
    /// Returns how many moved.
    pub async fn promote_due_retries(
        &self,
        now_ms: i64,
        limit: usize,
    ) -> Result<usize, StoreError> {
        self.run(move |db| {
            let txn = db.begin_write()?;
            let mut moved = 0;
            {
                let mut t = Tables::open(&txn)?;
                let mut due = Vec::new();
                for entry in t.retry.range(..=(now_ms, u64::MAX))?.take(limit) {
                    let (key, value) = entry?;
                    due.push((key.value(), value.value().to_owned()));
                }
                for (key, value) in due {
                    t.retry.remove(key)?;
                    let record: RetryRecord = match serde_json::from_str(&value) {
                        Ok(record) => record,
                        Err(e) => {
                            tracing::error!(error = %e, "dropping undecodable retry");
                            continue;
                        }
                    };
                    let seq = t.next_seq()?;
                    t.pending.insert(
                        (record.queue.as_str(), record.deadline, seq),
                        record.envelope.as_str(),
                    )?;
                    moved += 1;
                }
            }
            txn.commit()?;
            Ok(moved)
        })
        .await
    }

    /// Depth of `queue` plus cumulative counts of requests whose deadline is
    /// within each bound (seconds from `now_s`).
    pub async fn backlog(
        &self,
        queue: String,
        now_s: i64,
        bounds_s: Vec<i64>,
    ) -> Result<Backlog, StoreError> {
        self.run(move |db| {
            let txn = db.begin_read()?;
            let pending = txn.open_table(PENDING)?;
            let mut backlog = Backlog {
                depth: 0,
                cumulative: vec![0; bounds_s.len()],
            };
            for entry in pending.range(queue_range(&queue))? {
                let (key, _) = entry?;
                let (_, deadline, _) = key.value();
                backlog.depth += 1;
                for (count, bound) in backlog.cumulative.iter_mut().zip(&bounds_s) {
                    if deadline <= now_s.saturating_add(*bound) {
                        *count += 1;
                    }
                }
            }
            Ok(backlog)
        })
        .await
    }

    /// Deletes expired liveness and cancellation markers. Returns rows removed.
    pub async fn sweep_request_markers(&self, now_ms: i64) -> Result<u64, StoreError> {
        self.run(move |db| {
            let txn = db.begin_write()?;
            let mut removed = 0;
            {
                for def in [ACTIVE, CANCELLED] {
                    let mut table = txn.open_table(def)?;
                    let before = table.len()?;
                    table.retain(|_, v| {
                        serde_json::from_str::<TokenRecord>(v)
                            .map(|r| r.expires_at_ms > now_ms)
                            .unwrap_or(false)
                    })?;
                    removed += before - table.len()?;
                }
            }
            txn.commit()?;
            Ok(removed)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use crate::api::result::{ResultMessage, StoredBody};
    use crate::store::blob::BlobKey;
    use crate::store::requests::{Admission, Admitted, ClaimRef, NewRequest, Outcome, Peeked};
    use crate::store::staging::PayloadSink;
    use crate::store::test_support::{envelope, new_request, open, options};
    use crate::store::{PayloadBody, Store};

    const NOW_MS: i64 = 1_000_000;

    async fn claim_head(store: &Store, queue: &str) -> (Peeked, ClaimRef) {
        let mut peeked = store.peek(queue.into(), 1).await.unwrap();
        let head = peeked.remove(0);
        let env = head.envelope.clone().unwrap();
        let admitted = store
            .admit(
                queue.into(),
                vec![Admission::Claim {
                    key: head.key,
                    generation: env.generation_key(),
                }],
                NOW_MS,
            )
            .await
            .unwrap();
        let Some(Admitted::Claimed(claim)) = admitted.into_iter().next() else {
            panic!("not claimed")
        };
        (head, claim)
    }

    async fn payload_bytes(
        store: &Store,
        env: &crate::api::request::InternalRequest,
    ) -> Option<Vec<u8>> {
        match store.open_payload(env).await.unwrap()? {
            PayloadBody::Inline(bytes) => Some(bytes.to_vec()),
            PayloadBody::File { mut file, .. } => {
                use tokio::io::AsyncReadExt;
                let mut buf = Vec::new();
                file.read_to_end(&mut buf).await.unwrap();
                Some(buf)
            }
        }
    }

    /// Stages `body` through a sink with an 8-byte inline limit.
    async fn blob_request(store: &Store, id: &str, token: &str, body: &[u8]) -> NewRequest {
        let mut sink = PayloadSink::new(store.blobs().clone(), 8, 1 << 20);
        sink.push(body).await.unwrap();
        let payload = sink.finish(token).await.unwrap();
        let mut env = envelope(id, token, "q", 100);
        env.payload = payload.info("audio/wav");
        NewRequest {
            envelope: env,
            payload,
        }
    }

    #[tokio::test]
    async fn peek_orders_by_deadline_then_submission() {
        let (_dir, store) = open();
        store
            .submit(vec![
                new_request("late", "q", 300),
                new_request("early", "q", 100),
                new_request("other-queue", "r", 1),
                new_request("early2", "q", 100),
            ])
            .await
            .unwrap();
        let ids: Vec<String> = store
            .peek("q".into(), 10)
            .await
            .unwrap()
            .into_iter()
            .map(|p| p.envelope.unwrap().request.id)
            .collect();
        assert_eq!(ids, ["early", "early2", "late"]);
        assert_eq!(store.peek("q".into(), 2).await.unwrap().len(), 2);
        assert!(store.has_pending("r".into()).await.unwrap());
        assert!(!store.has_pending("nope".into()).await.unwrap());
    }

    #[tokio::test]
    async fn claim_removes_from_queue_and_payload_stays_readable() {
        let (_dir, store) = open();
        store
            .submit(vec![new_request("a", "q", 100)])
            .await
            .unwrap();
        let (head, _) = claim_head(&store, "q").await;
        assert!(!store.has_pending("q".into()).await.unwrap());
        let env = head.envelope.unwrap();
        assert_eq!(
            payload_bytes(&store, &env).await.unwrap(),
            br#"{"prompt":"a"}"#
        );
    }

    #[tokio::test]
    async fn admitting_a_row_twice_reports_gone() {
        let (_dir, store) = open();
        store
            .submit(vec![new_request("a", "q", 100)])
            .await
            .unwrap();
        let head = store.peek("q".into(), 1).await.unwrap().remove(0);
        let admit = || Admission::Discard { key: head.key };
        let out = store
            .admit("q".into(), vec![admit(), admit()], NOW_MS)
            .await
            .unwrap();
        assert_eq!(out, [Admitted::Finished, Admitted::Gone]);
    }

    #[tokio::test]
    async fn finish_writes_result_and_clears_request_state() {
        let (_dir, store) = open();
        store
            .submit(vec![new_request("a", "q", 100)])
            .await
            .unwrap();
        let (head, claim) = claim_head(&store, "q").await;
        let env = head.envelope.unwrap();
        let result = ResultMessage::http(&env, 200, b"ok");
        let applied = store
            .apply_outcomes(
                vec![Outcome::Finish {
                    claim: claim.clone(),
                    envelope: env.clone(),
                    result: result.clone(),
                }],
                NOW_MS,
            )
            .await
            .unwrap();
        assert_eq!(applied.results_written, 1);
        let body = store
            .pop_result("results".into(), NOW_MS)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_str::<ResultMessage>(&body).unwrap(),
            result
        );
        assert!(payload_bytes(&store, &env).await.is_none());
        let applied = store
            .apply_outcomes(vec![Outcome::Release { claim }], NOW_MS)
            .await
            .unwrap();
        assert_eq!(applied.fenced, 1);
        assert!(!store.has_pending("q".into()).await.unwrap());
        assert_eq!(store.cancel(vec!["a".into()], NOW_MS).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn blob_payload_lives_until_the_result_is_written() {
        let (dir, store) = open();
        let req = blob_request(&store, "a", "0a", b"RIFF-large-wav-bytes").await;
        assert_eq!(req.envelope.payload.size, 20);
        store.submit(vec![req]).await.unwrap();
        let (head, claim) = claim_head(&store, "q").await;
        let env = head.envelope.unwrap();
        assert_eq!(env.payload.content_type, "audio/wav");
        assert_eq!(
            payload_bytes(&store, &env).await.unwrap(),
            b"RIFF-large-wav-bytes"
        );
        store
            .apply_outcomes(
                vec![Outcome::Finish {
                    claim,
                    result: ResultMessage::http(&env, 200, b"{}"),
                    envelope: env.clone(),
                }],
                NOW_MS,
            )
            .await
            .unwrap();
        assert!(!dir.path().join("blobs/requests/0a").exists());
        assert!(payload_bytes(&store, &env).await.is_none());
    }

    #[tokio::test]
    async fn blob_payload_survives_retry_and_release() {
        let (_dir, store) = open();
        store
            .submit(vec![blob_request(&store, "a", "0a", b"0123456789").await])
            .await
            .unwrap();
        let (head, claim) = claim_head(&store, "q").await;
        store
            .apply_outcomes(
                vec![Outcome::Retry {
                    claim,
                    envelope: head.envelope.unwrap(),
                    due_ms: NOW_MS,
                }],
                NOW_MS,
            )
            .await
            .unwrap();
        store.promote_due_retries(NOW_MS, 10).await.unwrap();
        let (head, claim) = claim_head(&store, "q").await;
        store
            .apply_outcomes(vec![Outcome::Release { claim }], NOW_MS)
            .await
            .unwrap();
        let env = head.envelope.unwrap();
        assert_eq!(payload_bytes(&store, &env).await.unwrap(), b"0123456789");
    }

    #[tokio::test]
    async fn reopening_recovers_claims_and_deletes_orphan_blobs() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (store, _) = Store::open(dir.path(), &options()).unwrap();
            store
                .submit(vec![blob_request(&store, "a", "0a", b"0123456789").await])
                .await
                .unwrap();
            claim_head(&store, "q").await;
            // Staged but never submitted, as if the process died mid-submit.
            let _orphan = blob_request(&store, "b", "0b", b"9876543210").await;
            let mut w = store.blobs().writer().await.unwrap();
            w.write(b"result").await.unwrap();
            w.commit(&BlobKey::result("0c").unwrap()).await.unwrap();
        }
        let (store, recovery) = Store::open(dir.path(), &options()).unwrap();
        assert_eq!(recovery.claims, 1);
        assert_eq!(recovery.orphan_blobs, 2);
        let (head, _) = claim_head(&store, "q").await;
        assert_eq!(
            payload_bytes(&store, &head.envelope.unwrap())
                .await
                .unwrap(),
            b"0123456789"
        );
        assert!(!dir.path().join("blobs/requests/0b").exists());
    }

    #[tokio::test]
    async fn fenced_result_blob_is_deleted() {
        let (dir, store) = open();
        store
            .submit(vec![new_request("a", "q", 100)])
            .await
            .unwrap();
        let (head, claim) = claim_head(&store, "q").await;
        let env = head.envelope.unwrap();
        let key = BlobKey::result("0d").unwrap();
        let mut w = store.blobs().writer().await.unwrap();
        w.write(b"audio").await.unwrap();
        let digest = w.commit(&key).await.unwrap();
        let result = ResultMessage::http_by_reference(
            &env,
            200,
            StoredBody {
                payload_ref: key.to_ref(),
                content_type: "audio/wav".into(),
                size: digest.size,
                sha256: digest.sha256,
            },
        );
        let stale = ClaimRef {
            claim_id: claim.claim_id + 1,
            ..claim
        };
        let applied = store
            .apply_outcomes(
                vec![Outcome::Finish {
                    claim: stale,
                    envelope: env,
                    result,
                }],
                NOW_MS,
            )
            .await
            .unwrap();
        assert_eq!(applied.fenced, 1);
        assert!(!dir.path().join("blobs/results/0d").exists());
    }

    #[tokio::test]
    async fn undecodable_rows_are_skipped_not_fatal() {
        use crate::store::tables::{CLAIMED, RETRY};
        let (_dir, store) = open();
        store
            .run(|db| {
                let txn = db.begin_write()?;
                {
                    txn.open_table(RETRY)?.insert((0, 1), "garbage")?;
                    txn.open_table(CLAIMED)?.insert("x\u{0}ff", "garbage")?;
                }
                txn.commit()?;
                Ok(())
            })
            .await
            .unwrap();
        store
            .submit(vec![new_request("a", "q", 100)])
            .await
            .unwrap();
        let (head, claim) = claim_head(&store, "q").await;
        store
            .apply_outcomes(
                vec![
                    Outcome::Release {
                        claim: ClaimRef {
                            generation: "x\u{0}ff".into(),
                            claim_id: 1,
                        },
                    },
                    Outcome::Retry {
                        claim,
                        envelope: head.envelope.unwrap(),
                        due_ms: NOW_MS,
                    },
                ],
                NOW_MS,
            )
            .await
            .unwrap();
        assert_eq!(store.promote_due_retries(NOW_MS, 10).await.unwrap(), 1);
        assert!(store.has_pending("q".into()).await.unwrap());
    }

    #[tokio::test]
    async fn ending_a_claim_drops_its_unreferenced_result_blob() {
        let (dir, store) = open();
        store
            .submit(vec![new_request("a", "q", 100)])
            .await
            .unwrap();
        let (_, claim) = claim_head(&store, "q").await;
        // An attempt wrote a body, then lost to a drain.
        let mut w = store.blobs().writer().await.unwrap();
        w.write(b"late body").await.unwrap();
        w.commit(&BlobKey::result("61").unwrap()).await.unwrap();
        store
            .apply_outcomes(vec![Outcome::Release { claim }], NOW_MS)
            .await
            .unwrap();
        assert!(!dir.path().join("blobs/results/61").exists());
    }

    #[tokio::test]
    async fn stale_claim_id_is_fenced() {
        let (_dir, store) = open();
        store
            .submit(vec![new_request("a", "q", 100)])
            .await
            .unwrap();
        let (_, claim) = claim_head(&store, "q").await;
        let stale = ClaimRef {
            claim_id: claim.claim_id + 1,
            ..claim
        };
        let applied = store
            .apply_outcomes(vec![Outcome::Release { claim: stale }], NOW_MS)
            .await
            .unwrap();
        assert_eq!(applied.fenced, 1);
        assert!(!store.has_pending("q".into()).await.unwrap());
    }

    #[tokio::test]
    async fn release_restores_the_original_position() {
        let (_dir, store) = open();
        store
            .submit(vec![new_request("a", "q", 100), new_request("b", "q", 200)])
            .await
            .unwrap();
        let (head, claim) = claim_head(&store, "q").await;
        store
            .apply_outcomes(vec![Outcome::Release { claim }], NOW_MS)
            .await
            .unwrap();
        let again = store.peek("q".into(), 1).await.unwrap().remove(0);
        assert_eq!(again.key, head.key);
    }

    #[tokio::test]
    async fn retry_waits_until_due() {
        let (_dir, store) = open();
        store
            .submit(vec![new_request("a", "q", 100)])
            .await
            .unwrap();
        let (head, claim) = claim_head(&store, "q").await;
        let mut env = head.envelope.unwrap();
        env.routing.retry_count = 1;
        store
            .apply_outcomes(
                vec![Outcome::Retry {
                    claim,
                    envelope: env,
                    due_ms: NOW_MS + 500,
                }],
                NOW_MS,
            )
            .await
            .unwrap();
        assert_eq!(
            store.promote_due_retries(NOW_MS + 499, 10).await.unwrap(),
            0
        );
        assert!(!store.has_pending("q".into()).await.unwrap());
        assert_eq!(
            store.promote_due_retries(NOW_MS + 500, 10).await.unwrap(),
            1
        );
        let (head, _) = claim_head(&store, "q").await;
        assert_eq!(head.envelope.unwrap().routing.retry_count, 1);
    }

    #[tokio::test]
    async fn cancel_marks_only_the_live_generation() {
        let (_dir, store) = open();
        store
            .submit(vec![new_request("a", "q", 2_000)])
            .await
            .unwrap();
        assert_eq!(
            store
                .cancel(vec!["a".into(), "missing".into(), "".into()], NOW_MS)
                .await
                .unwrap(),
            1
        );
        assert!(
            store
                .is_cancelled("a".into(), "61".into(), NOW_MS)
                .await
                .unwrap()
        );
        assert!(
            !store
                .is_cancelled("a".into(), "other".into(), NOW_MS)
                .await
                .unwrap()
        );
        store
            .submit(vec![new_request("a", "q", 2_000)])
            .await
            .unwrap();
        assert!(
            !store
                .is_cancelled("a".into(), "61".into(), NOW_MS)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn cancel_after_deadline_is_a_noop() {
        let (_dir, store) = open();
        store.submit(vec![new_request("a", "q", 10)]).await.unwrap();
        assert_eq!(store.cancel(vec!["a".into()], 10_000).await.unwrap(), 0);
        assert_eq!(store.sweep_request_markers(10_000).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn backlog_counts_deadline_buckets() {
        let (_dir, store) = open();
        store
            .submit(vec![
                new_request("expired", "q", 90),
                new_request("now", "q", 100),
                new_request("soon", "q", 105),
                new_request("later", "q", 1000),
                new_request("elsewhere", "r", 100),
            ])
            .await
            .unwrap();
        let b = store
            .backlog("q".into(), 100, vec![-1, 0, 5, 60])
            .await
            .unwrap();
        assert_eq!(b.depth, 4);
        assert_eq!(b.cumulative, [1, 2, 3, 3]);
    }
}
