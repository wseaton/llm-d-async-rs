use std::sync::atomic::Ordering;

use bytes::Bytes;
use redb::{ReadableDatabase, ReadableTable, ReadableTableMetadata, Table, WriteTransaction};

use crate::api::payload::PayloadStorage;
use crate::api::request::InternalRequest;
use crate::api::result::ResultMessage;
use crate::store::blob::key::BlobKey;
use crate::store::embedded::EmbeddedStore;
use crate::store::embedded::tables::{
    ACTIVE, BLOBS, BlobRecord, CANCELLED, CLAIMED, ClaimedRecord, META, META_SEQ, PAYLOADS,
    PENDING, RESULTS, RETRY, ResultRecord, RetryRecord, TokenRecord,
};
use crate::store::error::StoreError;
use crate::store::queue::{
    Admission, Admitted, Applied, Backlog, CANCEL_MARKER_TTL_MS, ClaimRef, NewRequest, Outcome,
    PayloadBody, Peeked, PendingKey,
};
use crate::store::staging::StagedPayload;

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
                    self.blobs.remove(key.to_string().as_str())?;
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
            self.blobs.insert(
                key.to_string().as_str(),
                serde_json::to_string(&record)?.as_str(),
            )?;
        }
        let record = ResultRecord {
            expires_at_ms,
            blob: blob.map(|k| k.to_string()),
            body: serde_json::to_string(result)?,
        };
        let seq = self.next_seq()?;
        self.results.insert(
            (envelope.routing.result_queue_name.as_str(), seq),
            serde_json::to_string(&record)?.as_str(),
        )?;
        Ok(())
    }

    /// Drops `key` unless a result references it: a body written by an
    /// attempt whose claim was already over is garbage, but the same outcome
    /// applied twice must not delete what its first application recorded.
    fn discard_unreferenced(&mut self, key: Option<BlobKey>) -> Result<(), StoreError> {
        if let Some(key) = key
            && self.blobs.get(key.to_string().as_str())?.is_none()
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

impl EmbeddedStore {
    pub(crate) async fn submit_requests(
        &self,
        requests: Vec<NewRequest>,
    ) -> Result<(), StoreError> {
        self.run(move |db| {
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
                            t.payloads
                                .insert(r.envelope.generation_key().as_str(), bytes.as_slice())?;
                        }
                        StagedPayload::Blob { key, .. } => {
                            let record = BlobRecord {
                                content_type: r.envelope.payload.content_type.clone(),
                                expires_at_ms: None,
                            };
                            t.blobs.insert(
                                key.to_string().as_str(),
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
        })
        .await
    }

    pub(crate) async fn peek_queue(
        &self,
        queue: String,
        limit: usize,
        now_ms: i64,
    ) -> Result<Vec<Peeked>, StoreError> {
        self.run(move |db| {
            let txn = db.begin_read()?;
            let pending = txn.open_table(PENDING)?;
            let cancelled = txn.open_table(CANCELLED)?;
            let mut out = Vec::new();
            for entry in pending.range(queue_range(&queue))?.take(limit) {
                let (key, value) = entry?;
                let (_, deadline, seq) = key.value();
                let envelope: Result<InternalRequest, String> =
                    serde_json::from_str(value.value()).map_err(|e| e.to_string());
                let is_cancelled = match &envelope {
                    Ok(env) => match cancelled.get(env.request.id.as_str())? {
                        Some(v) => serde_json::from_str::<TokenRecord>(v.value()).is_ok_and(|r| {
                            r.token == env.routing.request_token && r.expires_at_ms > now_ms
                        }),
                        None => false,
                    },
                    Err(_) => false,
                };
                out.push(Peeked {
                    key: PendingKey { deadline, seq },
                    envelope,
                    cancelled: is_cancelled,
                });
            }
            Ok(out)
        })
        .await
    }

    pub(crate) async fn queue_has_pending(&self, queue: String) -> Result<bool, StoreError> {
        self.run(move |db| {
            let txn = db.begin_read()?;
            let pending = txn.open_table(PENDING)?;
            Ok(pending.range(queue_range(&queue))?.next().is_some())
        })
        .await
    }

    pub(crate) async fn cancelled(
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

    pub(crate) async fn cancel_ids(
        &self,
        ids: Vec<String>,
        now_ms: i64,
    ) -> Result<usize, StoreError> {
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

    pub(crate) async fn admit_rows(
        &self,
        queue: String,
        admissions: Vec<Admission>,
        now_ms: i64,
    ) -> Result<Vec<Admitted>, StoreError> {
        let claim_ids = std::sync::Arc::clone(&self.next_claim_id);
        let (out, dropped) = self
            .run(move |db| {
                let txn = db.begin_write()?;
                let mut out = Vec::with_capacity(admissions.len());
                let dropped;
                {
                    let mut t = Tables::open(&txn)?;
                    for admission in &admissions {
                        let key = admission.key();
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
                                let payload = t
                                    .payloads
                                    .get(generation.as_str())?
                                    .map(|v| Bytes::copy_from_slice(v.value()));
                                out.push(Admitted::Claimed { claim, payload });
                            }
                            Admission::Finish {
                                envelope, result, ..
                            } => {
                                t.finish(envelope, result, now_ms)?;
                                out.push(Admitted::Finished);
                            }
                            Admission::Discard { .. } => {
                                if let Ok(envelope) =
                                    serde_json::from_str::<InternalRequest>(&envelope_json)
                                {
                                    t.drop_payload(&envelope)?;
                                }
                                out.push(Admitted::Finished);
                            }
                        }
                    }
                    dropped = std::mem::take(&mut t.dropped);
                }
                txn.commit()?;
                Ok((out, dropped))
            })
            .await?;
        self.blobs.remove_dropped(&dropped).await;
        Ok(out)
    }

    pub(crate) async fn end_claims(
        &self,
        outcomes: Vec<Outcome>,
        now_ms: i64,
    ) -> Result<Applied, StoreError> {
        let (applied, dropped) = self.run(move |db| {
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
                }
                dropped = std::mem::take(&mut t.dropped);
            }
            txn.commit()?;
            Ok((applied, dropped))
        })
        .await?;
        self.blobs.remove_dropped(&dropped).await;
        Ok(applied)
    }

    pub(crate) async fn promote_retries(
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

    pub(crate) async fn queue_backlog(
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
    pub(crate) async fn sweep_request_markers(&self, now_ms: i64) -> Result<u64, StoreError> {
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

    pub(crate) async fn payload(
        &self,
        envelope: &InternalRequest,
    ) -> Result<Option<PayloadBody>, StoreError> {
        match envelope.payload.storage {
            PayloadStorage::Inline => {
                let generation = envelope.generation_key();
                self.run(move |db| {
                    let txn = db.begin_read()?;
                    let payloads = txn.open_table(PAYLOADS)?;
                    Ok(payloads
                        .get(generation.as_str())?
                        .map(|v| PayloadBody::Inline(Bytes::copy_from_slice(v.value()))))
                })
                .await
            }
            PayloadStorage::Blob => {
                let Some(key) = BlobKey::request(&envelope.routing.request_token) else {
                    return Ok(None);
                };
                Ok(self.blobs.open(&key).await?.map(PayloadBody::Blob))
            }
        }
    }
}
