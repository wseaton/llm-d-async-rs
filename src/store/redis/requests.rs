use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;

use crate::api::payload::PayloadStorage;
use crate::api::progress::Progress;
use crate::api::request::InternalRequest;
use crate::api::result::ResultMessage;
use crate::store::blob::key::BlobKey;
use crate::store::error::StoreError;
use crate::store::queue::{
    Admission, Admitted, Applied, Backlog, CANCEL_MARKER_TTL_MS, ClaimRef, NewRequest, Outcome,
    PayloadBody, Peeked, PendingKey, RequestStatus,
};
use crate::store::redis::wire::{self, Body, Decoded};
use crate::store::redis::{Held, RedisStore, keys, lock, secs};
use crate::store::staging::StagedPayload;

/// Saved progress outlives its request's deadline by this much, so a
/// continuation dispatched right at the deadline still finds it.
const PROGRESS_GRACE_MS: i64 = 10 * 60 * 1000;

/// Splits a generation key back into ID and token.
fn claim_key_of(generation: &str) -> String {
    match generation.split_once('\u{0}') {
        Some((id, token)) => keys::claim_key(id, token),
        None => generation.to_owned(),
    }
}

fn request_blob(envelope: &InternalRequest) -> Option<BlobKey> {
    match envelope.payload.storage {
        PayloadStorage::Blob => BlobKey::request(&envelope.routing.request_token),
        PayloadStorage::Inline => None,
    }
}

fn result_blob(result: &ResultMessage) -> Option<BlobKey> {
    BlobKey::from_ref(&result.payload_ref)
}

fn progress_expiry_ms(envelope: &InternalRequest) -> i64 {
    envelope
        .request
        .deadline
        .saturating_mul(1000)
        .saturating_add(PROGRESS_GRACE_MS)
}

/// What the finish scripts need beyond the claim or the pending member.
struct Finish<'a> {
    envelope: &'a InternalRequest,
    result: &'a ResultMessage,
    result_json: String,
    payload_key: String,
}

impl<'a> Finish<'a> {
    fn new(
        envelope: &'a InternalRequest,
        result: &'a ResultMessage,
        payload_key: String,
    ) -> Result<Self, StoreError> {
        Ok(Self {
            envelope,
            result,
            result_json: serde_json::to_string(result)?,
            payload_key,
        })
    }
}

impl RedisStore {
    pub(crate) async fn submit_requests(
        &self,
        requests: Vec<NewRequest>,
    ) -> Result<(), StoreError> {
        let mut pipe = ::redis::pipe();
        pipe.atomic();
        for r in requests {
            let mut envelope = r.envelope;
            let body = match r.payload {
                StagedPayload::Inline(bytes) if wire::is_json(&bytes) => {
                    Body::Inline(Bytes::from(bytes))
                }
                StagedPayload::Inline(bytes) => {
                    self.spill(&envelope, bytes).await?;
                    envelope.payload.storage = PayloadStorage::Blob;
                    Body::Blob
                }
                StagedPayload::Blob { .. } => Body::Blob,
            };
            let id = &envelope.request.id;
            let token = &envelope.routing.request_token;
            let member = wire::encode(&envelope, &body)?;
            pipe.del(keys::cancel(id)).ignore();
            pipe.cmd("SET")
                .arg(keys::active(id))
                .arg(token)
                .arg("PXAT")
                .arg(envelope.request.deadline.saturating_mul(1000).max(1))
                .ignore();
            pipe.zadd(
                &envelope.routing.request_queue_name,
                &member,
                envelope.request.deadline,
            )
            .ignore();
            if let Body::Blob = body
                && let Some(blob) = request_blob(&envelope)
            {
                pipe.hset(
                    keys::BLOB_TYPES,
                    blob.to_string(),
                    &envelope.payload.content_type,
                )
                .ignore();
            }
            if let Some(progress) = &envelope.progress {
                pipe.cmd("SET")
                    .arg(keys::progress(id, token))
                    .arg(serde_json::to_string(progress)?)
                    .arg("PXAT")
                    .arg(progress_expiry_ms(&envelope))
                    .ignore();
            }
        }
        let () = pipe.query_async(&mut self.conn()).await?;
        Ok(())
    }

    /// Writes a small body that cannot travel inline to its blob.
    async fn spill(&self, envelope: &InternalRequest, bytes: Vec<u8>) -> Result<(), StoreError> {
        let token = &envelope.routing.request_token;
        let key = BlobKey::request(token).ok_or_else(|| {
            StoreError::Config(format!("request token {token:?} cannot name a blob"))
        })?;
        let mut writer = self.inner.blobs.create(&key).await?;
        writer.write(Bytes::from(bytes)).await?;
        writer.commit().await?;
        Ok(())
    }

    pub(crate) async fn peek_queue(
        &self,
        queue: String,
        limit: usize,
        now_ms: i64,
    ) -> Result<Vec<Peeked>, StoreError> {
        let mut conn = self.conn();
        let rows: Vec<(String, f64)> = ::redis::cmd("ZRANGEBYSCORE")
            .arg(&queue)
            .arg("-inf")
            .arg("+inf")
            .arg("WITHSCORES")
            .arg("LIMIT")
            .arg(0)
            .arg(limit)
            .query_async(&mut conn)
            .await?;
        let decoded: Vec<Result<Decoded, String>> = rows
            .iter()
            .map(|(member, _)| wire::decode(member).map_err(|e| e.to_string()))
            .collect();
        let mut pipe = ::redis::pipe();
        for d in decoded.iter().flatten() {
            let (id, token) = (&d.envelope.request.id, &d.envelope.routing.request_token);
            pipe.get(keys::cancel(id))
                .cmd("PEXPIRETIME")
                .arg(keys::cancel(id))
                .get(keys::progress(id, token));
        }
        let looked: Vec<(Option<String>, i64, Option<String>)> =
            if decoded.iter().any(Result::is_ok) {
                pipe.query_async(&mut conn).await?
            } else {
                Vec::new()
            };
        let mut looked = looked.into_iter();
        let mut cache = HashMap::with_capacity(rows.len());
        let mut out = Vec::with_capacity(rows.len());
        for ((member, score), decoded) in rows.into_iter().zip(decoded) {
            let seq = keys::claim_id_of(&member);
            let (envelope, cancelled) = match decoded {
                Ok(d) => {
                    let (marker, marker_expiry, progress) =
                        looked.next().unwrap_or((None, -2, None));
                    let mut envelope = d.envelope;
                    let cancelled = marker.as_deref()
                        == Some(envelope.routing.request_token.as_str())
                        && (marker_expiry < 0 || marker_expiry > now_ms);
                    envelope.progress = progress.and_then(|p| {
                        serde_json::from_str::<Progress>(&p)
                            .inspect_err(|e| tracing::warn!(id = %envelope.request.id, error = %e, "ignoring undecodable progress"))
                            .ok()
                    });
                    (Ok(envelope), cancelled)
                }
                Err(e) => (Err(e), false),
            };
            let deadline = match &envelope {
                Ok(env) => env.request.deadline,
                Err(_) => score as i64,
            };
            cache.insert(seq, member);
            out.push(Peeked {
                key: PendingKey { deadline, seq },
                envelope,
                cancelled,
            });
        }
        lock(&self.inner.peeked).insert(queue, cache);
        Ok(out)
    }

    fn take_peeked(&self, queue: &str, seq: u64) -> Option<String> {
        lock(&self.inner.peeked)
            .get_mut(queue)
            .and_then(|m| m.remove(&seq))
    }

    pub(crate) async fn admit_rows(
        &self,
        queue: String,
        admissions: Vec<Admission>,
        now_ms: i64,
    ) -> Result<Vec<Admitted>, StoreError> {
        let mut out = Vec::with_capacity(admissions.len());
        for admission in admissions {
            let Some(member) = self.take_peeked(&queue, admission.key().seq) else {
                out.push(Admitted::Gone);
                continue;
            };
            out.push(match admission {
                Admission::Claim { generation, .. } => {
                    self.claim_member(&queue, &member, generation, now_ms)
                        .await?
                }
                Admission::Finish {
                    envelope, result, ..
                } => {
                    let payload_key = match wire::decode(&member).map(|d| d.body) {
                        Ok(Body::Key(k)) => k,
                        _ => String::new(),
                    };
                    let finish = Finish::new(&envelope, &result, payload_key)?;
                    if self
                        .finish_pending(&queue, &member, &finish, now_ms)
                        .await?
                    {
                        self.remove_blob(request_blob(&envelope)).await;
                        Admitted::Finished
                    } else {
                        self.discard_unreferenced(result_blob(&result)).await?;
                        Admitted::Gone
                    }
                }
                Admission::Discard { .. } => {
                    let blob = wire::decode(&member)
                        .ok()
                        .and_then(|d| request_blob(&d.envelope));
                    let removed: i64 = self
                        .inner
                        .scripts
                        .discard
                        .key(&queue)
                        .key(keys::BLOB_TYPES)
                        .key(keys::BLOB_EXPIRY)
                        .arg(&member)
                        .arg(blob.as_ref().map(ToString::to_string).unwrap_or_default())
                        .invoke_async(&mut self.conn())
                        .await?;
                    if removed == 1 {
                        self.remove_blob(blob).await;
                        Admitted::Finished
                    } else {
                        Admitted::Gone
                    }
                }
            });
        }
        Ok(out)
    }

    async fn claim_member(
        &self,
        queue: &str,
        member: &str,
        generation: String,
        now_ms: i64,
    ) -> Result<Admitted, StoreError> {
        let decoded = match wire::decode(member) {
            Ok(d) => d,
            Err(e) => {
                tracing::error!(%queue, error = %e, "cannot claim an undecodable request");
                return Ok(Admitted::Gone);
            }
        };
        let env = &decoded.envelope;
        let claim_key = keys::claim_key(&env.request.id, &env.routing.request_token);
        let claim_id = loop {
            let id: u64 = rand::random();
            if id != 0 {
                break id;
            }
        };
        let owner = format!("{claim_id:016x}");
        let claimed: i64 = self
            .inner
            .scripts
            .claim
            .key(queue)
            .key(keys::claimed(queue))
            .key(keys::claim_owners(queue))
            .key(keys::claims_idx(queue))
            .arg(&claim_key)
            .arg(member)
            .arg(&owner)
            .arg(self.lease_expiry(now_ms, env.request.deadline))
            .invoke_async(&mut self.conn())
            .await?;
        if claimed != 1 {
            return Ok(Admitted::Gone);
        }
        lock(&self.inner.held).insert(
            generation.clone(),
            Held {
                queue: queue.to_owned(),
                claim_key,
                owner,
                deadline: env.request.deadline,
            },
        );
        let payload = match decoded.body {
            Body::Inline(raw) => Some(raw),
            Body::Key(key) => {
                let bytes: Option<Vec<u8>> = ::redis::cmd("GET")
                    .arg(key)
                    .query_async(&mut self.conn())
                    .await?;
                bytes.map(Bytes::from)
            }
            Body::Blob => None,
        };
        Ok(Admitted::Claimed {
            claim: ClaimRef {
                generation,
                claim_id,
            },
            payload,
        })
    }

    /// The finish script's keys and arguments after the leading three of
    /// each.
    fn finish_tail(
        &self,
        invocation: &mut ::redis::ScriptInvocation<'_>,
        f: &Finish<'_>,
        now_ms: i64,
    ) {
        let env = f.envelope;
        let (id, token) = (&env.request.id, &env.routing.request_token);
        let route = &env.routing.result_queue_name;
        let ttl_ms =
            i64::try_from(env.routing.result_ttl_seconds.saturating_mul(1000)).unwrap_or(i64::MAX);
        let result_blob = result_blob(f.result);
        invocation
            .key(route)
            .key(keys::active(id))
            .key(keys::cancel(id))
            .key(keys::progress(id, token))
            .key(&f.payload_key)
            .key(keys::RESULT_ROUTES)
            .key(keys::BLOB_TYPES)
            .key(keys::BLOB_EXPIRY)
            .arg(&f.result_json)
            .arg(ttl_ms)
            .arg(now_ms)
            .arg(token)
            .arg(request_blob(env).map(|k| k.to_string()).unwrap_or_default())
            .arg(
                result_blob
                    .as_ref()
                    .map(ToString::to_string)
                    .unwrap_or_default(),
            )
            .arg(&f.result.content_type)
            .arg(route)
            .arg(keys::RESULTS_CHANNEL);
    }

    async fn finish_pending(
        &self,
        queue: &str,
        member: &str,
        f: &Finish<'_>,
        now_ms: i64,
    ) -> Result<bool, StoreError> {
        let mut invocation = self.inner.scripts.finish_pending.prepare_invoke();
        invocation.key(queue).key("").key("").arg(member).arg("");
        self.finish_tail(&mut invocation, f, now_ms);
        let done: i64 = invocation.invoke_async(&mut self.conn()).await?;
        Ok(done == 1)
    }

    async fn finish_claimed(
        &self,
        held: &Held,
        owner: &str,
        f: &Finish<'_>,
        now_ms: i64,
    ) -> Result<bool, StoreError> {
        let mut invocation = self.inner.scripts.finish_claimed.prepare_invoke();
        invocation
            .key(keys::claimed(&held.queue))
            .key(keys::claim_owners(&held.queue))
            .key(keys::claims_idx(&held.queue))
            .arg(&held.claim_key)
            .arg(owner);
        self.finish_tail(&mut invocation, f, now_ms);
        let done: i64 = invocation.invoke_async(&mut self.conn()).await?;
        Ok(done == 1)
    }

    /// The member a claim holds, as it was claimed.
    async fn claimed_member(
        &self,
        queue: &str,
        claim_key: &str,
    ) -> Result<Option<String>, StoreError> {
        Ok(::redis::cmd("HGET")
            .arg(keys::claimed(queue))
            .arg(claim_key)
            .query_async(&mut self.conn())
            .await?)
    }

    pub(crate) async fn end_claims(
        &self,
        outcomes: Arc<[Outcome]>,
        now_ms: i64,
    ) -> Result<Applied, StoreError> {
        let mut applied = Applied::default();
        for outcome in outcomes.iter() {
            let claim = outcome.claim();
            let owner = format!("{:016x}", claim.claim_id);
            let held = self.held(&claim.generation).unwrap_or_else(|| Held {
                queue: String::new(),
                claim_key: claim_key_of(&claim.generation),
                owner: String::new(),
                deadline: 0,
            });
            let ended = if held.queue.is_empty() {
                false
            } else {
                match outcome {
                    Outcome::Finish {
                        envelope, result, ..
                    } => {
                        let payload_key = match self
                            .claimed_member(&held.queue, &held.claim_key)
                            .await?
                            .map(|m| wire::decode(&m).map(|d| d.body))
                        {
                            Some(Ok(Body::Key(k))) => k,
                            _ => String::new(),
                        };
                        let f = Finish::new(envelope, result, payload_key)?;
                        let done = self.finish_claimed(&held, &owner, &f, now_ms).await?;
                        if done {
                            applied
                                .result_routes
                                .push(envelope.routing.result_queue_name.clone());
                            self.remove_blob(request_blob(envelope)).await;
                        }
                        done
                    }
                    Outcome::Retry {
                        envelope, due_ms, ..
                    } => self.retry(&held, &owner, envelope, *due_ms).await?,
                    Outcome::Release { .. } => {
                        let released: i64 = self
                            .inner
                            .scripts
                            .release
                            .key(&held.queue)
                            .key(keys::claimed(&held.queue))
                            .key(keys::claim_owners(&held.queue))
                            .key(keys::claims_idx(&held.queue))
                            .arg(&held.claim_key)
                            .arg(&owner)
                            .arg(held.deadline)
                            .invoke_async(&mut self.conn())
                            .await?;
                        released == 1
                    }
                }
            };
            if ended || held.owner == owner {
                self.forget(&claim.generation);
            }
            if !ended {
                applied.fenced += 1;
                if let Outcome::Finish { result, .. } = outcome {
                    self.discard_unreferenced(result_blob(result)).await?;
                }
            }
        }
        Ok(applied)
    }

    /// Parks a claimed request until `due_ms` with `envelope`'s routing and
    /// progress, and ends the claim, in one step.
    async fn retry(
        &self,
        held: &Held,
        owner: &str,
        envelope: &InternalRequest,
        due_ms: i64,
    ) -> Result<bool, StoreError> {
        let Some(member) = self.claimed_member(&held.queue, &held.claim_key).await? else {
            return Ok(false);
        };
        let body = wire::decode(&member)?.body;
        let requeued = wire::encode(envelope, &body)?;
        let (id, token) = (&envelope.request.id, &envelope.routing.request_token);
        let progress = envelope
            .progress
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?
            .unwrap_or_default();
        let retried: i64 = self
            .inner
            .scripts
            .retry
            .key(keys::claimed(&held.queue))
            .key(keys::claim_owners(&held.queue))
            .key(keys::claims_idx(&held.queue))
            .key(&self.inner.options.retry_queue)
            .key(keys::progress(id, token))
            .arg(&held.claim_key)
            .arg(owner)
            .arg(secs(due_ms))
            .arg(requeued)
            .arg(progress)
            .arg(progress_expiry_ms(envelope))
            .invoke_async(&mut self.conn())
            .await?;
        Ok(retried == 1)
    }

    /// Deletes a blob the store no longer references. Failures are logged:
    /// orphan collection deletes it later.
    async fn remove_blob(&self, key: Option<BlobKey>) {
        if let Some(key) = key
            && let Err(e) = self.inner.blobs.remove(&key).await
        {
            tracing::warn!(blob = %key, error = %e, "deleting a blob failed; orphan collection will retry");
        }
    }

    /// Deletes a result body an outcome wrote unless a result references it.
    async fn discard_unreferenced(&self, key: Option<BlobKey>) -> Result<(), StoreError> {
        if let Some(key) = key {
            let referenced: bool = ::redis::cmd("HEXISTS")
                .arg(keys::BLOB_TYPES)
                .arg(key.to_string())
                .query_async(&mut self.conn())
                .await?;
            if !referenced {
                self.remove_blob(Some(key)).await;
            }
        }
        Ok(())
    }

    pub(crate) async fn payload(
        &self,
        envelope: &InternalRequest,
    ) -> Result<Option<PayloadBody>, StoreError> {
        if let Some(key) = request_blob(envelope) {
            return Ok(self.inner.blobs.open(&key).await?.map(PayloadBody::Blob));
        }
        let queue = &envelope.routing.request_queue_name;
        let claim_key = keys::claim_key(&envelope.request.id, &envelope.routing.request_token);
        let Some(member) = self.claimed_member(queue, &claim_key).await? else {
            return Ok(None);
        };
        match wire::decode(&member)?.body {
            Body::Inline(raw) => Ok(Some(PayloadBody::Inline(raw))),
            Body::Key(key) => {
                let bytes: Option<Vec<u8>> = ::redis::cmd("GET")
                    .arg(key)
                    .query_async(&mut self.conn())
                    .await?;
                Ok(bytes.map(|b| PayloadBody::Inline(Bytes::from(b))))
            }
            Body::Blob => Ok(None),
        }
    }

    pub(crate) async fn cancelled(
        &self,
        id: String,
        token: String,
        now_ms: i64,
    ) -> Result<bool, StoreError> {
        let (marker, expires): (Option<String>, i64) = ::redis::pipe()
            .get(keys::cancel(&id))
            .cmd("PEXPIRETIME")
            .arg(keys::cancel(&id))
            .query_async(&mut self.conn())
            .await?;
        Ok(marker.as_deref() == Some(token.as_str()) && (expires < 0 || expires > now_ms))
    }

    pub(crate) async fn status(
        &self,
        id: String,
        token: Option<String>,
    ) -> Result<RequestStatus, StoreError> {
        let mut conn = self.conn();
        let live: Option<String> = ::redis::cmd("GET")
            .arg(keys::active(&id))
            .query_async(&mut conn)
            .await?;
        let Some(live) = live else {
            return Ok(RequestStatus::Done);
        };
        if token.is_some_and(|t| t != live) {
            return Ok(RequestStatus::Done);
        }
        let claim_key = keys::claim_key(&id, &live);
        let queues: Vec<String> = lock(&self.inner.joined).iter().cloned().collect();
        for queue in queues {
            let claimed: bool = ::redis::cmd("HEXISTS")
                .arg(keys::claimed(&queue))
                .arg(&claim_key)
                .query_async(&mut conn)
                .await?;
            if claimed {
                return Ok(RequestStatus::InProgress);
            }
        }
        Ok(RequestStatus::Queued)
    }

    pub(crate) async fn cancel_ids(
        &self,
        ids: Vec<String>,
        now_ms: i64,
    ) -> Result<usize, StoreError> {
        let mut marked = 0;
        for id in ids.iter().filter(|id| !id.is_empty()) {
            let n: i64 = self
                .inner
                .scripts
                .cancel
                .key(keys::active(id))
                .key(keys::cancel(id))
                .arg(now_ms)
                .arg(CANCEL_MARKER_TTL_MS)
                .invoke_async(&mut self.conn())
                .await?;
            if n == 1 {
                marked += 1;
            }
        }
        Ok(marked)
    }

    pub(crate) async fn queue_backlog(
        &self,
        queue: String,
        now_s: i64,
        bounds_s: Vec<i64>,
    ) -> Result<Backlog, StoreError> {
        let mut pipe = ::redis::pipe();
        pipe.zcard(&queue);
        for bound in &bounds_s {
            pipe.zcount(&queue, "-inf", now_s.saturating_add(*bound));
        }
        let counts: Vec<u64> = pipe.query_async(&mut self.conn()).await?;
        let mut counts = counts.into_iter();
        Ok(Backlog {
            depth: counts.next().unwrap_or(0),
            cumulative: counts.collect(),
        })
    }

    pub(crate) async fn collect_orphan_blobs(&self, cutoff_ms: i64) -> Result<usize, StoreError> {
        let listed = self.inner.blobs.list(cutoff_ms).await?;
        if listed.is_empty() {
            return Ok(0);
        }
        let referenced: Vec<String> = ::redis::cmd("HKEYS")
            .arg(keys::BLOB_TYPES)
            .query_async(&mut self.conn())
            .await?;
        let referenced: std::collections::HashSet<String> = referenced.into_iter().collect();
        let mut removed = 0;
        for l in listed {
            if !referenced.contains(&l.key.to_string()) {
                self.inner.blobs.remove(&l.key).await?;
                removed += 1;
            }
        }
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use crate::store::redis::requests::claim_key_of;

    #[test]
    fn generations_map_back_to_claim_keys() {
        assert_eq!(claim_key_of("a\u{0}0f"), "a\u{0}0f");
        assert_eq!(claim_key_of("a\u{0}"), "a");
        assert_eq!(claim_key_of("a"), "a");
    }
}
