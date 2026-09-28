//! Durable queue state in Redis, in upstream llm-d-async's `redis-sortedset`
//! layout, so Go producers, Go dispatchers and the llm-d-router coordinator's
//! async-broker share queues, results and cancellation with this processor.
//!
//! ```text
//!  submit ──► <queue> (zset, score = deadline) ──CLAIM──► <queue>:claimed / :claim-owners / :claims-idx
//!                ▲    │                                     │     │     │
//!                │    └─ terminal at admission ─┐           │     │     └─ lease lapses ─► RECLAIM ─► <queue>
//!                │                              ▼           ▼     │
//!                │                        <result route> (list, LPUSH) ◄── FINISH
//!                └── PROMOTE ◄── retry-sortedset (score = due) ◄──┘ RETRY
//! ```
//!
//! A claim is a lease: its holder renews it while it holds the request, and
//! any replica serving the queue returns it once it lapses. What upstream
//! does not store lives under `llm-d-async:` (blob references, result claim
//! IDs, control values) or in `request-progress:<id>:<token>`.

pub(crate) mod counters;
pub(crate) mod keys;
mod requests;
mod results;
pub(crate) mod scripts;
pub mod wire;

#[cfg(test)]
pub(crate) mod test_support;

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use ::redis::aio::ConnectionManager;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::api::request::InternalRequest;
use crate::boxed::BoxFuture;
use crate::clock::now_millis;
use crate::store::blob::key::BlobKey;
use crate::store::blob::{BlobBody, BlobStore};
use crate::store::error::StoreError;
use crate::store::queue::{
    AckOutcome, Admission, Admitted, Applied, Backlog, NewRequest, Outcome, PayloadBody, Peeked,
    RequestStatus, ResultClaim,
};
use crate::store::redis::counters::RedisCounters;
use crate::store::redis::scripts::Scripts;
use crate::store::signal::ResultSignal;
use crate::store::{QueueStore, Stamped};

/// A claim is returned this long after its request's deadline at the
/// latest, as upstream does.
const RECLAIM_GRACE_S: f64 = 300.0;
const RECLAIM_BATCH: usize = 100;
const FIRST_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct RedisOptions {
    /// How long a claim lasts without renewal.
    pub claim_lease: Duration,
    /// How often lapsed claims are returned to their queues.
    pub reclaim_interval: Duration,
    /// The sorted set retries wait in, shared with Go dispatchers.
    pub retry_queue: String,
    pub result_blob_retention: Duration,
}

/// A claim this process holds.
#[derive(Debug, Clone)]
struct Held {
    queue: String,
    claim_key: String,
    owner: String,
    deadline: i64,
}

struct Inner {
    client: ::redis::Client,
    conn: ConnectionManager,
    blobs: BlobStore,
    scripts: Scripts,
    options: RedisOptions,
    /// Held claims by generation.
    held: Mutex<HashMap<String, Held>>,
    joined: Mutex<BTreeSet<String>>,
    /// The members each queue's last peek returned, by pending key.
    peeked: Mutex<HashMap<String, HashMap<u64, String>>>,
    stop: CancellationToken,
}

#[derive(Clone)]
pub struct RedisStore {
    inner: Arc<Inner>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

fn secs(ms: i64) -> f64 {
    ms as f64 / 1000.0
}

fn millis(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

impl RedisStore {
    /// Connects to the Redis at `url` and starts renewing held claims,
    /// returning lapsed ones, and relaying result writes to `results`.
    pub async fn open(
        url: &str,
        blobs: BlobStore,
        options: RedisOptions,
        results: Arc<ResultSignal>,
    ) -> Result<Self, StoreError> {
        let client = ::redis::Client::open(url)?;
        let conn = client.get_connection_manager().await?;
        let store = Self {
            inner: Arc::new(Inner {
                client,
                conn,
                blobs,
                scripts: Scripts::new(),
                options,
                held: Mutex::default(),
                joined: Mutex::default(),
                peeked: Mutex::default(),
                stop: CancellationToken::new(),
            }),
        };
        store.check().await?;
        tokio::spawn(store.clone().renew_loop());
        tokio::spawn(store.clone().reclaim_loop());
        tokio::spawn(store.clone().relay_loop(results));
        Ok(store)
    }

    pub fn counters(&self, slot_ttl: Duration) -> RedisCounters {
        RedisCounters::new(self.conn(), slot_ttl)
    }

    fn conn(&self) -> ConnectionManager {
        self.inner.conn.clone()
    }

    async fn check(&self) -> Result<(), StoreError> {
        let _: String = ::redis::cmd("PING").query_async(&mut self.conn()).await?;
        Ok(())
    }

    fn lease_expiry(&self, now_ms: i64, deadline: i64) -> f64 {
        let lease = secs(now_ms) + self.inner.options.claim_lease.as_secs_f64();
        lease.min(deadline as f64 + RECLAIM_GRACE_S)
    }

    fn held(&self, generation: &str) -> Option<Held> {
        lock(&self.inner.held).get(generation).cloned()
    }

    fn forget(&self, generation: &str) {
        lock(&self.inner.held).remove(generation);
    }

    /// Renews every held claim well before its lease lapses. A claim that
    /// cannot be renewed is no longer this process's to end.
    async fn renew_loop(self) {
        let lease = self.inner.options.claim_lease;
        let every = (lease / 3).clamp(Duration::from_secs(1), Duration::from_secs(30));
        loop {
            tokio::select! {
                () = self.inner.stop.cancelled() => return,
                () = tokio::time::sleep(every) => {}
            }
            let held: Vec<(String, Held)> = lock(&self.inner.held)
                .iter()
                .map(|(g, h)| (g.clone(), h.clone()))
                .collect();
            for (generation, h) in held {
                let expiry = self.lease_expiry(now_millis(), h.deadline);
                let renewed: Result<i64, _> = self
                    .inner
                    .scripts
                    .renew
                    .key(keys::claimed(&h.queue))
                    .key(keys::claims_idx(&h.queue))
                    .key(keys::claim_owners(&h.queue))
                    .arg(&h.claim_key)
                    .arg(expiry)
                    .arg(&h.owner)
                    .invoke_async(&mut self.conn())
                    .await;
                match renewed {
                    Ok(1) => {}
                    Ok(_) => {
                        tracing::warn!(queue = %h.queue, claim = %h.claim_key, "lost a claim before renewing it");
                        let mut map = lock(&self.inner.held);
                        if map.get(&generation).is_some_and(|c| c.owner == h.owner) {
                            map.remove(&generation);
                        }
                    }
                    Err(e) => {
                        tracing::warn!(queue = %h.queue, error = %e, "renewing a claim failed")
                    }
                }
            }
        }
    }

    /// Returns claims whose lease lapsed, on every joined queue, whoever
    /// held them.
    async fn reclaim_loop(self) {
        loop {
            tokio::select! {
                () = self.inner.stop.cancelled() => return,
                () = tokio::time::sleep(self.inner.options.reclaim_interval) => {}
            }
            if let Err(e) = self.reclaim(now_millis()).await {
                tracing::warn!(error = %e, "returning lapsed claims failed");
            }
        }
    }

    pub(crate) async fn reclaim(&self, now_ms: i64) -> Result<usize, StoreError> {
        let queues: Vec<String> = lock(&self.inner.joined).iter().cloned().collect();
        let mut moved = 0;
        for queue in queues {
            let n: usize = self
                .inner
                .scripts
                .reclaim
                .key(&queue)
                .key(keys::claimed(&queue))
                .key(keys::claim_owners(&queue))
                .key(keys::claims_idx(&queue))
                .arg(secs(now_ms))
                .arg(RECLAIM_BATCH)
                .invoke_async(&mut self.conn())
                .await?;
            if n > 0 {
                tracing::info!(%queue, returned = n, "returned lapsed claims to the queue");
            }
            moved += n;
        }
        Ok(moved)
    }

    /// Wakes long polls for results any replica writes.
    async fn relay_loop(self, results: Arc<ResultSignal>) {
        let mut backoff = FIRST_BACKOFF;
        loop {
            match self.relay(&results, &mut backoff).await {
                Ok(()) => return,
                Err(e) => {
                    tracing::warn!(error = %e, retry_in = ?backoff, "result notifications lost; reconnecting")
                }
            }
            tokio::select! {
                () = self.inner.stop.cancelled() => return,
                () = tokio::time::sleep(backoff) => {}
            }
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    }

    async fn relay(
        &self,
        results: &ResultSignal,
        backoff: &mut Duration,
    ) -> Result<(), StoreError> {
        let mut pubsub = self.inner.client.get_async_pubsub().await?;
        pubsub.subscribe(keys::RESULTS_CHANNEL).await?;
        *backoff = FIRST_BACKOFF;
        results.all();
        let mut messages = pubsub.on_message();
        loop {
            tokio::select! {
                () = self.inner.stop.cancelled() => return Ok(()),
                message = messages.next() => {
                    let Some(message) = message else {
                        return Err(StoreError::Closed);
                    };
                    match message.get_payload::<String>() {
                        Ok(route) => results.written([route.as_str()]),
                        Err(_) => results.all(),
                    }
                }
            }
        }
    }
}

impl QueueStore for RedisStore {
    fn blobs(&self) -> &BlobStore {
        &self.inner.blobs
    }

    fn ping(&self) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(self.check())
    }

    fn join(&self, queue: &str) -> BoxFuture<'_, Result<(), StoreError>> {
        lock(&self.inner.joined).insert(queue.to_owned());
        Box::pin(async { Ok(()) })
    }

    fn leave(&self, queue: &str) {
        lock(&self.inner.joined).remove(queue);
        lock(&self.inner.peeked).remove(queue);
    }

    fn submit(&self, requests: Vec<NewRequest>) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(self.submit_requests(requests))
    }

    fn peek(
        &self,
        queue: String,
        limit: usize,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<Vec<Peeked>, StoreError>> {
        Box::pin(self.peek_queue(queue, limit, now_ms))
    }

    fn has_pending(&self, queue: String, _now_ms: i64) -> BoxFuture<'_, Result<bool, StoreError>> {
        Box::pin(async move {
            let n: u64 = ::redis::cmd("ZCARD")
                .arg(&queue)
                .query_async(&mut self.conn())
                .await?;
            Ok(n > 0)
        })
    }

    fn admit(
        &self,
        queue: String,
        admissions: Vec<Admission>,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<Vec<Admitted>, StoreError>> {
        Box::pin(self.admit_rows(queue, admissions, now_ms))
    }

    fn apply_outcomes(
        &self,
        outcomes: Arc<[Outcome]>,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<Applied, StoreError>> {
        Box::pin(self.end_claims(outcomes, now_ms))
    }

    fn promote_due_retries(
        &self,
        now_ms: i64,
        limit: usize,
    ) -> BoxFuture<'_, Result<usize, StoreError>> {
        Box::pin(async move {
            Ok(self
                .inner
                .scripts
                .promote
                .key(&self.inner.options.retry_queue)
                .arg(secs(now_ms))
                .arg(limit)
                .invoke_async(&mut self.conn())
                .await?)
        })
    }

    fn open_payload<'a>(
        &'a self,
        envelope: &'a InternalRequest,
    ) -> BoxFuture<'a, Result<Option<PayloadBody>, StoreError>> {
        Box::pin(self.payload(envelope))
    }

    fn is_cancelled(
        &self,
        id: String,
        token: String,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<bool, StoreError>> {
        Box::pin(self.cancelled(id, token, now_ms))
    }

    fn request_status(
        &self,
        id: String,
        token: Option<String>,
    ) -> BoxFuture<'_, Result<RequestStatus, StoreError>> {
        Box::pin(self.status(id, token))
    }

    fn cancel(&self, ids: Vec<String>, now_ms: i64) -> BoxFuture<'_, Result<usize, StoreError>> {
        Box::pin(self.cancel_ids(ids, now_ms))
    }

    fn backlog(
        &self,
        queue: String,
        now_ms: i64,
        bounds_s: Vec<i64>,
    ) -> BoxFuture<'_, Result<Backlog, StoreError>> {
        Box::pin(self.queue_backlog(queue, now_ms.div_euclid(1000), bounds_s))
    }

    fn pop_result(
        &self,
        route: String,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<Option<String>, StoreError>> {
        Box::pin(self.pop(route, now_ms))
    }

    fn claim_result(
        &self,
        route: String,
        owner: String,
        lease_ms: i64,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<Option<ResultClaim>, StoreError>> {
        Box::pin(self.claim(route, owner, lease_ms, now_ms))
    }

    fn renew_result(
        &self,
        route: String,
        claim_id: u64,
        owner: String,
        lease_ms: i64,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<bool, StoreError>> {
        Box::pin(self.renew(route, claim_id, owner, lease_ms, now_ms))
    }

    fn ack_result(
        &self,
        route: String,
        claim_id: u64,
        owner: String,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<AckOutcome, StoreError>> {
        Box::pin(self.ack(route, claim_id, owner, now_ms))
    }

    fn open_result_blob(
        &self,
        key: BlobKey,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<Option<(BlobBody, String)>, StoreError>> {
        Box::pin(self.result_blob(key, now_ms))
    }

    fn result_depth(&self, route: String, now_ms: i64) -> BoxFuture<'_, Result<u64, StoreError>> {
        Box::pin(self.depth(route, now_ms))
    }

    fn kv_get(&self, key: String) -> BoxFuture<'_, Result<Stamped<Option<Vec<u8>>>, StoreError>> {
        Box::pin(async move {
            let mut conn = self.conn();
            let value: Option<Vec<u8>> = ::redis::cmd("GET")
                .arg(keys::kv(&key))
                .query_async(&mut conn)
                .await?;
            let (s, us): (i64, i64) = ::redis::cmd("TIME").query_async(&mut conn).await?;
            Ok(Stamped {
                value,
                now_ms: s * 1000 + us / 1000,
            })
        })
    }

    fn kv_put(&self, key: String, value: Option<Vec<u8>>) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move {
            let cmd = match value {
                Some(v) => {
                    let mut c = ::redis::cmd("SET");
                    c.arg(keys::kv(&key)).arg(v);
                    c
                }
                None => {
                    let mut c = ::redis::cmd("DEL");
                    c.arg(keys::kv(&key));
                    c
                }
            };
            let _: ::redis::Value = cmd.query_async(&mut self.conn()).await?;
            Ok(())
        })
    }

    fn sweep(&self, now_ms: i64) -> BoxFuture<'_, Result<u64, StoreError>> {
        Box::pin(self.sweep_results(now_ms))
    }

    fn collect_orphans(&self, cutoff_ms: i64) -> BoxFuture<'_, Result<usize, StoreError>> {
        Box::pin(self.collect_orphan_blobs(cutoff_ms))
    }

    fn close(&self) -> BoxFuture<'_, Result<(), StoreError>> {
        self.inner.stop.cancel();
        Box::pin(async { Ok(()) })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::api::progress::Progress;
    use crate::api::result::ResultMessage;
    use crate::clock::now_millis;
    use crate::store::Store;
    use crate::store::conformance::conformance_tests;
    use crate::store::queue::{Admission, Admitted, ClaimRef, Outcome, Peeked, RequestStatus};
    use crate::store::redis::test_support::{fixture, open_with, options};
    use crate::store::redis::{RedisOptions, keys};

    conformance_tests!(crate::store::redis::test_support::fixture);

    async fn raw(url: &str) -> ::redis::aio::MultiplexedConnection {
        ::redis::Client::open(url)
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap()
    }

    /// What upstream's Go producer runs to submit (redis_sortedset_producer.go),
    /// with the payload inline or, as llm-d-async#458 writes it, apart.
    async fn go_submit(
        conn: &mut ::redis::aio::MultiplexedConnection,
        id: &str,
        token: &str,
        deadline: i64,
        payload_apart: bool,
    ) {
        let payload = r#"{"model":"m","prompt":"hi","max_tokens":8}"#;
        let (payload_ref, inline) = if payload_apart {
            (
                format!(r#","payload_ref":"request-payload:{id}:{token}""#),
                "null",
            )
        } else {
            (String::new(), payload)
        };
        let member = format!(
            r#"{{"internal":{{"request_token":"{token}","request_queue_name":"request-sortedset","result_queue_name":"results:req:{id}"{payload_ref}}},"request_kind":"redis","data":{{"id":"{id}","created":1,"deadline":{deadline},"payload":{inline},"metadata":{{"userid":"acme"}},"endpoint":"/v1/completions","request_queue_name":"request-sortedset","result_queue_name":"results:req:{id}"}}}}"#
        );
        let mut pipe = ::redis::pipe();
        pipe.atomic()
            .del(keys::cancel(id))
            .ignore()
            .cmd("SET")
            .arg(keys::active(id))
            .arg(token)
            .arg("EX")
            .arg(deadline - now_millis() / 1000)
            .ignore();
        if payload_apart {
            pipe.set(format!("request-payload:{id}:{token}"), payload)
                .ignore();
        }
        pipe.zadd("request-sortedset", member, deadline).ignore();
        let () = pipe.query_async(conn).await.unwrap();
    }

    async fn claim_one(
        store: &Store,
        queue: &str,
        now_ms: i64,
    ) -> (Peeked, ClaimRef, Option<bytes::Bytes>) {
        store.join(queue).await.unwrap();
        let head = store.peek(queue.into(), 1, now_ms).await.unwrap().remove(0);
        let generation = head.envelope.as_ref().unwrap().generation_key();
        let out = store
            .admit(
                queue.into(),
                vec![Admission::Claim {
                    key: head.key,
                    generation,
                }],
                now_ms,
            )
            .await
            .unwrap();
        let Some(Admitted::Claimed { claim, payload }) = out.into_iter().next() else {
            panic!("not claimed")
        };
        (head, claim, payload)
    }

    #[tokio::test]
    async fn serves_what_the_go_producer_submits_and_answers_as_upstream() {
        let Some(f) = fixture().await else { return };
        let mut conn = raw(&f.server.url).await;
        let now = now_millis();
        let deadline = now / 1000 + 600;
        go_submit(&mut conn, "acme:job-1", "9f86d081884c7d65", deadline, false).await;
        go_submit(
            &mut conn,
            "acme:job-2",
            "0a1b2c3d4e5f6071",
            deadline + 1,
            true,
        )
        .await;

        for (id, token) in [
            ("acme:job-1", "9f86d081884c7d65"),
            ("acme:job-2", "0a1b2c3d4e5f6071"),
        ] {
            let (head, claim, payload) = claim_one(&f.store, "request-sortedset", now).await;
            let env = head.envelope.unwrap();
            assert_eq!(env.request.id, id);
            assert_eq!(env.request.metadata["userid"], "acme");
            assert_eq!(
                payload.as_deref(),
                Some(br#"{"model":"m","prompt":"hi","max_tokens":8}"#.as_slice())
            );
            assert_eq!(
                f.store.request_status(id.into(), None).await.unwrap(),
                RequestStatus::InProgress
            );
            let result = ResultMessage::http(&env, 200, br#"{"choices":[]}"#);
            let applied = f
                .store
                .apply_outcomes(
                    vec![Outcome::Finish {
                        claim,
                        envelope: env,
                        result,
                    }]
                    .into(),
                    now,
                )
                .await
                .unwrap();
            assert_eq!(applied.result_routes, [format!("results:req:{id}")]);

            // What the coordinator's async-broker reads: the mailbox head,
            // non-destructively, and whether the request is still active.
            let head: Vec<String> = ::redis::cmd("LRANGE")
                .arg(format!("results:req:{id}"))
                .arg(0)
                .arg(0)
                .query_async(&mut conn)
                .await
                .unwrap();
            let got: serde_json::Value = serde_json::from_str(&head[0]).unwrap();
            assert_eq!(got["id"], id);
            assert_eq!(got["status_code"], 200);
            assert_eq!(got["payload"], r#"{"choices":[]}"#);
            assert_eq!(got["request_token"], token);
            let (active, payload_key, claims): (bool, bool, u64) = ::redis::pipe()
                .exists(keys::active(id))
                .exists(format!("request-payload:{id}:{token}"))
                .hlen(keys::claimed("request-sortedset"))
                .query_async(&mut conn)
                .await
                .unwrap();
            assert!(
                !active,
                "request-active is cleared once the result is written"
            );
            assert!(!payload_key, "a separate payload is deleted with its claim");
            assert_eq!(claims, 0);
        }
    }

    #[tokio::test]
    async fn a_dead_replicas_claim_is_returned_after_its_lease() {
        let lease = RedisOptions {
            claim_lease: Duration::from_secs(30),
            ..options()
        };
        let Some(a) = open_with(lease).await else {
            return;
        };
        let b = crate::store::redis::RedisStore::open(
            &a.server.url,
            a.store.blobs().clone(),
            options(),
            std::sync::Arc::new(crate::store::signal::ResultSignal::default()),
        )
        .await
        .unwrap();
        let now = now_millis();
        let mut req = crate::store::test_support::new_request("a", "q", now / 1000 + 3600);
        req.envelope.routing.request_token = "0a".into();
        a.store.submit(vec![req]).await.unwrap();
        let (_, claim, _) = claim_one(&a.store, "q", now).await;
        // Replica a dies: its claim stays, unrenewed.
        crate::store::QueueStore::close(&a.redis).await.unwrap();
        crate::store::QueueStore::join(&b, "q").await.unwrap();
        assert_eq!(b.reclaim(now + 29_000).await.unwrap(), 0);
        assert_eq!(b.reclaim(now + 30_000).await.unwrap(), 1);
        let store_b: Store = std::sync::Arc::new(b);
        let (head, _, payload) = claim_one(&store_b, "q", now + 30_000).await;
        assert_eq!(head.envelope.unwrap().request.id, "a");
        assert_eq!(payload.as_deref(), Some(br#"{"prompt":"a"}"#.as_slice()));
        let fenced = a
            .store
            .apply_outcomes(vec![Outcome::Release { claim }].into(), now + 30_000)
            .await
            .unwrap();
        assert_eq!(
            fenced.fenced, 1,
            "the dead replica's late outcome is fenced"
        );
    }

    #[tokio::test]
    async fn progress_survives_a_go_dispatcher_rewriting_the_envelope() {
        let Some(f) = fixture().await else { return };
        let mut conn = raw(&f.server.url).await;
        let now = now_millis();
        let mut req = crate::store::test_support::new_request("a", "q", now / 1000 + 3600);
        req.envelope.routing.request_token = "0a".into();
        f.store.submit(vec![req]).await.unwrap();
        let (head, claim, _) = claim_one(&f.store, "q", now).await;
        let mut env = head.envelope.unwrap();
        let progress = Progress {
            prompt_token_ids: vec![1, 2, 3],
            token_ids: vec![7, 8],
            finish_reason: None,
            resumes: 1,
        };
        env.progress = Some(progress.clone());
        env.routing.retry_count = 1;
        f.store
            .apply_outcomes(
                vec![Outcome::Retry {
                    claim,
                    envelope: env,
                    due_ms: now,
                }]
                .into(),
                now,
            )
            .await
            .unwrap();
        let claims: u64 = ::redis::cmd("HLEN")
            .arg(keys::claimed("q"))
            .query_async(&mut conn)
            .await
            .unwrap();
        assert_eq!(claims, 0, "a retry ends its claim as it parks the request");

        // A Go dispatcher sharing the retry set re-serializes the envelope
        // from its typed structs, keeping only the fields it knows.
        let parked: Vec<String> = ::redis::cmd("ZRANGE")
            .arg("retry-sortedset")
            .arg(0)
            .arg(-1)
            .query_async(&mut conn)
            .await
            .unwrap();
        assert!(!parked[0].contains("progress"), "{}", parked[0]);
        let mut member: serde_json::Value = serde_json::from_str(&parked[0]).unwrap();
        member["internal"]["queue_id"] = "go-dispatcher".into();
        let () = ::redis::pipe()
            .zrem("retry-sortedset", &parked[0])
            .ignore()
            .zadd("retry-sortedset", member.to_string(), now / 1000)
            .ignore()
            .query_async(&mut conn)
            .await
            .unwrap();

        assert_eq!(f.store.promote_due_retries(now, 10).await.unwrap(), 1);
        let (head, _, _) = claim_one(&f.store, "q", now).await;
        let env = head.envelope.unwrap();
        assert_eq!(env.routing.queue_id, "go-dispatcher");
        assert_eq!(env.routing.retry_count, 1);
        assert_eq!(env.progress, Some(progress));
    }
}
