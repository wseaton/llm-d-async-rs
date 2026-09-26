//! Durable queue state in Postgres, shared by any number of replicas.
//!
//! Replicas split each queue by leasing partitions (see [`partitions`]). A
//! claim is held by the process that made it and fenced by an attempt number
//! drawn fresh for every dispatch, so an outcome from a process whose claims
//! were returned, or from an older dispatch, changes nothing. Admission counters live here too (see [`counters`]), so quota
//! limits hold across replicas.

/// The database's clock in Unix milliseconds, as SQL, for `concat!`.
macro_rules! db_now_ms {
    () => {
        "(extract(epoch FROM clock_timestamp()) * 1000)::bigint"
    };
}

pub(crate) mod cached;
mod cancel_checks;
pub mod connect;
pub mod counters;
mod listen;
pub mod partitions;
mod requests;
mod results;
mod schema;

#[cfg(test)]
pub(crate) mod test_support;

use std::sync::atomic::AtomicI64;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use deadpool_postgres::Pool;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::api::request::InternalRequest;
use crate::boxed::BoxFuture;
use crate::store::blob::key::BlobKey;
use crate::store::blob::{BlobBody, BlobStore};
use crate::store::error::StoreError;
use crate::store::postgres::cached::Cached;
use crate::store::postgres::cancel_checks::CancelChecks;
use crate::store::postgres::connect::Database;
use crate::store::postgres::counters::PgCounters;
use crate::store::postgres::partitions::State;
use crate::store::queue::{
    AckOutcome, Admission, Admitted, Applied, Backlog, NewRequest, Outcome, PayloadBody, Peeked,
    RequestStatus, ResultClaim,
};
use crate::store::signal::ResultSignal;
use crate::store::{QueueStore, Stamped};

/// The database clock, which every replica times leases by.
pub(crate) const DB_NOW_MS: &str = db_now_ms!();

/// Notified in every transaction that writes a result.
pub(crate) const RESULTS_CHANNEL: &str = "lda_results";

#[derive(Debug, Clone)]
pub struct PostgresOptions {
    /// How long a partition lease, a process's claims, or a quota holder
    /// last without a heartbeat.
    pub lease_ttl: Duration,
    pub result_blob_retention: Duration,
}

pub(crate) struct Inner {
    pool: Pool,
    /// Partition leases, apart from the data path.
    control: Pool,
    blobs: BlobStore,
    /// This process's name in leases.
    owner: String,
    /// This process's row in `lda_processes`, which its claims name.
    process: AtomicI64,
    lease_ttl: Duration,
    result_blob_retention_ms: i64,
    state: Mutex<State>,
    /// Wakes the balancer when a queue is joined or left.
    wake: Notify,
    results: Arc<ResultSignal>,
    cancel_checks: CancelChecks,
}

pub struct PgStore {
    inner: Arc<Inner>,
    counters: PgCounters,
    stop: CancellationToken,
    tasks: TaskTracker,
}

impl PgStore {
    /// Creates the schema if needed and starts the partition balancer and
    /// the result listener. `results` is notified whenever any replica
    /// writes a result.
    pub async fn open(
        db: Database,
        blobs: BlobStore,
        options: &PostgresOptions,
        results: Arc<ResultSignal>,
    ) -> Result<Self, StoreError> {
        schema::migrate(&db.pool).await?;
        let stop = CancellationToken::new();
        let tasks = TaskTracker::new();
        let owner = format!("{:032x}", rand::random::<u128>());
        let ttl_ms = i64::try_from(options.lease_ttl.as_millis()).unwrap_or(i64::MAX);
        let process = partitions::register(&db.control.get().await?, ttl_ms).await?;
        let inner = Arc::new(Inner {
            pool: db.pool.clone(),
            control: db.control.clone(),
            blobs,
            owner,
            process: AtomicI64::new(process),
            lease_ttl: options.lease_ttl,
            result_blob_retention_ms: i64::try_from(options.result_blob_retention.as_millis())
                .unwrap_or(i64::MAX),
            state: Mutex::new(State::default()),
            wake: Notify::new(),
            results: Arc::clone(&results),
            cancel_checks: CancelChecks::start(db.pool.clone(), stop.clone()),
        });
        tasks.spawn(balance(Arc::clone(&inner), stop.clone()));
        tasks.spawn(listen::run(db.clone(), results, stop.clone()));
        let counters = PgCounters::start(db.pool.clone(), options.lease_ttl);
        tracing::info!(owner = %inner.owner, process, "postgres store opened");
        Ok(Self {
            inner,
            counters,
            stop,
            tasks,
        })
    }

    /// Admission counters shared through this database.
    pub fn counters(&self) -> PgCounters {
        self.counters.clone()
    }

    pub fn owner(&self) -> &str {
        &self.inner.owner
    }
}

/// Heartbeats this process and every queue it takes part in, a third of a
/// lease apart and at least once a second, and at once when a queue is
/// joined or left.
async fn balance(inner: Arc<Inner>, stop: CancellationToken) {
    let period = (inner.lease_ttl / 3).clamp(Duration::from_millis(100), Duration::from_secs(1));
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            () = stop.cancelled() => return,
            _ = ticker.tick() => {}
            () = inner.wake.notified() => {}
        }
        if let Err(e) = inner.beat().await {
            tracing::warn!(error = %e, "process heartbeat failed");
        }
        let mut beats = tokio::task::JoinSet::new();
        for queue in inner.queue_names() {
            let inner = Arc::clone(&inner);
            beats.spawn(async move {
                if let Err(e) = inner.rebalance(&queue).await {
                    tracing::warn!(%queue, error = %e, "partition heartbeat failed");
                }
            });
        }
        while beats.join_next().await.is_some() {}
    }
}

impl QueueStore for PgStore {
    fn blobs(&self) -> &BlobStore {
        &self.inner.blobs
    }

    fn ping(&self) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move {
            let client = self.inner.pool.get().await?;
            client.execute_cached("SELECT 1", &[]).await?;
            Ok(())
        })
    }

    fn join(&self, queue: &str) -> BoxFuture<'_, Result<(), StoreError>> {
        let queue = queue.to_owned();
        Box::pin(async move {
            self.inner.want(&queue)?;
            self.inner.rebalance(&queue).await
        })
    }

    fn leave(&self, queue: &str) {
        self.inner.unwant(queue);
    }

    fn submit(&self, requests: Vec<NewRequest>) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(self.inner.submit(requests))
    }

    fn peek(
        &self,
        queue: String,
        limit: usize,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<Vec<Peeked>, StoreError>> {
        Box::pin(self.inner.peek(queue, limit, now_ms))
    }

    fn has_pending(&self, queue: String, now_ms: i64) -> BoxFuture<'_, Result<bool, StoreError>> {
        Box::pin(self.inner.has_pending(queue, now_ms))
    }

    fn admit(
        &self,
        queue: String,
        admissions: Vec<Admission>,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<Vec<Admitted>, StoreError>> {
        Box::pin(self.inner.admit(queue, admissions, now_ms))
    }

    fn apply_outcomes(
        &self,
        outcomes: Arc<[Outcome]>,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<Applied, StoreError>> {
        Box::pin(self.inner.apply_outcomes(outcomes, now_ms))
    }

    fn promote_due_retries(
        &self,
        _now_ms: i64,
        _limit: usize,
    ) -> BoxFuture<'_, Result<usize, StoreError>> {
        Box::pin(async { Ok(0) })
    }

    fn open_payload<'a>(
        &'a self,
        envelope: &'a InternalRequest,
    ) -> BoxFuture<'a, Result<Option<PayloadBody>, StoreError>> {
        Box::pin(self.inner.open_payload(envelope))
    }

    fn is_cancelled(
        &self,
        id: String,
        token: String,
        _now_ms: i64,
    ) -> BoxFuture<'_, Result<bool, StoreError>> {
        Box::pin(self.inner.cancel_checks.is_cancelled(id, token))
    }

    fn request_status(
        &self,
        id: String,
        token: Option<String>,
    ) -> BoxFuture<'_, Result<RequestStatus, StoreError>> {
        Box::pin(self.inner.status(id, token))
    }

    fn cancel(&self, ids: Vec<String>, now_ms: i64) -> BoxFuture<'_, Result<usize, StoreError>> {
        Box::pin(self.inner.cancel(ids, now_ms))
    }

    fn backlog(
        &self,
        queue: String,
        now_ms: i64,
        bounds_s: Vec<i64>,
    ) -> BoxFuture<'_, Result<Backlog, StoreError>> {
        Box::pin(self.inner.backlog(queue, now_ms, bounds_s))
    }

    fn pop_result(
        &self,
        route: String,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<Option<String>, StoreError>> {
        Box::pin(self.inner.pop_result(route, now_ms))
    }

    fn claim_result(
        &self,
        route: String,
        owner: String,
        lease_ms: i64,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<Option<ResultClaim>, StoreError>> {
        Box::pin(self.inner.claim_result(route, owner, lease_ms, now_ms))
    }

    fn renew_result(
        &self,
        route: String,
        claim_id: u64,
        owner: String,
        lease_ms: i64,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<bool, StoreError>> {
        Box::pin(
            self.inner
                .renew_result(route, claim_id, owner, lease_ms, now_ms),
        )
    }

    fn ack_result(
        &self,
        route: String,
        claim_id: u64,
        owner: String,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<AckOutcome, StoreError>> {
        Box::pin(self.inner.ack_result(route, claim_id, owner, now_ms))
    }

    fn open_result_blob(
        &self,
        key: BlobKey,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<Option<(BlobBody, String)>, StoreError>> {
        Box::pin(self.inner.open_result_blob(key, now_ms))
    }

    fn result_depth(&self, route: String, now_ms: i64) -> BoxFuture<'_, Result<u64, StoreError>> {
        Box::pin(self.inner.result_depth(route, now_ms))
    }

    fn kv_get(&self, key: String) -> BoxFuture<'_, Result<Stamped<Option<Vec<u8>>>, StoreError>> {
        Box::pin(self.inner.kv_get(key))
    }

    fn kv_put(&self, key: String, value: Option<Vec<u8>>) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(self.inner.kv_put(key, value))
    }

    fn sweep(&self, now_ms: i64) -> BoxFuture<'_, Result<u64, StoreError>> {
        Box::pin(self.inner.sweep(now_ms))
    }

    fn collect_orphans(&self, cutoff_ms: i64) -> BoxFuture<'_, Result<usize, StoreError>> {
        Box::pin(self.inner.collect_orphans(cutoff_ms))
    }

    fn close(&self) -> BoxFuture<'_, Result<(), StoreError>> {
        Box::pin(async move {
            self.stop.cancel();
            self.tasks.close();
            self.tasks.wait().await;
            self.counters.close().await;
            let mut first_error = None;
            for queue in self.inner.queue_names() {
                if let Err(e) = self.inner.leave_queue(&queue).await {
                    tracing::warn!(%queue, error = %e, "failed to release partitions");
                    first_error.get_or_insert(e);
                }
            }
            if let Err(e) = self.inner.retire().await {
                tracing::warn!(error = %e, "failed to return this process's claims");
                first_error.get_or_insert(e);
            }
            first_error.map_or(Ok(()), Err)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use crate::api::result::ResultMessage;
    use crate::store::Store;
    use crate::store::conformance::{NOW_MS, conformance_tests};
    use crate::store::postgres::partitions::PARTITIONS;
    use crate::store::postgres::test_support::{Fixture, chunk_blobs, replica, schema_url};
    use crate::store::queue::{Admission, Admitted, ClaimRef, Outcome, Peeked};
    use crate::store::test_support::new_request;

    conformance_tests!(crate::store::postgres::test_support::fixture);

    const QUEUE: &str = "q";
    const SHORT_TTL: Duration = Duration::from_millis(900);

    async fn pair(ttl: Duration) -> Option<(Fixture, Fixture)> {
        let url = schema_url().await?;
        let a = replica(&url, chunk_blobs, ttl).await;
        let b = replica(&url, chunk_blobs, ttl).await;
        Some((a, b))
    }

    async fn owners(f: &Fixture) -> HashMap<String, i64> {
        f.db.pool
            .get()
            .await
            .unwrap()
            .query(
                "SELECT owner, count(*) FROM lda_partitions WHERE queue = $1 GROUP BY owner",
                &[&QUEUE],
            )
            .await
            .unwrap()
            .iter()
            .map(|r| (r.get(0), r.get(1)))
            .collect()
    }

    async fn claim_all(store: &Store, limit: usize) -> Vec<(Peeked, ClaimRef)> {
        let peeked = store.peek(QUEUE.into(), limit, NOW_MS).await.unwrap();
        let admissions = peeked
            .iter()
            .map(|p| Admission::Claim {
                key: p.key,
                generation: p.envelope.as_ref().unwrap().generation_key(),
            })
            .collect();
        let admitted = store.admit(QUEUE.into(), admissions, NOW_MS).await.unwrap();
        peeked
            .into_iter()
            .zip(admitted)
            .filter_map(|(p, a)| match a {
                Admitted::Claimed { claim, .. } => Some((p, claim)),
                _ => None,
            })
            .collect()
    }

    async fn submit(store: &Store, n: usize) {
        let reqs = (0..n)
            .map(|i| new_request(&format!("r{i:03}"), QUEUE, 10_000 + i as i64))
            .collect();
        store.submit(reqs).await.unwrap();
    }

    /// Rebalances both replicas until each holds its share.
    async fn settle(a: &Fixture, b: &Fixture) {
        for _ in 0..20 {
            a.pg.inner.rebalance(QUEUE).await.unwrap();
            b.pg.inner.rebalance(QUEUE).await.unwrap();
            let held = owners(a).await;
            if held.get(a.pg.owner()) == Some(&i64::from(PARTITIONS / 2))
                && held.get(b.pg.owner()) == Some(&i64::from(PARTITIONS / 2))
            {
                return;
            }
        }
        panic!("replicas never split the queue: {:?}", owners(a).await);
    }

    #[tokio::test]
    async fn one_replica_takes_every_partition() {
        let Some(f) = crate::store::postgres::test_support::fixture().await else {
            return;
        };
        f.store.join(QUEUE).await.unwrap();
        assert_eq!(
            owners(&f).await,
            HashMap::from([(f.pg.owner().to_owned(), i64::from(PARTITIONS))])
        );
    }

    #[tokio::test]
    async fn replicas_split_a_queue_and_never_share_a_request() {
        let Some((a, b)) = pair(Duration::from_secs(30)).await else {
            return;
        };
        a.store.join(QUEUE).await.unwrap();
        b.store.join(QUEUE).await.unwrap();
        settle(&a, &b).await;
        submit(&a.store, 200).await;
        let from_a = claim_all(&a.store, 500).await;
        let from_b = claim_all(&b.store, 500).await;
        assert!(!from_a.is_empty() && !from_b.is_empty());
        let mut ids: Vec<String> = from_a
            .iter()
            .chain(&from_b)
            .map(|(p, _)| p.envelope.as_ref().unwrap().request.id.clone())
            .collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 200, "every request claimed exactly once");
        assert!(claim_all(&a.store, 500).await.is_empty());
    }

    #[tokio::test]
    async fn a_dead_replicas_claims_return_after_its_lease_and_its_outcomes_are_fenced() {
        let Some((a, b)) = pair(SHORT_TTL).await else {
            return;
        };
        a.store.join(QUEUE).await.unwrap();
        submit(&a.store, 20).await;
        let held = claim_all(&a.store, 20).await;
        assert_eq!(held.len(), 20);

        // `a` dies: no heartbeat, no goodbye.
        a.pg.stop.cancel();
        a.pg.tasks.close();
        a.pg.tasks.wait().await;

        b.store.join(QUEUE).await.unwrap();
        let mut recovered = Vec::new();
        for _ in 0..50 {
            b.pg.inner.beat().await.unwrap();
            b.pg.inner.rebalance(QUEUE).await.unwrap();
            recovered.extend(claim_all(&b.store, 20).await);
            if recovered.len() == 20 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(recovered.len(), 20, "the survivor redelivers every claim");

        let (peeked, claim) = &held[0];
        let env = peeked.envelope.clone().unwrap();
        let applied = a
            .store
            .apply_outcomes(
                vec![Outcome::Finish {
                    claim: claim.clone(),
                    result: ResultMessage::http(&env, 200, b"late"),
                    envelope: env,
                }]
                .into(),
                NOW_MS,
            )
            .await
            .unwrap();
        assert_eq!(applied.fenced, 1);
        assert_eq!(applied.result_routes.len(), 0);

        let (peeked, claim) = recovered
            .iter()
            .find(|(p, _)| p.key == held[0].0.key)
            .unwrap();
        assert_ne!(claim.claim_id, held[0].1.claim_id);
        let env = peeked.envelope.clone().unwrap();
        let applied = b
            .store
            .apply_outcomes(
                vec![Outcome::Finish {
                    claim: claim.clone(),
                    result: ResultMessage::http(&env, 200, b"ok"),
                    envelope: env,
                }]
                .into(),
                NOW_MS,
            )
            .await
            .unwrap();
        assert_eq!(applied.result_routes.len(), 1);
    }

    #[tokio::test]
    async fn closing_hands_every_partition_over_at_once() {
        let Some((a, b)) = pair(Duration::from_secs(30)).await else {
            return;
        };
        a.store.join(QUEUE).await.unwrap();
        b.store.join(QUEUE).await.unwrap();
        settle(&a, &b).await;
        a.store.leave(QUEUE);
        a.store.close().await.unwrap();
        b.pg.inner.rebalance(QUEUE).await.unwrap();
        assert_eq!(
            owners(&b).await,
            HashMap::from([(b.pg.owner().to_owned(), i64::from(PARTITIONS))])
        );
    }

    #[tokio::test]
    async fn joining_splits_partitions_at_once_while_claims_are_in_flight() {
        let Some((a, b)) = pair(Duration::from_secs(30)).await else {
            return;
        };
        a.store.join(QUEUE).await.unwrap();
        submit(&a.store, 100).await;
        assert_eq!(claim_all(&a.store, 100).await.len(), 100);
        b.store.join(QUEUE).await.unwrap();
        a.pg.inner.rebalance(QUEUE).await.unwrap();
        b.pg.inner.rebalance(QUEUE).await.unwrap();
        let half = i64::from(PARTITIONS / 2);
        assert_eq!(
            owners(&a).await,
            HashMap::from([
                (a.pg.owner().to_owned(), half),
                (b.pg.owner().to_owned(), half)
            ]),
            "the split does not wait for claims"
        );
    }

    #[tokio::test]
    async fn a_handoff_leaves_claims_with_their_process() {
        let Some((a, b)) = pair(Duration::from_secs(30)).await else {
            return;
        };
        a.store.join(QUEUE).await.unwrap();
        submit(&a.store, 10).await;
        let (peeked, claim) = claim_all(&a.store, 1).await.remove(0);
        b.store.join(QUEUE).await.unwrap();
        a.store.leave(QUEUE);
        a.pg.inner.rebalance(QUEUE).await.unwrap();
        b.pg.inner.rebalance(QUEUE).await.unwrap();
        assert_eq!(
            owners(&b).await,
            HashMap::from([(b.pg.owner().to_owned(), i64::from(PARTITIONS))]),
            "the leaver hands every partition over with a claim in flight"
        );
        assert!(
            a.pg.inner.queue_names().contains(&QUEUE.to_owned()),
            "the leaver stays until its claim ends"
        );

        let taken = claim_all(&b.store, 10).await;
        assert_eq!(
            taken.len(),
            9,
            "the new owner takes the rest, not the claim"
        );
        assert!(taken.iter().all(|(p, _)| p.key != peeked.key));

        let env = peeked.envelope.unwrap();
        let applied = a
            .store
            .apply_outcomes(
                vec![Outcome::Finish {
                    claim,
                    result: ResultMessage::http(&env, 200, b"{}"),
                    envelope: env,
                }]
                .into(),
                NOW_MS,
            )
            .await
            .unwrap();
        assert_eq!(
            applied.result_routes.len(),
            1,
            "the leaver finishes its claim"
        );
        a.pg.inner.rebalance(QUEUE).await.unwrap();
        assert!(a.pg.inner.queue_names().is_empty());
        assert!(claim_all(&b.store, 10).await.is_empty());
    }

    #[tokio::test]
    async fn a_lapsed_process_that_claims_as_it_lapses_is_returned_again() {
        let Some((a, b)) = pair(SHORT_TTL).await else {
            return;
        };
        a.store.join(QUEUE).await.unwrap();
        submit(&a.store, 2).await;
        let first = claim_all(&a.store, 1).await;
        a.pg.stop.cancel();
        a.pg.tasks.close();
        a.pg.tasks.wait().await;
        tokio::time::sleep(SHORT_TTL + Duration::from_millis(200)).await;
        b.pg.inner.beat().await.unwrap();
        assert_eq!(pending_ids(&b).await.len(), 2);

        // A claim that commits after its process lapsed, as a slow statement
        // would, is returned by the next heartbeat.
        b.db.pool
            .get()
            .await
            .unwrap()
            .execute(
                "UPDATE lda_requests SET claimed_by = $1, dispatch_attempt = nextval('lda_dispatch_attempts')",
                &[&a.pg.inner.process()],
            )
            .await
            .unwrap();
        b.pg.inner.beat().await.unwrap();
        assert_eq!(pending_ids(&b).await.len(), 2);
        assert_eq!(first.len(), 1);
    }

    #[tokio::test]
    async fn sweeping_forgets_lapsed_processes_and_returns_their_claims() {
        let Some(f) = crate::store::postgres::test_support::fixture().await else {
            return;
        };
        f.store.join(QUEUE).await.unwrap();
        submit(&f.store, 1).await;
        let client = f.db.pool.get().await.unwrap();
        client
            .batch_execute(
                "INSERT INTO lda_processes (expires_ms) VALUES (0);
                 UPDATE lda_requests SET claimed_by = 9223372036854775807;",
            )
            .await
            .unwrap();
        f.store.sweep(NOW_MS).await.unwrap();
        let processes: i64 = client
            .query_one("SELECT count(*) FROM lda_processes", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(processes, 1, "only the live process is left");
        assert_eq!(
            pending_ids(&f).await,
            ["r000"],
            "a claim naming no process is returned"
        );
    }

    #[tokio::test]
    async fn a_process_forgotten_by_the_sweep_registers_again_and_drops_its_claims() {
        let Some(f) = crate::store::postgres::test_support::fixture().await else {
            return;
        };
        f.store.join(QUEUE).await.unwrap();
        submit(&f.store, 1).await;
        let (peeked, claim) = claim_all(&f.store, 1).await.remove(0);
        let old = f.pg.inner.process();
        f.db.pool
            .get()
            .await
            .unwrap()
            .execute("DELETE FROM lda_processes WHERE id = $1", &[&old])
            .await
            .unwrap();
        f.store.sweep(NOW_MS).await.unwrap();
        f.pg.inner.beat().await.unwrap();
        assert_ne!(f.pg.inner.process(), old);
        assert!(f.pg.inner.state.lock().unwrap().claims.is_empty());
        let env = peeked.envelope.unwrap();
        let applied = f
            .store
            .apply_outcomes(
                vec![Outcome::Finish {
                    claim,
                    result: ResultMessage::http(&env, 200, b"{}"),
                    envelope: env,
                }]
                .into(),
                NOW_MS,
            )
            .await
            .unwrap();
        assert_eq!(applied.fenced, 1, "the old process's outcome is fenced");
        assert_eq!(claim_all(&f.store, 1).await.len(), 1);
    }

    #[tokio::test]
    async fn closing_returns_the_claims_left_and_removes_the_process() {
        let Some((a, b)) = pair(Duration::from_secs(30)).await else {
            return;
        };
        a.store.join(QUEUE).await.unwrap();
        submit(&a.store, 1).await;
        assert_eq!(claim_all(&a.store, 1).await.len(), 1);
        a.store.close().await.unwrap();
        assert_eq!(pending_ids(&b).await, ["r000"]);
        let alive: bool =
            b.db.pool
                .get()
                .await
                .unwrap()
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM lda_processes WHERE id = $1)",
                    &[&a.pg.inner.process()],
                )
                .await
                .unwrap()
                .get(0);
        assert!(!alive);
    }

    async fn pending_ids(f: &Fixture) -> Vec<String> {
        f.db.pool
            .get()
            .await
            .unwrap()
            .query(
                "SELECT id FROM lda_requests WHERE claimed_by = 0 ORDER BY id",
                &[],
            )
            .await
            .unwrap()
            .iter()
            .map(|r| r.get(0))
            .collect()
    }

    #[tokio::test]
    async fn a_claim_whose_reply_was_lost_is_returned() {
        let Some(f) = crate::store::postgres::test_support::fixture().await else {
            return;
        };
        f.store.join(QUEUE).await.unwrap();
        submit(&f.store, 1).await;
        f.db.pool
            .get()
            .await
            .unwrap()
            .execute(
                "UPDATE lda_requests
                 SET claimed_by = $1, dispatch_attempt = nextval('lda_dispatch_attempts')",
                &[&f.pg.inner.process()],
            )
            .await
            .unwrap();
        assert!(!f.store.has_pending(QUEUE.into(), NOW_MS).await.unwrap());
        f.pg.inner
            .state
            .lock()
            .unwrap()
            .queues
            .get_mut(QUEUE)
            .unwrap()
            .reconcile = true;
        f.pg.inner.rebalance(QUEUE).await.unwrap();
        assert!(f.store.has_pending(QUEUE.into(), NOW_MS).await.unwrap());
    }

    #[tokio::test]
    async fn cancel_and_results_cross_replicas() {
        let Some((a, b)) = pair(Duration::from_secs(30)).await else {
            return;
        };
        a.store.join(QUEUE).await.unwrap();
        submit(&a.store, 1).await;
        let (peeked, claim) = claim_all(&a.store, 1).await.remove(0);
        let env = peeked.envelope.unwrap();
        assert_eq!(
            b.store
                .cancel(vec![env.request.id.clone()], NOW_MS)
                .await
                .unwrap(),
            1
        );
        assert!(
            a.store
                .is_cancelled(
                    env.request.id.clone(),
                    env.routing.request_token.clone(),
                    NOW_MS
                )
                .await
                .unwrap()
        );

        let watch = b.results.watch("results");
        let other = b.results.watch("elsewhere");
        let woken = watch.notify().notified();
        let not_woken = other.notify().notified();
        tokio::pin!(woken, not_woken);
        not_woken.as_mut().enable();
        woken.as_mut().enable();
        a.store
            .apply_outcomes(
                vec![Outcome::Finish {
                    claim,
                    result: ResultMessage::cancelled(&env),
                    envelope: env,
                }]
                .into(),
                NOW_MS,
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), woken)
            .await
            .expect("the other replica hears about the result");
        assert!(
            tokio::time::timeout(Duration::from_millis(200), not_woken)
                .await
                .is_err(),
            "a write to one route wakes no other"
        );
        let claimed = b
            .store
            .claim_result("results".into(), "c".into(), 1_000, NOW_MS)
            .await
            .unwrap()
            .unwrap();
        assert!(claimed.body.contains("\"r000\""));
    }

    #[tokio::test]
    async fn cancellation_checks_coalesce_under_load() {
        let Some(f) = crate::store::postgres::test_support::fixture().await else {
            return;
        };
        submit(&f.store, 50).await;
        f.store
            .cancel((0..25).map(|i| format!("r{i:03}")).collect(), NOW_MS)
            .await
            .unwrap();
        let store = Arc::clone(&f.store);
        let checks = (0..300).map(|i| {
            let store = Arc::clone(&store);
            tokio::spawn(async move {
                let id = format!("r{:03}", i % 50);
                let token: String = id.bytes().map(|b| format!("{b:02x}")).collect();
                (i % 50, store.is_cancelled(id, token, NOW_MS).await.unwrap())
            })
        });
        for check in checks {
            let (i, cancelled) = check.await.unwrap();
            assert_eq!(cancelled, i < 25, "request {i}");
        }
    }

    async fn fixture_with(connections: usize) -> Option<Fixture> {
        let url = schema_url().await?;
        let mut f = replica(&url, chunk_blobs, Duration::from_secs(30)).await;
        let db = crate::store::postgres::connect::Database::connect(&url, connections, None)
            .await
            .unwrap();
        let pg = Arc::new(super_open(db.clone(), chunk_blobs(&db), Arc::clone(&f.results)).await);
        f.store = pg.clone();
        f.pg = pg;
        f.db = db;
        Some(f)
    }

    async fn super_open(
        db: crate::store::postgres::connect::Database,
        blobs: crate::store::blob::BlobStore,
        results: Arc<crate::store::signal::ResultSignal>,
    ) -> crate::store::postgres::PgStore {
        crate::store::postgres::PgStore::open(
            db,
            blobs,
            &crate::store::postgres::test_support::options(Duration::from_secs(30)),
            results,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn ending_a_claim_with_a_blob_needs_one_connection() {
        let Some(f) = fixture_with(1).await else {
            return;
        };
        let mut sink =
            crate::store::staging::PayloadSink::new(f.store.blobs().clone(), "0a", 4, 1 << 20);
        sink.push(b"0123456789").await.unwrap();
        let payload = sink.finish().await.unwrap();
        let mut req = new_request("a", QUEUE, 10_000);
        req.envelope.routing.request_token = "0a".into();
        req.envelope.payload = payload.info("audio/wav");
        req.payload = payload;
        f.store.submit(vec![req]).await.unwrap();
        f.store.join(QUEUE).await.unwrap();
        let (peeked, claim) = claim_all(&f.store, 1).await.remove(0);
        let env = peeked.envelope.unwrap();
        let applied = tokio::time::timeout(
            Duration::from_secs(5),
            f.store.apply_outcomes(
                vec![Outcome::Finish {
                    claim,
                    result: ResultMessage::http(&env, 200, b"{}"),
                    envelope: env,
                }]
                .into(),
                NOW_MS,
            ),
        )
        .await
        .expect("a finish waited on its own connection")
        .unwrap();
        assert_eq!(applied.result_routes.len(), 1);
        let key = crate::store::blob::key::BlobKey::request("0a").unwrap();
        assert!(f.store.blobs().open(&key).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn a_stale_and_a_current_finish_in_one_batch_write_one_result() {
        let Some(f) = crate::store::postgres::test_support::fixture().await else {
            return;
        };
        f.store.join(QUEUE).await.unwrap();
        submit(&f.store, 1).await;
        let (peeked, stale) = claim_all(&f.store, 1).await.remove(0);
        f.store
            .apply_outcomes(
                vec![Outcome::Release {
                    claim: stale.clone(),
                }]
                .into(),
                NOW_MS,
            )
            .await
            .unwrap();
        let (_, current) = claim_all(&f.store, 1).await.remove(0);
        let env = peeked.envelope.unwrap();
        let finish = |claim: ClaimRef| Outcome::Finish {
            claim,
            result: ResultMessage::http(&env, 200, b"{}"),
            envelope: env.clone(),
        };
        let applied = f
            .store
            .apply_outcomes(vec![finish(stale), finish(current)].into(), NOW_MS)
            .await
            .unwrap();
        assert_eq!((applied.result_routes.len(), applied.fenced), (1, 1));
        assert_eq!(
            f.store
                .result_depth("results".into(), NOW_MS)
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn cancel_takes_any_deadline_and_only_the_newest_generation() {
        let Some(f) = crate::store::postgres::test_support::fixture().await else {
            return;
        };
        let far = new_request("far", QUEUE, i64::MAX);
        let old = new_request("dup", QUEUE, 10_000);
        let mut new = new_request("dup", QUEUE, 10_000);
        new.envelope.routing.request_token = "6e6577".into();
        f.store.submit(vec![far, old, new]).await.unwrap();
        assert_eq!(
            f.store
                .cancel(vec!["far".into(), "dup".into()], NOW_MS)
                .await
                .unwrap(),
            2
        );
        let dup_token: String = "dup".bytes().map(|b| format!("{b:02x}")).collect();
        assert!(
            !f.store
                .is_cancelled("dup".into(), dup_token, NOW_MS)
                .await
                .unwrap()
        );
        assert!(
            f.store
                .is_cancelled("dup".into(), "6e6577".into(), NOW_MS)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn lost_claims_are_found_without_an_admit_error() {
        let Some(f) = crate::store::postgres::test_support::fixture().await else {
            return;
        };
        f.store.join(QUEUE).await.unwrap();
        submit(&f.store, 1).await;
        f.db.pool
            .get()
            .await
            .unwrap()
            .execute(
                "UPDATE lda_requests
                 SET claimed_by = $1, dispatch_attempt = nextval('lda_dispatch_attempts')",
                &[&f.pg.inner.process()],
            )
            .await
            .unwrap();
        for _ in 0..10 {
            f.pg.inner.rebalance(QUEUE).await.unwrap();
        }
        assert!(f.store.has_pending(QUEUE.into(), NOW_MS).await.unwrap());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sweeping_drops_idle_counters_and_keeps_limits_exact() {
        use crate::gate::admission::counters::{BucketSpec, Counters};
        let Some(f) = crate::store::postgres::test_support::fixture().await else {
            return;
        };
        let counters = f.pg.counters();
        let slot = counters.acquire_slot("k".into(), 1).await.unwrap().unwrap();
        assert!(
            counters
                .admit("r".into(), 5, Duration::from_millis(100))
                .await
                .unwrap()
        );
        let expired = BucketSpec {
            rate: 1.0,
            capacity: 1.0,
            expires_ms: crate::clock::now_millis() - 1,
        };
        assert!(counters.take_token("b".into(), expired).await.unwrap());
        tokio::time::sleep(Duration::from_millis(150)).await;
        f.store.sweep(NOW_MS).await.unwrap();
        assert!(
            counters
                .acquire_slot("k".into(), 1)
                .await
                .unwrap()
                .is_none(),
            "a held slot survives the sweep"
        );
        drop(slot);
        let count = |table: &'static str| {
            let pool = f.db.pool.clone();
            async move {
                pool.get()
                    .await
                    .unwrap()
                    .query_one(&format!("SELECT count(*) FROM {table}"), &[])
                    .await
                    .unwrap()
                    .get::<_, i64>(0)
            }
        };
        for _ in 0..100 {
            f.store.sweep(NOW_MS).await.unwrap();
            if count("lda_quota_keys").await == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        for table in [
            "lda_quota_keys",
            "lda_quota_windows",
            "lda_quota_admits",
            "lda_rate_buckets",
        ] {
            assert_eq!(count(table).await, 0, "{table}");
        }
        let a = counters.acquire_slot("k".into(), 1).await.unwrap();
        assert!(a.is_some());
        assert!(
            counters
                .acquire_slot("k".into(), 1)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn schema_migration_is_idempotent_and_concurrent() {
        let Some(url) = schema_url().await else {
            return;
        };
        let opens = (0..4).map(|_| {
            let url = url.clone();
            tokio::spawn(async move { replica(&url, chunk_blobs, Duration::from_secs(30)).await })
        });
        for open in opens {
            open.await.unwrap();
        }
    }

    async fn body_rows(f: &Fixture) -> Vec<String> {
        f.db.pool
            .get()
            .await
            .unwrap()
            .query(
                "SELECT r.id FROM lda_payloads b LEFT JOIN lda_requests r ON r.seq = b.seq
                 ORDER BY r.id",
                &[],
            )
            .await
            .unwrap()
            .iter()
            .map(|r| r.get::<_, Option<String>>(0).unwrap_or_default())
            .collect()
    }

    #[tokio::test]
    async fn inline_bodies_leave_with_their_requests() {
        let Some(f) = crate::store::postgres::test_support::fixture().await else {
            return;
        };
        f.store.join(QUEUE).await.unwrap();
        submit(&f.store, 4).await;
        let mut sink =
            crate::store::staging::PayloadSink::new(f.store.blobs().clone(), "0a", 4, 1 << 20);
        sink.push(b"0123456789").await.unwrap();
        let payload = sink.finish().await.unwrap();
        let mut blob = new_request("blob", QUEUE, 20_000);
        blob.envelope.routing.request_token = "0a".into();
        blob.envelope.payload = payload.info("audio/wav");
        blob.payload = payload;
        f.store.submit(vec![blob]).await.unwrap();
        assert_eq!(body_rows(&f).await, ["r000", "r001", "r002", "r003"]);

        let peeked = f.store.peek(QUEUE.into(), 10, NOW_MS).await.unwrap();
        let env = |i: usize| Box::new(peeked[i].envelope.clone().unwrap());
        let admissions = vec![
            Admission::Claim {
                key: peeked[0].key,
                generation: env(0).generation_key(),
            },
            Admission::Claim {
                key: peeked[1].key,
                generation: env(1).generation_key(),
            },
            Admission::Finish {
                key: peeked[2].key,
                result: Box::new(ResultMessage::http(&env(2), 200, b"{}")),
                envelope: env(2),
            },
            Admission::Discard { key: peeked[3].key },
        ];
        let admitted = f
            .store
            .admit(QUEUE.into(), admissions, NOW_MS)
            .await
            .unwrap();
        let mut claims = Vec::new();
        for (a, want) in admitted.into_iter().zip(["r000", "r001"]) {
            let Admitted::Claimed { claim, payload } = a else {
                panic!("{want} was not claimed: {a:?}");
            };
            assert_eq!(
                payload.as_deref(),
                Some(format!(r#"{{"prompt":"{want}"}}"#).as_bytes())
            );
            claims.push(claim);
        }
        assert_eq!(body_rows(&f).await, ["r000", "r001"]);

        let retried = *env(1);
        let applied = f
            .store
            .apply_outcomes(
                vec![
                    Outcome::Finish {
                        claim: claims.remove(0),
                        result: ResultMessage::http(&env(0), 200, b"{}"),
                        envelope: *env(0),
                    },
                    Outcome::Retry {
                        claim: claims.remove(0),
                        envelope: retried.clone(),
                        due_ms: NOW_MS,
                    },
                ]
                .into(),
                NOW_MS,
            )
            .await
            .unwrap();
        assert_eq!(applied.result_routes.len(), 1);
        assert_eq!(body_rows(&f).await, ["r001"], "a retry keeps its body");
        match f.store.open_payload(&retried).await.unwrap() {
            Some(crate::store::queue::PayloadBody::Inline(b)) => {
                assert_eq!(&b[..], br#"{"prompt":"r001"}"#)
            }
            _ => panic!("the retried body is not inline"),
        }

        let (peeked, claim) = claim_all(&f.store, 10)
            .await
            .into_iter()
            .find(|(p, _)| p.envelope.as_ref().unwrap().request.id == "blob")
            .unwrap();
        let env = peeked.envelope.unwrap();
        f.store
            .apply_outcomes(
                vec![Outcome::Finish {
                    claim,
                    result: ResultMessage::http(&env, 200, b"{}"),
                    envelope: env,
                }]
                .into(),
                NOW_MS,
            )
            .await
            .unwrap();
        let key = crate::store::blob::key::BlobKey::request("0a").unwrap();
        assert!(f.store.blobs().open(&key).await.unwrap().is_none());
        assert_eq!(body_rows(&f).await, ["r001"]);
    }

    #[tokio::test]
    async fn migration_moves_version_3_bodies_and_claims() {
        let Some(url) = schema_url().await else {
            return;
        };
        let db = crate::store::postgres::connect::Database::connect(&url, 1, None)
            .await
            .unwrap();
        let inline = new_request("a", QUEUE, 10_000);
        let bodiless = new_request("b", QUEUE, 10_001);
        let envelopes = [
            serde_json::to_string(&inline.envelope).unwrap(),
            serde_json::to_string(&bodiless.envelope).unwrap(),
        ];
        db.pool
            .get()
            .await
            .unwrap()
            .batch_execute(
                "CREATE TABLE lda_requests (
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
                 );
                 CREATE TABLE lda_partitions (
                    queue            TEXT    NOT NULL,
                    partition_id     INTEGER NOT NULL,
                    owner            TEXT    NOT NULL DEFAULT '',
                    epoch            BIGINT  NOT NULL DEFAULT 0,
                    draining         BOOLEAN NOT NULL DEFAULT false,
                    lease_expires_ms BIGINT  NOT NULL DEFAULT 0,
                    PRIMARY KEY (queue, partition_id)
                 );
                 INSERT INTO lda_partitions VALUES ('q', 0, 'gone', 4, true, 0);
                 CREATE TABLE lda_schema (version INTEGER NOT NULL);
                 INSERT INTO lda_schema VALUES (3);",
            )
            .await
            .unwrap();
        db.pool
            .get()
            .await
            .unwrap()
            .execute(
                "INSERT INTO lda_requests
                     (id, request_token, queue, partition_id, deadline, envelope, payload,
                      dispatch_epoch)
                 VALUES ('a', $1, 'q', 0, 10000, $2, 'body', 4),
                        ('b', $3, 'q', 0, 10001, $4, NULL, 0)",
                &[
                    &inline.envelope.routing.request_token,
                    &envelopes[0],
                    &bodiless.envelope.routing.request_token,
                    &envelopes[1],
                ],
            )
            .await
            .unwrap();

        let f = replica(&url, chunk_blobs, Duration::from_secs(30)).await;
        let client = f.db.pool.get().await.unwrap();
        let flags: Vec<(String, bool, i64)> = client
            .query(
                "SELECT id, inline_payload, claimed_by FROM lda_requests ORDER BY id",
                &[],
            )
            .await
            .unwrap()
            .iter()
            .map(|r| (r.get(0), r.get(1), r.get(2)))
            .collect();
        assert_eq!(
            flags,
            [("a".into(), true, 0), ("b".into(), false, 0)],
            "a version 3 claim goes back to pending"
        );
        let dropped: i64 = client
            .query_one(
                "SELECT count(*) FROM information_schema.columns
                 WHERE table_schema = current_schema() AND table_name = 'lda_partitions'
                   AND column_name IN ('epoch', 'draining')",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(dropped, 0);
        assert_eq!(body_rows(&f).await, ["a"]);
        match f.store.open_payload(&inline.envelope).await.unwrap() {
            Some(crate::store::queue::PayloadBody::Inline(b)) => assert_eq!(&b[..], b"body"),
            _ => panic!("the migrated body is not inline"),
        }
        submit(&f.store, 1).await;
        assert_eq!(body_rows(&f).await, ["a", "r000"]);
    }
}
