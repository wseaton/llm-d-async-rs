//! Admission counters shared by every replica on one database, with exact
//! limits.
//!
//! Each process runs at most one statement per counter key at a time;
//! requests that arrive while it runs go in the next statement together.
//!
//! Concurrency slots are counted per heartbeated holder, so a replica that
//! dies stops counting once its holder lapses. A statement that fails may
//! still have committed, so it is never retried: it retires its holder
//! instead. A retired holder takes no new grants, stays renewed while any
//! statement, grant or release names it, and is deleted (freeing whatever
//! it still counts) once none does.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use deadpool_postgres::Pool;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::boxed::BoxFuture;
use crate::gate::admission::counters::{BucketSpec, CounterError, Counters, Slot};
use crate::store::error::StoreError;
use crate::store::postgres::DB_NOW_MS;
use crate::store::postgres::cached::Cached;

const STATEMENT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, thiserror::Error)]
enum StatementError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("timed out after {0:?}")]
    Timeout(Duration),
    #[error("quota holder lease lapsed")]
    Lapsed,
}

impl From<tokio_postgres::Error> for StatementError {
    fn from(e: tokio_postgres::Error) -> Self {
        Self::Store(e.into())
    }
}

impl From<deadpool_postgres::PoolError> for StatementError {
    fn from(e: deadpool_postgres::PoolError) -> Self {
        Self::Store(e.into())
    }
}

/// One holder row. `known` counts the statements in flight, grants handed
/// out, and releases queued under it.
struct Holder {
    name: String,
    state: Mutex<HolderState>,
}

struct HolderState {
    known: i64,
    renewed: Instant,
    retired: bool,
    deleting: bool,
    gone: bool,
}

type Reply = Result<(bool, Option<Arc<Holder>>), Arc<StatementError>>;

/// Sends one statement per key at a time; callers that arrive while it runs
/// share the next one, first come first admitted.
struct Batcher<K> {
    queues: Mutex<HashMap<K, Vec<oneshot::Sender<Reply>>>>,
}

impl<K> Default for Batcher<K> {
    fn default() -> Self {
        Self {
            queues: Mutex::new(HashMap::new()),
        }
    }
}

struct Shared {
    pool: Pool,
    ttl: Duration,
    registering: tokio::sync::Mutex<()>,
    holders: Mutex<Holders>,
    slots: Batcher<(String, u32)>,
    rates: Batcher<(String, u32, i64)>,
    buckets: Batcher<(String, u64, u64, i64)>,
    releases: Mutex<Releasing>,
    tasks: TaskTracker,
    stop: CancellationToken,
}

#[derive(Default)]
struct Holders {
    current: Option<Arc<Holder>>,
    all: Vec<Arc<Holder>>,
}

#[derive(Default)]
struct Releasing {
    pending: Vec<(Arc<Holder>, HashMap<String, i32>)>,
    running: bool,
}

#[derive(Clone)]
pub struct PgCounters(Arc<Shared>);

fn millis(d: Duration) -> i64 {
    i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
}

async fn timed<T>(f: impl Future<Output = Result<T, StatementError>>) -> Result<T, StatementError> {
    tokio::time::timeout(STATEMENT_TIMEOUT, f)
        .await
        .map_err(|_| StatementError::Timeout(STATEMENT_TIMEOUT))?
}

impl PgCounters {
    /// Starts the holder heartbeat. Slots of a process that stops
    /// heartbeating are freed once `ttl` lapses.
    pub fn start(pool: Pool, ttl: Duration) -> Self {
        let shared = Arc::new(Shared {
            pool,
            ttl,
            registering: tokio::sync::Mutex::new(()),
            holders: Mutex::new(Holders::default()),
            slots: Batcher::default(),
            rates: Batcher::default(),
            buckets: Batcher::default(),
            releases: Mutex::new(Releasing::default()),
            tasks: TaskTracker::new(),
            stop: CancellationToken::new(),
        });
        let beat = Arc::clone(&shared);
        shared.tasks.spawn(async move { beat.heartbeat().await });
        Self(shared)
    }

    /// Stops the heartbeat, writes queued releases, and deletes every holder
    /// so nothing this process counted outlives it.
    pub async fn close(&self) {
        self.0.stop.cancel();
        self.0.tasks.close();
        self.0.tasks.wait().await;
        let holders: Vec<Arc<Holder>> = self
            .0
            .holders
            .lock()
            .map(|h| h.all.clone())
            .unwrap_or_default();
        for h in holders {
            if let Err(e) = self.0.delete_holder(&h).await {
                tracing::warn!(holder = %h.name, error = %e, "failed to delete quota holder on close");
            }
        }
    }
}

type Run<K> = fn(
    Arc<Shared>,
    K,
    usize,
) -> BoxFuture<'static, Result<(usize, Option<Arc<Holder>>), StatementError>>;

/// How one kind of counter runs its statements.
struct Kind<K: 'static> {
    batcher: fn(&Shared) -> &Batcher<K>,
    run: Run<K>,
    /// Gives back a grant whose caller went away before it arrived.
    abandon: fn(&Arc<Shared>, &K, Arc<Holder>),
}

/// Queues a request for `key`. The request that finds no statement in
/// flight starts a task that runs statements until the queue is empty.
async fn batch<K>(shared: &Arc<Shared>, kind: &'static Kind<K>, key: K) -> Reply
where
    K: Clone + Eq + Hash + Send + 'static,
{
    let (tx, rx) = oneshot::channel();
    let start = match (kind.batcher)(shared).queues.lock() {
        Ok(mut queues) => {
            let busy = queues.contains_key(&key);
            queues.entry(key.clone()).or_default().push(tx);
            !busy
        }
        Err(_) => return Err(Arc::new(StatementError::Store(StoreError::Closed))),
    };
    if start {
        tokio::spawn(flush(Arc::clone(shared), kind, key));
    }
    rx.await
        .unwrap_or_else(|_| Err(Arc::new(StatementError::Store(StoreError::Closed))))
}

async fn flush<K>(shared: Arc<Shared>, kind: &'static Kind<K>, key: K)
where
    K: Clone + Eq + Hash + Send + 'static,
{
    loop {
        let waiting = match (kind.batcher)(&shared).queues.lock() {
            Ok(mut queues) => {
                let waiting = queues.get_mut(&key).map(std::mem::take).unwrap_or_default();
                if waiting.is_empty() {
                    queues.remove(&key);
                    return;
                }
                waiting
            }
            Err(_) => return,
        };
        match (kind.run)(Arc::clone(&shared), key.clone(), waiting.len()).await {
            Ok((granted, holder)) => {
                for (i, reply) in waiting.into_iter().enumerate() {
                    if let Err(Ok((true, Some(holder)))) =
                        reply.send(Ok((i < granted, holder.clone())))
                    {
                        (kind.abandon)(&shared, &key, holder);
                    }
                }
            }
            Err(e) => {
                let e = Arc::new(e);
                for reply in waiting {
                    let _ = reply.send(Err(Arc::clone(&e)));
                }
            }
        }
    }
}

impl Shared {
    /// Counts a statement against the current holder, registering one first
    /// if there is none.
    async fn begin(&self) -> Result<Arc<Holder>, StatementError> {
        let _registering = self.registering.lock().await;
        let current = self
            .holders
            .lock()
            .map_err(|_| StoreError::Closed)?
            .current
            .clone();
        let holder = match current {
            Some(h) if !h.state.lock().map(|s| s.retired).unwrap_or(true) => h,
            _ => {
                let name = format!("{:032x}", rand::random::<u128>());
                let started = Instant::now();
                timed(async {
                    let client = self.pool.get().await?;
                    client
                        .execute_cached(
                            &format!(
                                "INSERT INTO lda_quota_holders (holder, expires_ms)
                                 VALUES ($1, {DB_NOW_MS} + $2)"
                            ),
                            &[&name, &millis(self.ttl)],
                        )
                        .await?;
                    Ok(())
                })
                .await?;
                let holder = Arc::new(Holder {
                    name,
                    state: Mutex::new(HolderState {
                        known: 0,
                        renewed: started,
                        retired: false,
                        deleting: false,
                        gone: false,
                    }),
                });
                let mut holders = self.holders.lock().map_err(|_| StoreError::Closed)?;
                holders.current = Some(Arc::clone(&holder));
                holders.all.push(Arc::clone(&holder));
                holder
            }
        };
        if let Ok(mut st) = holder.state.lock() {
            st.known += 1;
        }
        Ok(holder)
    }

    fn retire(&self, holder: &Arc<Holder>) {
        if let Ok(mut st) = holder.state.lock() {
            st.retired = true;
        }
        if let Ok(mut holders) = self.holders.lock()
            && holders
                .current
                .as_ref()
                .is_some_and(|c| Arc::ptr_eq(c, holder))
        {
            holders.current = None;
        }
    }

    /// Takes `n` off the holder's known count and deletes it once it is
    /// retired and nothing names it.
    async fn settle(&self, holder: &Arc<Holder>, n: i64) {
        let delete = match holder.state.lock() {
            Ok(mut st) => {
                st.known -= n;
                let delete = st.retired && !st.gone && !st.deleting && st.known == 0;
                if delete {
                    st.deleting = true;
                }
                delete
            }
            Err(_) => false,
        };
        if delete && let Err(e) = self.delete_holder(holder).await {
            tracing::warn!(holder = %holder.name, error = %e, "failed to delete retired quota holder; retrying");
        }
    }

    async fn delete_holder(&self, holder: &Arc<Holder>) -> Result<(), StatementError> {
        timed(async {
            let client = self.pool.get().await?;
            client
                .execute_cached(
                    "DELETE FROM lda_quota_holders WHERE holder = $1",
                    &[&holder.name],
                )
                .await?;
            client
                .execute_cached(
                    "DELETE FROM lda_quota_slots WHERE holder = $1",
                    &[&holder.name],
                )
                .await?;
            Ok(())
        })
        .await?;
        if let Ok(mut st) = holder.state.lock() {
            st.gone = true;
        }
        if let Ok(mut holders) = self.holders.lock() {
            holders.all.retain(|h| !Arc::ptr_eq(h, holder));
        }
        Ok(())
    }

    async fn acquire_slots(
        &self,
        key: &str,
        limit: u32,
        n: usize,
    ) -> Result<(usize, Option<Arc<Holder>>), StatementError> {
        let n = i32::try_from(n).unwrap_or(i32::MAX);
        let limit = i32::try_from(limit).unwrap_or(i32::MAX);
        let mut retried = false;
        loop {
            let holder = self.begin().await?;
            let granted: Result<i32, StatementError> = timed(async {
                let client = self.pool.get().await?;
                let row = client
                    .query_one_cached(
                        "SELECT lda_quota_acquire($1, $2, $3, $4)",
                        &[&key, &holder.name, &n, &limit],
                    )
                    .await?;
                let granted: i32 = row.get(0);
                if granted < 0 {
                    return Err(StatementError::Lapsed);
                }
                Ok(granted)
            })
            .await;
            match granted {
                Ok(granted) => {
                    self.settle(&holder, 1 - i64::from(granted)).await;
                    return Ok((usize::try_from(granted).unwrap_or(0), Some(holder)));
                }
                Err(e) => {
                    self.retire(&holder);
                    self.settle(&holder, 1).await;
                    if matches!(e, StatementError::Lapsed) && !retried {
                        retried = true;
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    fn release(self: &Arc<Self>, holder: Arc<Holder>, key: String) {
        let start = match self.releases.lock() {
            Ok(mut r) => {
                match r.pending.iter_mut().find(|(h, _)| Arc::ptr_eq(h, &holder)) {
                    Some((_, keys)) => *keys.entry(key).or_default() += 1,
                    None => r.pending.push((holder, HashMap::from([(key, 1)]))),
                }
                let start = !r.running;
                r.running = true;
                start
            }
            Err(_) => false,
        };
        if start {
            let this = Arc::clone(self);
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move { this.flush_releases().await });
            }
        }
    }

    /// Sends each holder's queued releases once; a failed release retires
    /// its holder, whose deletion frees the slots instead.
    async fn flush_releases(&self) {
        loop {
            let batch = match self.releases.lock() {
                Ok(mut r) => {
                    if r.pending.is_empty() {
                        r.running = false;
                        return;
                    }
                    std::mem::take(&mut r.pending)
                }
                Err(_) => return,
            };
            for (holder, by_key) in batch {
                let total: i64 = by_key.values().map(|n| i64::from(*n)).sum();
                let (keys, counts): (Vec<String>, Vec<i32>) = by_key.into_iter().unzip();
                let released: Result<(), StatementError> = timed(async {
                    let client = self.pool.get().await?;
                    client
                        .execute_cached(
                            "UPDATE lda_quota_slots s SET used = greatest(s.used - r.n, 0)
                             FROM unnest($2::text[], $3::int[]) AS r(key, n)
                             WHERE s.holder = $1 AND s.key = r.key",
                            &[&holder.name, &keys, &counts],
                        )
                        .await?;
                    Ok(())
                })
                .await;
                if let Err(e) = released {
                    tracing::warn!(holder = %holder.name, error = %e, "failed to release quota slots; retiring the holder");
                    self.retire(&holder);
                }
                self.settle(&holder, total).await;
            }
        }
    }

    async fn heartbeat(&self) {
        let period = (self.ttl / 3).max(Duration::from_millis(100));
        let mut ticker = tokio::time::interval(period);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = self.stop.cancelled() => return,
                _ = ticker.tick() => {}
            }
            let holders = self
                .holders
                .lock()
                .map(|h| h.all.clone())
                .unwrap_or_default();
            if holders.is_empty() {
                continue;
            }
            for h in holders {
                let deleting = h.state.lock().map(|s| s.deleting).unwrap_or(false);
                if deleting {
                    if let Err(e) = self.delete_holder(&h).await {
                        tracing::warn!(holder = %h.name, error = %e, "failed to delete retired quota holder");
                    }
                } else {
                    self.renew(&h).await;
                }
            }
            let reaped: Result<(), StatementError> = timed(async {
                let client = self.pool.get().await?;
                client
                    .execute_cached(
                        &format!(
                            "WITH dead AS (
                                DELETE FROM lda_quota_holders WHERE expires_ms < {DB_NOW_MS} - $1
                                RETURNING holder
                             )
                             DELETE FROM lda_quota_slots s USING dead WHERE s.holder = dead.holder"
                        ),
                        &[&millis(self.ttl)],
                    )
                    .await?;
                Ok(())
            })
            .await;
            if let Err(e) = reaped {
                tracing::warn!(error = %e, "failed to reap lapsed quota holders");
            }
        }
    }

    async fn renew(&self, holder: &Arc<Holder>) {
        let started = Instant::now();
        let renewed: Result<bool, StatementError> = timed(async {
            let client = self.pool.get().await?;
            let n = client
                .execute_cached(
                    &format!(
                        "UPDATE lda_quota_holders SET expires_ms = {DB_NOW_MS} + $2
                         WHERE holder = $1 AND expires_ms > {DB_NOW_MS}"
                    ),
                    &[&holder.name, &millis(self.ttl)],
                )
                .await?;
            Ok(n == 1)
        })
        .await;
        let lapsed = {
            let Ok(mut st) = holder.state.lock() else {
                return;
            };
            match renewed {
                Ok(true) => {
                    st.renewed = started;
                    false
                }
                Ok(false) => {
                    if st.known > 0 {
                        tracing::error!(holder = %holder.name, known = st.known, "quota holder lease lapsed with grants in use; the limit may be exceeded until they finish");
                    }
                    st.retired = true;
                    st.gone = true;
                    true
                }
                Err(e) if started.duration_since(st.renewed) >= self.ttl => {
                    tracing::error!(holder = %holder.name, error = %e, "cannot renew quota holder; retiring it");
                    st.retired = true;
                    false
                }
                Err(e) => {
                    tracing::warn!(holder = %holder.name, error = %e, "failed to renew quota holder");
                    false
                }
            }
        };
        let retired = holder.state.lock().map(|s| s.retired).unwrap_or(true);
        if let Ok(mut holders) = self.holders.lock() {
            if retired
                && holders
                    .current
                    .as_ref()
                    .is_some_and(|c| Arc::ptr_eq(c, holder))
            {
                holders.current = None;
            }
            if lapsed {
                holders.all.retain(|h| !Arc::ptr_eq(h, holder));
            }
        }
    }

    async fn admit_rate(
        &self,
        key: &str,
        limit: u32,
        window_ms: i64,
        n: usize,
    ) -> Result<(usize, Option<Arc<Holder>>), StatementError> {
        let n = i32::try_from(n).unwrap_or(i32::MAX);
        let limit = i32::try_from(limit).unwrap_or(i32::MAX);
        let granted: i32 = timed(async {
            let client = self.pool.get().await?;
            Ok(client
                .query_one_cached(
                    "SELECT lda_quota_admit($1, $2, $3, $4)",
                    &[&key, &n, &limit, &window_ms],
                )
                .await?
                .get(0))
        })
        .await?;
        Ok((usize::try_from(granted).unwrap_or(0), None))
    }

    async fn take_tokens(
        &self,
        key: &str,
        spec: BucketSpec,
        n: usize,
    ) -> Result<(usize, Option<Arc<Holder>>), StatementError> {
        let n = i32::try_from(n).unwrap_or(i32::MAX);
        let granted: i32 = timed(async {
            let client = self.pool.get().await?;
            Ok(client
                .query_one_cached(
                    "SELECT lda_rate_take($1, $2, $3, $4, $5)",
                    &[&key, &n, &spec.rate, &spec.capacity, &spec.expires_ms],
                )
                .await?
                .get(0))
        })
        .await?;
        Ok((usize::try_from(granted).unwrap_or(0), None))
    }
}

static SLOTS: Kind<(String, u32)> = Kind {
    batcher: |s| &s.slots,
    run: |s, (key, limit), n| Box::pin(async move { s.acquire_slots(&key, limit, n).await }),
    abandon: |s, (key, _), holder| s.release(holder, key.clone()),
};

static RATES: Kind<(String, u32, i64)> = Kind {
    batcher: |s| &s.rates,
    run: |s, (key, limit, window_ms), n| {
        Box::pin(async move { s.admit_rate(&key, limit, window_ms, n).await })
    },
    abandon: |_, _, _| {},
};

static BUCKETS: Kind<(String, u64, u64, i64)> = Kind {
    batcher: |s| &s.buckets,
    run: |s, (key, rate, capacity, expires_ms), n| {
        let spec = BucketSpec {
            rate: f64::from_bits(rate),
            capacity: f64::from_bits(capacity),
            expires_ms,
        };
        Box::pin(async move { s.take_tokens(&key, spec, n).await })
    },
    abandon: |_, _, _| {},
};

fn counter_error(e: Arc<StatementError>) -> CounterError {
    CounterError::Abandoned(e.to_string())
}

impl Counters for PgCounters {
    fn acquire_slot(
        &self,
        key: String,
        limit: u32,
    ) -> BoxFuture<'_, Result<Option<Slot>, CounterError>> {
        Box::pin(async move {
            let shared = &self.0;
            let reply = batch(shared, &SLOTS, (key.clone(), limit))
                .await
                .map_err(counter_error)?;
            match reply {
                (true, Some(holder)) => {
                    let owner = Arc::clone(shared);
                    Ok(Some(Slot::new(move || owner.release(holder, key))))
                }
                _ => Ok(None),
            }
        })
    }

    fn admit(
        &self,
        key: String,
        limit: u32,
        window: Duration,
    ) -> BoxFuture<'_, Result<bool, CounterError>> {
        Box::pin(async move {
            let (ok, _) = batch(&self.0, &RATES, (key, limit, millis(window)))
                .await
                .map_err(counter_error)?;
            Ok(ok)
        })
    }

    fn take_token(
        &self,
        key: String,
        bucket: BucketSpec,
    ) -> BoxFuture<'_, Result<bool, CounterError>> {
        Box::pin(async move {
            let capacity = bucket.capacity.max(1.0);
            if !capacity.is_finite() || !bucket.rate.is_finite() {
                return Ok(false);
            }
            let key = (
                key,
                bucket.rate.to_bits(),
                capacity.to_bits(),
                bucket.expires_ms,
            );
            let (ok, _) = batch(&self.0, &BUCKETS, key).await.map_err(counter_error)?;
            Ok(ok)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::gate::admission::counters::Counters;
    use crate::gate::admission::counters::conformance::counters_conformance;
    use crate::store::postgres::counters::PgCounters;
    use crate::store::postgres::test_support::fixture as store;

    async fn fixture() -> Option<Arc<PgCounters>> {
        let f = store().await?;
        Some(Arc::new(f.pg.counters()))
    }

    counters_conformance!(fixture);

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn slots_are_counted_across_replicas() {
        let Some(url) = crate::store::postgres::test_support::schema_url().await else {
            return;
        };
        let a = crate::store::postgres::test_support::replica(
            &url,
            crate::store::postgres::test_support::chunk_blobs,
            Duration::from_secs(30),
        )
        .await;
        let b = crate::store::postgres::test_support::replica(
            &url,
            crate::store::postgres::test_support::chunk_blobs,
            Duration::from_secs(30),
        )
        .await;
        let (ca, cb) = (a.pg.counters(), b.pg.counters());
        let held_a = ca.acquire_slot("t".into(), 3).await.unwrap().unwrap();
        let _held_b = cb.acquire_slot("t".into(), 3).await.unwrap().unwrap();
        let _held_b2 = cb.acquire_slot("t".into(), 3).await.unwrap().unwrap();
        assert!(ca.acquire_slot("t".into(), 3).await.unwrap().is_none());
        assert!(cb.acquire_slot("t".into(), 3).await.unwrap().is_none());
        assert!(
            ca.admit("r".into(), 1, Duration::from_secs(60))
                .await
                .unwrap()
        );
        assert!(
            !cb.admit("r".into(), 1, Duration::from_secs(60))
                .await
                .unwrap()
        );
        drop(held_a);
        let mut again = None;
        for _ in 0..100 {
            again = cb.acquire_slot("t".into(), 3).await.unwrap();
            if again.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            again.is_some(),
            "a slot released on one replica frees it for another"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_dead_replicas_slots_free_once_its_holder_lapses() {
        let Some(url) = crate::store::postgres::test_support::schema_url().await else {
            return;
        };
        let ttl = Duration::from_millis(600);
        let a = crate::store::postgres::test_support::replica(
            &url,
            crate::store::postgres::test_support::chunk_blobs,
            ttl,
        )
        .await;
        let b = crate::store::postgres::test_support::replica(
            &url,
            crate::store::postgres::test_support::chunk_blobs,
            ttl,
        )
        .await;
        let ca = a.pg.counters();
        let held = ca.acquire_slot("t".into(), 1).await.unwrap().unwrap();
        ca.0.stop.cancel();
        ca.0.tasks.close();
        ca.0.tasks.wait().await;
        std::mem::forget(held);
        let cb = b.pg.counters();
        assert!(cb.acquire_slot("t".into(), 1).await.unwrap().is_none());
        tokio::time::sleep(ttl + Duration::from_millis(300)).await;
        assert!(cb.acquire_slot("t".into(), 1).await.unwrap().is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn closing_frees_every_slot() {
        let Some(url) = crate::store::postgres::test_support::schema_url().await else {
            return;
        };
        let a = crate::store::postgres::test_support::replica(
            &url,
            crate::store::postgres::test_support::chunk_blobs,
            Duration::from_secs(30),
        )
        .await;
        let b = crate::store::postgres::test_support::replica(
            &url,
            crate::store::postgres::test_support::chunk_blobs,
            Duration::from_secs(30),
        )
        .await;
        let ca = a.pg.counters();
        let held = ca.acquire_slot("t".into(), 1).await.unwrap().unwrap();
        std::mem::forget(held);
        ca.close().await;
        assert!(
            b.pg.counters()
                .acquire_slot("t".into(), 1)
                .await
                .unwrap()
                .is_some()
        );
    }
}
