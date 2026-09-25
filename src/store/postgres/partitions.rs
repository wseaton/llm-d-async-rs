//! Partition leases: how replicas split a queue, and who holds a claim.
//!
//! Each queue hashes its requests into [`PARTITIONS`] partitions. A replica
//! that joins a queue heartbeats a membership row and leases its share,
//! `ceil(PARTITIONS / live members)`, of the partitions; it releases any
//! partition above its share at once. A partition's owner claims its pending
//! requests.
//!
//! A claim belongs to the process that made it, not to the partition: each
//! process heartbeats a row in `lda_processes`, and a claim stamps the row's
//! id and a fresh attempt number on the request. A partition handed to
//! another replica keeps its claims in flight with the process that made
//! them, which finishes them while the new owner dispatches the rest. The
//! claims of a process whose heartbeat lapses go back to pending.
//!
//! ```text
//!   process A          lda_requests row             process B
//!   ─────────          ────────────────             ─────────
//!   owns partition 7
//!   claim  ──────────> claimed_by = A, attempt = 41
//!   hands 7 to B ─────────────────────────────────> owns partition 7
//!   finish(41) ──────> deleted                      claims pending rows of 7
//!
//!   A dies:            claimed_by = A ──(A's row lapses)──> claimed_by = 0
//!                                                   claims it again, attempt 42
//!   A's late finish(41) matches nothing: fenced
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::store::error::StoreError;
use crate::store::postgres::cached::Cached;
use crate::store::postgres::requests::RELEASE;
use crate::store::postgres::{DB_NOW_MS, Inner};

pub const PARTITIONS: i32 = 64;

/// Claims are reconciled on every tenth heartbeat of a queue.
const RECONCILE_EVERY: u64 = 10;

/// Returns the claims of every process whose heartbeat lapsed to pending.
pub(crate) const RETURN_LAPSED: &str = concat!(
    "UPDATE lda_requests SET claimed_by = 0
     WHERE claimed_by > 0 AND claimed_by = ANY(ARRAY(
         SELECT id FROM lda_processes WHERE expires_ms < ",
    db_now_ms!(),
    "))"
);

/// FNV-1a: a stable spread of request IDs over partitions.
pub fn partition_of(id: &str) -> i32 {
    let mut hash: u32 = 0x811c_9dc5;
    for b in id.bytes() {
        hash ^= u32::from(b);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    i32::try_from(hash % PARTITIONS.unsigned_abs()).unwrap_or(0)
}

#[derive(Default)]
pub(crate) struct State {
    pub queues: HashMap<String, QueueLeases>,
    /// Claims this process holds, by generation.
    pub claims: HashMap<String, Tracked>,
}

pub(crate) struct QueueLeases {
    pub wanted: bool,
    seeded: bool,
    /// A claim statement failed after it may have committed: rows may be
    /// stamped for this process that it does not know about.
    pub reconcile: bool,
    beats: u64,
    /// Held by claiming and by rebalancing, never both at once, so a
    /// reconcile cannot mistake a claim in flight for a lost one.
    pub cycle: Arc<tokio::sync::Mutex<()>>,
}

impl QueueLeases {
    fn new() -> Self {
        Self {
            wanted: true,
            seeded: false,
            reconcile: false,
            beats: 0,
            cycle: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Tracked {
    pub queue: String,
    pub id: String,
    pub token: String,
    pub attempt: i64,
}

/// Registers a process and returns its id.
pub(crate) async fn register(
    client: &deadpool_postgres::Object,
    ttl_ms: i64,
) -> Result<i64, StoreError> {
    Ok(client
        .query_one_cached(
            &format!(
                "INSERT INTO lda_processes (expires_ms) VALUES ({DB_NOW_MS} + $1) RETURNING id"
            ),
            &[&ttl_ms],
        )
        .await?
        .get(0))
}

impl Inner {
    /// The id this process stamps its claims with.
    pub(crate) fn process(&self) -> i64 {
        self.process.load(Ordering::Acquire)
    }

    /// Renews this process's heartbeat and returns the claims of processes
    /// whose heartbeat lapsed to pending. A process the sweep has already
    /// forgotten registers again: its claims were returned, so the ones it
    /// still tracks are dropped and their outcomes fenced.
    pub(crate) async fn beat(&self) -> Result<(), StoreError> {
        let ttl_ms = self.lease_ttl_ms();
        let client = self.control.get().await?;
        let renewed = client
            .execute_cached(
                &format!("UPDATE lda_processes SET expires_ms = {DB_NOW_MS} + $2 WHERE id = $1"),
                &[&self.process(), &ttl_ms],
            )
            .await?;
        if renewed == 0 {
            let id = register(&client, ttl_ms).await?;
            tracing::warn!(
                old = self.process(),
                new = id,
                "process heartbeat was lost; registered again"
            );
            self.process.store(id, Ordering::Release);
            if let Ok(mut state) = self.state.lock() {
                state.claims.clear();
            }
        }
        client.execute_cached(RETURN_LAPSED, &[]).await?;
        Ok(())
    }

    /// Returns every claim this process still holds to pending and removes
    /// its heartbeat, for a process that is shutting down.
    pub(crate) async fn retire(&self) -> Result<(), StoreError> {
        let process = self.process();
        let mut client = self.control.get().await?;
        let txn = client.transaction().await?;
        txn.execute_cached(
            "UPDATE lda_requests SET claimed_by = 0 WHERE claimed_by = $1",
            &[&process],
        )
        .await?;
        txn.execute_cached("DELETE FROM lda_processes WHERE id = $1", &[&process])
            .await?;
        txn.commit().await?;
        Ok(())
    }

    pub(crate) fn want(&self, queue: &str) -> Result<(), StoreError> {
        let mut state = self.state.lock().map_err(|_| StoreError::Closed)?;
        state
            .queues
            .entry(queue.to_owned())
            .or_insert_with(QueueLeases::new)
            .wanted = true;
        Ok(())
    }

    pub(crate) fn unwant(&self, queue: &str) {
        if let Ok(mut state) = self.state.lock()
            && let Some(q) = state.queues.get_mut(queue)
        {
            q.wanted = false;
        }
        self.wake.notify_one();
    }

    pub(crate) fn cycle(&self, queue: &str) -> Result<Arc<tokio::sync::Mutex<()>>, StoreError> {
        let mut state = self.state.lock().map_err(|_| StoreError::Closed)?;
        let q = state
            .queues
            .entry(queue.to_owned())
            .or_insert_with(|| QueueLeases {
                wanted: false,
                ..QueueLeases::new()
            });
        Ok(Arc::clone(&q.cycle))
    }

    /// Runs `f` on the lease state of `queue`, creating it unwanted.
    fn with_queue<T>(
        &self,
        queue: &str,
        f: impl FnOnce(&mut QueueLeases, &HashMap<String, Tracked>) -> T,
    ) -> Result<T, StoreError> {
        let mut state = self.state.lock().map_err(|_| StoreError::Closed)?;
        let State { queues, claims } = &mut *state;
        let q = queues
            .entry(queue.to_owned())
            .or_insert_with(|| QueueLeases {
                wanted: false,
                ..QueueLeases::new()
            });
        Ok(f(q, claims))
    }

    /// Queues this process takes part in, wanted or still finishing claims.
    pub(crate) fn queue_names(&self) -> Vec<String> {
        self.state
            .lock()
            .map(|s| s.queues.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// One heartbeat of `queue`: renew, reconcile, and move to this
    /// process's share.
    pub(crate) async fn rebalance(&self, queue: &str) -> Result<(), StoreError> {
        let cycle = self.cycle(queue)?;
        let _cycle = cycle.lock().await;
        let (seeded, wanted) = self.with_queue(queue, |q, _| (q.seeded, q.wanted))?;
        let (held, members) = self.heartbeat(queue, wanted).await?;
        if !seeded {
            self.seed(queue).await?;
            self.with_queue(queue, |q, _| q.seeded = true)?;
        }
        let reconcile = self.with_queue(queue, |q, _| {
            q.beats += 1;
            if q.beats % RECONCILE_EVERY == 0 {
                q.reconcile = true;
            }
            q.reconcile
        })?;
        if reconcile && let Err(e) = self.reconcile(queue).await {
            tracing::warn!(%queue, error = %e, "failed to reconcile claims; retrying next heartbeat");
        }

        let target = if wanted {
            let members = members.max(1);
            usize::try_from((PARTITIONS + members - 1) / members).unwrap_or(0)
        } else {
            0
        };
        if held.len() < target {
            self.acquire(queue, target - held.len()).await?;
        } else if held.len() > target {
            let excess: Vec<i32> = held
                .iter()
                .rev()
                .take(held.len() - target)
                .copied()
                .collect();
            self.release(queue, &excess).await?;
        }

        if !wanted {
            self.forget_if_idle(queue).await?;
        }
        Ok(())
    }

    async fn seed(&self, queue: &str) -> Result<(), StoreError> {
        let client = self.control.get().await?;
        client
            .execute_cached(
                "INSERT INTO lda_partitions (queue, partition_id)
                 SELECT $1, g FROM generate_series(0, $2 - 1) AS g
                 ON CONFLICT (queue, partition_id) DO NOTHING",
                &[&queue, &PARTITIONS],
            )
            .await?;
        Ok(())
    }

    /// Renews this process's leases and membership, and returns the
    /// partitions it holds, in order, and the count of live members. A
    /// process that no longer wants the queue renews without counting, so
    /// peers take its partitions over.
    async fn heartbeat(&self, queue: &str, member: bool) -> Result<(Vec<i32>, i32), StoreError> {
        let ttl_ms = self.lease_ttl_ms();
        let mut client = self.control.get().await?;
        let txn = client.transaction().await?;
        if member {
            txn.execute_cached(
&format!(
                    "INSERT INTO lda_dispatchers (queue, owner, expires_ms) VALUES ($1, $2, {DB_NOW_MS} + $3)
                     ON CONFLICT (queue, owner) DO UPDATE SET expires_ms = EXCLUDED.expires_ms"
                ),
                &[&queue, &self.owner, &ttl_ms],
            )
            .await?;
        } else {
            txn.execute_cached(
                "DELETE FROM lda_dispatchers WHERE queue = $1 AND owner = $2",
                &[&queue, &self.owner],
            )
            .await?;
        }
        txn.execute_cached(
            &format!("DELETE FROM lda_dispatchers WHERE queue = $1 AND expires_ms < {DB_NOW_MS}"),
            &[&queue],
        )
        .await?;
        let members: i64 = txn
            .query_one_cached(
                "SELECT count(*) FROM lda_dispatchers WHERE queue = $1",
                &[&queue],
            )
            .await?
            .get(0);
        let rows = txn
            .query_cached(
                &format!(
                    "UPDATE lda_partitions SET lease_expires_ms = {DB_NOW_MS} + $3
                     WHERE queue = $1 AND owner = $2
                     RETURNING partition_id"
                ),
                &[&queue, &self.owner, &ttl_ms],
            )
            .await?;
        txn.commit().await?;
        let mut held: Vec<i32> = rows.iter().map(|r| r.get(0)).collect();
        held.sort_unstable();
        Ok((held, i32::try_from(members).unwrap_or(i32::MAX)))
    }

    /// Takes up to `limit` unowned or lapsed partitions.
    async fn acquire(&self, queue: &str, limit: usize) -> Result<(), StoreError> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let client = self.control.get().await?;
        client
            .execute_cached(
                &format!(
                    "WITH picked AS MATERIALIZED (
                        SELECT partition_id FROM lda_partitions
                        WHERE queue = $1 AND (owner = '' OR lease_expires_ms < {DB_NOW_MS})
                        ORDER BY lease_expires_ms, partition_id
                        LIMIT $4 FOR UPDATE SKIP LOCKED
                     )
                     UPDATE lda_partitions p
                     SET owner = $2, lease_expires_ms = {DB_NOW_MS} + $3
                     FROM picked
                     WHERE p.queue = $1 AND p.partition_id = picked.partition_id"
                ),
                &[&queue, &self.owner, &self.lease_ttl_ms(), &limit],
            )
            .await?;
        Ok(())
    }

    async fn release(&self, queue: &str, parts: &[i32]) -> Result<(), StoreError> {
        let client = self.control.get().await?;
        client
            .execute_cached(
                "UPDATE lda_partitions SET owner = '', lease_expires_ms = 0
                 WHERE queue = $1 AND owner = $2 AND partition_id = ANY($3)",
                &[&queue, &self.owner, &parts],
            )
            .await?;
        Ok(())
    }

    /// Returns rows claimed by this process that it holds no claim for: a
    /// claim statement that committed but whose reply was lost.
    async fn reconcile(&self, queue: &str) -> Result<(), StoreError> {
        let process = self.process();
        let client = self.control.get().await?;
        let rows = client
            .query_cached(
                "SELECT id, request_token, dispatch_attempt FROM lda_requests
                 WHERE claimed_by = $1 AND queue = $2",
                &[&process, &queue],
            )
            .await?;
        let untracked = self.with_queue(queue, |_, claims| {
            rows.iter()
                .filter_map(|row| {
                    let id: String = row.get(0);
                    let token: String = row.get(1);
                    let attempt: i64 = row.get(2);
                    let generation = crate::api::request::generation_key(&id, &token);
                    match claims.get(&generation) {
                        Some(t) if t.attempt == attempt => None,
                        _ => Some((id, token, attempt)),
                    }
                })
                .collect::<Vec<_>>()
        })?;
        if !untracked.is_empty() {
            let (ids, rest): (Vec<String>, Vec<(String, i64)>) =
                untracked.into_iter().map(|(i, t, a)| (i, (t, a))).unzip();
            let (tokens, attempts): (Vec<String>, Vec<i64>) = rest.into_iter().unzip();
            tracing::warn!(%queue, requests = ids.len(), "returning claims this process lost track of");
            client
                .execute_cached(RELEASE, &[&process, &ids, &tokens, &attempts])
                .await?;
        }
        self.with_queue(queue, |q, _| q.reconcile = false)?;
        Ok(())
    }

    /// Drops a queue nobody here wants once no claim on it is in flight.
    async fn forget_if_idle(&self, queue: &str) -> Result<(), StoreError> {
        let idle = self.with_queue(queue, |q, claims| {
            !q.wanted && !claims.values().any(|t| t.queue == queue)
        })?;
        if !idle {
            return Ok(());
        }
        self.leave_queue(queue).await?;
        let mut state = self.state.lock().map_err(|_| StoreError::Closed)?;
        if state.queues.get(queue).is_some_and(|q| !q.wanted) {
            state.queues.remove(queue);
        }
        Ok(())
    }

    /// Releases every partition of `queue` and drops the membership row.
    pub(crate) async fn leave_queue(&self, queue: &str) -> Result<(), StoreError> {
        let mut client = self.control.get().await?;
        let txn = client.transaction().await?;
        txn.execute_cached(
            "UPDATE lda_partitions SET owner = '', lease_expires_ms = 0
             WHERE queue = $1 AND owner = $2",
            &[&queue, &self.owner],
        )
        .await?;
        txn.execute_cached(
            "DELETE FROM lda_dispatchers WHERE queue = $1 AND owner = $2",
            &[&queue, &self.owner],
        )
        .await?;
        txn.commit().await?;
        Ok(())
    }

    pub(crate) fn lease_ttl_ms(&self) -> i64 {
        i64::try_from(self.lease_ttl.as_millis()).unwrap_or(i64::MAX)
    }
}

#[cfg(test)]
mod tests {
    use crate::store::postgres::partitions::{PARTITIONS, partition_of};

    #[test]
    fn partitions_spread_and_stay_stable() {
        assert_eq!(partition_of("a"), partition_of("a"));
        let mut seen = std::collections::BTreeSet::new();
        for i in 0..1000 {
            let p = partition_of(&format!("req-{i}"));
            assert!((0..PARTITIONS).contains(&p));
            seen.insert(p);
        }
        assert_eq!(seen.len(), PARTITIONS as usize);
    }
}
