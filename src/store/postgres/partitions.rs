//! Partition leases: how replicas split a queue.
//!
//! Each queue hashes its requests into [`PARTITIONS`] partitions. A replica
//! that joins a queue heartbeats a membership row and leases its share,
//! `ceil(PARTITIONS / live members)`, of the partitions. Acquiring a
//! partition bumps its epoch; a claim stamps the partition's epoch and a
//! fresh attempt number on the row. Requests stamped by an older epoch
//! belonged to a replica that lost the lease and go back to pending.
//!
//! ```text
//!   owned ──(above share)──> draining ──(nothing in flight, or handoff timeout)──> released
//!     ^                          │
//!     └───────(below share)──────┘
//! ```
//!
//! A draining partition dispatches nothing new, but its claims still finish.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Instant;

use crate::store::error::StoreError;
use crate::store::postgres::cached::Cached;
use crate::store::postgres::requests::RELEASE;
use crate::store::postgres::{DB_NOW_MS, Inner};

pub const PARTITIONS: i32 = 64;

/// Stale stamps come from partitions acquired recently, so only those are
/// reset on every heartbeat; every partition is reset, and claims
/// reconciled, on every tenth.
const FULL_RESET_EVERY: u64 = 10;

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
    leases: BTreeMap<i32, Lease>,
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
            leases: BTreeMap::new(),
            cycle: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Lease {
    epoch: i64,
    acquired_at: Instant,
    draining: bool,
    drain_since: Instant,
}

#[derive(Debug, Clone)]
pub(crate) struct Tracked {
    pub queue: String,
    pub id: String,
    pub token: String,
    pub partition: i32,
    pub epoch: i64,
    pub attempt: i64,
}

struct Held {
    partition: i32,
    epoch: i64,
    draining: bool,
}

fn held(rows: &[tokio_postgres::Row]) -> Vec<Held> {
    rows.iter()
        .map(|r| Held {
            partition: r.get(0),
            epoch: r.get(1),
            draining: r.get(2),
        })
        .collect()
}

fn inflight(claims: &HashMap<String, Tracked>, queue: &str, partition: i32, epoch: i64) -> usize {
    claims
        .values()
        .filter(|t| t.queue == queue && t.partition == partition && t.epoch == epoch)
        .count()
}

impl Inner {
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

    /// Queues this process takes part in, wanted or still draining.
    pub(crate) fn queue_names(&self) -> Vec<String> {
        self.state
            .lock()
            .map(|s| s.queues.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// One heartbeat of `queue`: renew, reconcile, and move toward this
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
        let now = Instant::now();
        let recent = self.with_queue(queue, |q, _| {
            let mut next = BTreeMap::new();
            for h in held {
                let mut lease = match q.leases.get(&h.partition) {
                    Some(l) if l.epoch == h.epoch => *l,
                    _ => Lease {
                        epoch: h.epoch,
                        acquired_at: now,
                        draining: false,
                        drain_since: now,
                    },
                };
                if h.draining && !lease.draining {
                    lease.drain_since = now;
                }
                lease.draining = h.draining;
                next.insert(h.partition, lease);
            }
            q.leases = next;
            q.beats += 1;
            if q.beats % FULL_RESET_EVERY == 0 {
                q.reconcile = true;
                return None;
            }
            let window = self.lease_ttl * 2;
            Some(
                q.leases
                    .iter()
                    .filter(|(_, l)| now.duration_since(l.acquired_at) < window)
                    .map(|(p, _)| *p)
                    .collect::<Vec<i32>>(),
            )
        })?;
        self.reset_stale(queue, recent.as_deref()).await?;
        if self.with_queue(queue, |q, _| q.reconcile)?
            && let Err(e) = self.reconcile(queue).await
        {
            tracing::warn!(%queue, error = %e, "failed to reconcile claims; retrying next heartbeat");
        }
        self.release_drained(queue).await?;

        let target = if wanted {
            let members = members.max(1);
            (PARTITIONS + members - 1) / members
        } else {
            0
        };
        let (active, draining) = self.with_queue(queue, |q, _| {
            let mut active = Vec::new();
            let mut draining = Vec::new();
            for (p, l) in &q.leases {
                if l.draining {
                    draining.push((*p, l.epoch));
                } else {
                    active.push((*p, l.epoch));
                }
            }
            (active, draining)
        })?;
        let active_n = i32::try_from(active.len()).unwrap_or(PARTITIONS);
        if active_n < target {
            let need = usize::try_from(target - active_n).unwrap_or(0);
            let undrain: Vec<i32> = draining.iter().take(need).map(|(p, _)| *p).collect();
            self.set_draining(queue, &undrain, false).await?;
            self.with_queue(queue, |q, _| {
                for p in &undrain {
                    if let Some(l) = q.leases.get_mut(p) {
                        l.draining = false;
                    }
                }
            })?;
            let need = need - undrain.len();
            if need > 0 {
                let acquired = self.acquire(queue, need).await?;
                let parts: Vec<i32> = acquired.iter().map(|h| h.partition).collect();
                let now = Instant::now();
                self.with_queue(queue, |q, _| {
                    for h in &acquired {
                        q.leases.insert(
                            h.partition,
                            Lease {
                                epoch: h.epoch,
                                acquired_at: now,
                                draining: false,
                                drain_since: now,
                            },
                        );
                    }
                })?;
                self.reset_stale(queue, Some(&parts)).await?;
            }
        } else if active_n > target {
            let excess = usize::try_from(active_n - target).unwrap_or(0);
            let drain = self.with_queue(queue, |_, claims| {
                let mut ranked: Vec<(usize, i32)> = active
                    .iter()
                    .map(|(p, epoch)| (inflight(claims, queue, *p, *epoch), *p))
                    .collect();
                ranked.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
                let mut picked: Vec<i32> =
                    ranked.into_iter().take(excess).map(|(_, p)| p).collect();
                picked.sort_unstable();
                picked
            })?;
            self.set_draining(queue, &drain, true).await?;
            let now = Instant::now();
            self.with_queue(queue, |q, _| {
                for p in &drain {
                    if let Some(l) = q.leases.get_mut(p) {
                        l.draining = true;
                        l.drain_since = now;
                    }
                }
            })?;
            self.release_drained(queue).await?;
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

    /// Renews this process's leases and membership, and counts live members.
    /// A process that no longer wants the queue renews without counting, so
    /// peers take its partitions over while it drains.
    async fn heartbeat(&self, queue: &str, member: bool) -> Result<(Vec<Held>, i32), StoreError> {
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
                     RETURNING partition_id, epoch, draining"
                ),
                &[&queue, &self.owner, &ttl_ms],
            )
            .await?;
        txn.commit().await?;
        Ok((held(&rows), i32::try_from(members).unwrap_or(i32::MAX)))
    }

    /// Takes up to `limit` unowned or lapsed partitions, bumping each epoch.
    async fn acquire(&self, queue: &str, limit: usize) -> Result<Vec<Held>, StoreError> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let client = self.control.get().await?;
        let rows = client
            .query_cached(
                &format!(
                    "WITH picked AS MATERIALIZED (
                        SELECT partition_id FROM lda_partitions
                        WHERE queue = $1 AND (owner = '' OR lease_expires_ms < {DB_NOW_MS})
                        ORDER BY lease_expires_ms, partition_id
                        LIMIT $4 FOR UPDATE SKIP LOCKED
                     )
                     UPDATE lda_partitions p
                     SET owner = $2, epoch = epoch + 1, draining = false,
                         lease_expires_ms = {DB_NOW_MS} + $3
                     FROM picked
                     WHERE p.queue = $1 AND p.partition_id = picked.partition_id
                     RETURNING p.partition_id, p.epoch, p.draining"
                ),
                &[&queue, &self.owner, &self.lease_ttl_ms(), &limit],
            )
            .await?;
        Ok(held(&rows))
    }

    /// Returns requests stamped under an older epoch of this process's
    /// partitions to pending: every partition when `only` is `None`. Rows
    /// another transaction holds are left for a later heartbeat.
    async fn reset_stale(&self, queue: &str, only: Option<&[i32]>) -> Result<(), StoreError> {
        if only.is_some_and(<[i32]>::is_empty) {
            return Ok(());
        }
        let parts: Vec<i32> = only.map(<[i32]>::to_vec).unwrap_or_default();
        let client = self.control.get().await?;
        client
            .execute_cached(
                "WITH stale AS MATERIALIZED (
                    SELECT r.seq FROM lda_requests r
                    JOIN lda_partitions p ON p.queue = r.queue AND p.partition_id = r.partition_id
                    WHERE r.queue = $1 AND r.dispatch_epoch > 0 AND p.owner = $2
                      AND r.dispatch_epoch < p.epoch
                      AND ($3 OR r.partition_id = ANY($4))
                    FOR UPDATE OF r SKIP LOCKED
                 )
                 UPDATE lda_requests SET dispatch_epoch = 0
                 WHERE dispatch_epoch > 0 AND seq IN (SELECT seq FROM stale)",
                &[&queue, &self.owner, &only.is_none(), &parts],
            )
            .await?;
        Ok(())
    }

    async fn set_draining(
        &self,
        queue: &str,
        parts: &[i32],
        draining: bool,
    ) -> Result<(), StoreError> {
        if parts.is_empty() {
            return Ok(());
        }
        let client = self.control.get().await?;
        client
            .execute_cached(
                "UPDATE lda_partitions SET draining = $3
                 WHERE queue = $1 AND owner = $2 AND partition_id = ANY($4)",
                &[&queue, &self.owner, &draining, &parts],
            )
            .await?;
        Ok(())
    }

    /// Releases draining partitions with nothing in flight, or draining
    /// longer than the handoff timeout.
    async fn release_drained(&self, queue: &str) -> Result<(), StoreError> {
        let now = Instant::now();
        let release = self.with_queue(queue, |q, claims| {
            q.leases
                .iter()
                .filter(|(p, l)| {
                    l.draining
                        && (inflight(claims, queue, **p, l.epoch) == 0
                            || now.duration_since(l.drain_since) >= self.handoff_timeout)
                })
                .map(|(p, _)| *p)
                .collect::<Vec<i32>>()
        })?;
        if release.is_empty() {
            return Ok(());
        }
        let client = self.control.get().await?;
        client
            .execute_cached(
                "UPDATE lda_partitions SET owner = '', draining = false, lease_expires_ms = 0
                 WHERE queue = $1 AND owner = $2 AND partition_id = ANY($3)",
                &[&queue, &self.owner, &release],
            )
            .await?;
        self.with_queue(queue, |q, _| {
            for p in &release {
                q.leases.remove(p);
            }
        })?;
        Ok(())
    }

    /// Returns rows stamped for this process that it holds no claim for: a
    /// claim statement that committed but whose reply was lost.
    async fn reconcile(&self, queue: &str) -> Result<(), StoreError> {
        let client = self.control.get().await?;
        let rows = client
            .query_cached(
                "SELECT r.id, r.request_token, r.dispatch_attempt FROM lda_requests r
                 JOIN lda_partitions p ON p.queue = r.queue AND p.partition_id = r.partition_id
                 WHERE r.queue = $1 AND r.dispatch_epoch > 0 AND p.owner = $2
                   AND r.dispatch_epoch = p.epoch",
                &[&queue, &self.owner],
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
                .execute_cached(RELEASE, &[&self.owner, &ids, &tokens, &attempts])
                .await?;
        }
        self.with_queue(queue, |q, _| q.reconcile = false)?;
        Ok(())
    }

    /// Drops a queue nobody here wants once it holds nothing.
    async fn forget_if_idle(&self, queue: &str) -> Result<(), StoreError> {
        let idle = self.with_queue(queue, |q, claims| {
            !q.wanted && q.leases.is_empty() && !claims.values().any(|t| t.queue == queue)
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
            "UPDATE lda_partitions SET owner = '', draining = false, lease_expires_ms = 0
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

    fn lease_ttl_ms(&self) -> i64 {
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
