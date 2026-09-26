//! Durable queue state behind [`QueueStore`].
//!
//! Two backends implement it:
//!
//! - [`embedded`]: redb in a local directory. One process owns it, so a claim
//!   is a plain row and every claim a dead process held returns to its queue
//!   when the store opens.
//! - [`postgres`]: shared by any number of replicas. Each queue hashes into
//!   partitions that replicas lease; a claim is fenced by its partition lease
//!   and a per-dispatch attempt number.
//!
//! Request and result bodies live apart from the queue in a [`BlobStore`].

pub mod blob;
pub mod config;
pub mod embedded;
pub mod error;
pub mod postgres;
pub mod queue;
pub mod signal;
pub mod staging;

#[cfg(test)]
pub(crate) mod conformance;
#[cfg(test)]
pub(crate) mod test_support;

use std::sync::Arc;

use crate::api::dispatch_rate::DispatchRateLimit;
use crate::api::request::InternalRequest;
use crate::boxed::BoxFuture;
use crate::store::blob::key::BlobKey;
use crate::store::blob::{BlobBody, BlobStore};
use crate::store::error::StoreError;
use crate::store::queue::{
    AckOutcome, Admission, Admitted, Applied, Backlog, NewRequest, Outcome, PayloadBody, Peeked,
    RequestStatus, ResultClaim,
};

pub type Store = Arc<dyn QueueStore>;

pub trait QueueStore: Send + Sync {
    fn blobs(&self) -> &BlobStore;

    /// Readiness probe.
    fn ping(&self) -> BoxFuture<'_, Result<(), StoreError>>;

    /// Starts consuming `queue`, then tries to take this process's share of
    /// it so the next [`QueueStore::peek`] can see requests. After an error
    /// the queue stays joined and the backend keeps trying.
    fn join(&self, queue: &str) -> BoxFuture<'_, Result<(), StoreError>>;

    /// Stops consuming `queue`. Claims already taken stay valid until they
    /// end.
    fn leave(&self, queue: &str);

    /// Enqueues requests whose payloads are already staged, all or none.
    fn submit(&self, requests: Vec<NewRequest>) -> BoxFuture<'_, Result<(), StoreError>>;

    /// Returns up to `limit` requests at the head of `queue` that this
    /// process may claim, without claiming them. Never reads payloads.
    fn peek(
        &self,
        queue: String,
        limit: usize,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<Vec<Peeked>, StoreError>>;

    /// Whether any request of `queue` is waiting, whoever may claim it.
    fn has_pending(&self, queue: String, now_ms: i64) -> BoxFuture<'_, Result<bool, StoreError>>;

    /// Applies a consumer's decisions for peeked rows of `queue` in one
    /// transaction.
    fn admit(
        &self,
        queue: String,
        admissions: Vec<Admission>,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<Vec<Admitted>, StoreError>>;

    /// Ends claims. Outcomes whose claim is not current are fenced; a result
    /// body they reference is deleted.
    fn apply_outcomes(
        &self,
        outcomes: Arc<[Outcome]>,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<Applied, StoreError>>;

    /// Moves up to `limit` retries due at `now_ms` back to their queues and
    /// returns how many moved. A backend that keeps retries in their queue
    /// until due has nothing to move.
    fn promote_due_retries(
        &self,
        now_ms: i64,
        limit: usize,
    ) -> BoxFuture<'_, Result<usize, StoreError>>;

    /// Opens the body of a claimed request. `None` when it is missing.
    fn open_payload<'a>(
        &'a self,
        envelope: &'a InternalRequest,
    ) -> BoxFuture<'a, Result<Option<PayloadBody>, StoreError>>;

    /// Whether this generation of a request was cancelled.
    fn is_cancelled(
        &self,
        id: String,
        token: String,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<bool, StoreError>>;

    /// Where the generation `token` of request `id` stands, or without a
    /// token its live generation.
    fn request_status(
        &self,
        id: String,
        token: Option<String>,
    ) -> BoxFuture<'_, Result<RequestStatus, StoreError>>;

    /// Marks the live generation of each ID cancelled. Unknown, finished and
    /// expired IDs are a no-op. Returns how many IDs were marked.
    fn cancel(&self, ids: Vec<String>, now_ms: i64) -> BoxFuture<'_, Result<usize, StoreError>>;

    /// Depth of `queue` plus cumulative counts of requests whose deadline is
    /// within each bound (seconds from `now_ms`).
    fn backlog(
        &self,
        queue: String,
        now_ms: i64,
        bounds_s: Vec<i64>,
    ) -> BoxFuture<'_, Result<Backlog, StoreError>>;

    /// Destructively takes the oldest result of `route`. A body blob it
    /// references stays readable for the result blob retention.
    fn pop_result(
        &self,
        route: String,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<Option<String>, StoreError>>;

    /// Leases the oldest result of `route` to `owner`. The result stays
    /// durable until acknowledged and returns to the route if the lease lapses.
    fn claim_result(
        &self,
        route: String,
        owner: String,
        lease_ms: i64,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<Option<ResultClaim>, StoreError>>;

    /// Extends a live lease. False when `owner` no longer holds it.
    fn renew_result(
        &self,
        route: String,
        claim_id: u64,
        owner: String,
        lease_ms: i64,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<bool, StoreError>>;

    /// Acknowledges a claimed result and deletes its body blob. Repeating a
    /// successful ack is safe.
    fn ack_result(
        &self,
        route: String,
        claim_id: u64,
        owner: String,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<AckOutcome, StoreError>>;

    /// Opens a result body with its content type. `None` when it is unknown,
    /// expired, or already deleted.
    fn open_result_blob(
        &self,
        key: BlobKey,
        now_ms: i64,
    ) -> BoxFuture<'_, Result<Option<(BlobBody, String)>, StoreError>>;

    /// Results waiting on `route`, excluding leased ones.
    fn result_depth(&self, route: String, now_ms: i64) -> BoxFuture<'_, Result<u64, StoreError>>;

    /// A control-plane value, stamped with the store's clock when it was read.
    fn kv_get(&self, key: String) -> BoxFuture<'_, Result<Stamped<Option<Vec<u8>>>, StoreError>>;

    /// Sets (`Some`) or clears (`None`) a control-plane value.
    fn kv_put(&self, key: String, value: Option<Vec<u8>>) -> BoxFuture<'_, Result<(), StoreError>>;

    /// Deletes expired markers, results, tombstones, and blobs. Returns rows
    /// removed.
    fn sweep(&self, now_ms: i64) -> BoxFuture<'_, Result<u64, StoreError>>;

    /// Deletes blobs last written at or before `cutoff_ms` that nothing
    /// references. Returns how many.
    fn collect_orphans(&self, cutoff_ms: i64) -> BoxFuture<'_, Result<usize, StoreError>>;

    /// Gives up everything this process holds so others can take it over.
    /// Call once every outcome is written.
    fn close(&self) -> BoxFuture<'_, Result<(), StoreError>>;
}

/// A value read from a store, with the store's clock at the read. Replicas
/// sharing a store agree on this clock; their own clocks may drift.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stamped<T> {
    pub value: T,
    pub now_ms: i64,
}

fn budget_key(key: &str) -> String {
    format!("budget/{key}")
}

fn dispatch_rate_key(key: &str) -> String {
    format!("dispatch-rate/{key}")
}

impl dyn QueueStore {
    /// The raw budget value, as the operator wrote it.
    pub async fn budget(&self, key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(self.kv_get(budget_key(key)).await?.value)
    }

    /// Sets (`Some`) or clears (`None`) a budget value.
    pub async fn set_budget(&self, key: &str, value: Option<Vec<u8>>) -> Result<(), StoreError> {
        self.kv_put(budget_key(key), value).await
    }

    /// The stored dispatch-rate command, stamped with the store's clock so
    /// every replica judges its expiry alike. `Err` in the inner result means
    /// the stored bytes do not decode, which gates must treat as fail-closed.
    pub async fn dispatch_rate(
        &self,
        key: &str,
    ) -> Result<Stamped<Option<Result<DispatchRateLimit, serde_json::Error>>>, StoreError> {
        let read = self.kv_get(dispatch_rate_key(key)).await?;
        Ok(Stamped {
            value: read.value.map(|bytes| serde_json::from_slice(&bytes)),
            now_ms: read.now_ms,
        })
    }

    pub async fn set_dispatch_rate(
        &self,
        key: &str,
        limit: Option<&DispatchRateLimit>,
    ) -> Result<(), StoreError> {
        let value = limit.map(serde_json::to_vec).transpose()?;
        self.kv_put(dispatch_rate_key(key), value).await
    }
}

/// Holds a queue joined until dropped.
pub struct Membership {
    store: Store,
    queue: String,
}

impl Membership {
    pub async fn join(store: Store, queue: String) -> Self {
        let membership = Self { store, queue };
        if let Err(e) = membership.store.join(&membership.queue).await {
            tracing::warn!(queue = %membership.queue, error = %e, "failed to take a share of the queue; retrying in the background");
        }
        membership
    }
}

impl Drop for Membership {
    fn drop(&mut self) {
        self.store.leave(&self.queue);
    }
}
