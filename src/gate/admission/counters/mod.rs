//! Admission counters shared by every gate that names the same key: quota
//! slots, sliding rate windows, and token buckets. [`local::LocalCounters`]
//! counts within one process; the Postgres store counts across replicas.

pub mod local;

#[cfg(test)]
pub(crate) mod conformance;

use std::time::Duration;

use crate::boxed::BoxFuture;
use crate::store::error::StoreError;

#[derive(Debug, thiserror::Error)]
pub enum CounterError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("counter statement abandoned: {0}")]
    Abandoned(String),
}

/// A held concurrency slot, given back when dropped.
pub struct Slot(Option<Box<dyn FnOnce() + Send>>);

impl Slot {
    pub fn new(release: impl FnOnce() + Send + 'static) -> Self {
        Self(Some(Box::new(release)))
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        if let Some(release) = self.0.take() {
            release();
        }
    }
}

/// A token bucket refilled at `rate` per second up to `capacity`, which
/// starts full again once `expires_ms` passes.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BucketSpec {
    pub rate: f64,
    pub capacity: f64,
    pub expires_ms: i64,
}

pub trait Counters: Send + Sync {
    /// Takes one of `limit` concurrent slots of `key`.
    fn acquire_slot(
        &self,
        key: String,
        limit: u32,
    ) -> BoxFuture<'_, Result<Option<Slot>, CounterError>>;

    /// Admits one request for `key` while fewer than `limit` were admitted in
    /// the trailing `window`. Each window length keeps its own log.
    fn admit(
        &self,
        key: String,
        limit: u32,
        window: Duration,
    ) -> BoxFuture<'_, Result<bool, CounterError>>;

    /// Takes one token from the bucket `key`.
    fn take_token(
        &self,
        key: String,
        bucket: BucketSpec,
    ) -> BoxFuture<'_, Result<bool, CounterError>>;
}
