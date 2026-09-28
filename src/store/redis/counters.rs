//! Admission counters in Redis. Concurrency slots and rate windows use
//! upstream llm-d-async's `redis-quota` scripts on the same keys, so a quota
//! gate here, a Go dispatcher's quota gate and the llm-d-router
//! coordinator's passthrough quota count against one another.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ::redis::Script;
use ::redis::aio::ConnectionManager;

use crate::boxed::BoxFuture;
use crate::clock::now_millis;
use crate::gate::admission::counters::{BucketSpec, CounterError, Counters, Slot};
use crate::store::error::StoreError;
use crate::store::redis::scripts::{QUOTA_ACQUIRE, QUOTA_RELEASE, QUOTA_WINDOW, TAKE_TOKEN};

struct CounterScripts {
    acquire: Script,
    release: Script,
    window: Script,
    bucket: Script,
}

#[derive(Clone)]
pub struct RedisCounters {
    conn: ConnectionManager,
    /// How long a slot counter outlives its last acquire or release. It is
    /// the only thing that frees slots a crashed process held.
    slot_ttl_s: u64,
    scripts: Arc<CounterScripts>,
}

fn store_error(e: ::redis::RedisError) -> CounterError {
    CounterError::Store(StoreError::from(e))
}

fn now_nanos() -> i128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i128::try_from(d.as_nanos()).unwrap_or(i128::MAX))
}

impl RedisCounters {
    pub fn new(conn: ConnectionManager, slot_ttl: Duration) -> Self {
        Self {
            conn,
            slot_ttl_s: slot_ttl.as_secs().max(1),
            scripts: Arc::new(CounterScripts {
                acquire: Script::new(QUOTA_ACQUIRE),
                release: Script::new(QUOTA_RELEASE),
                window: Script::new(QUOTA_WINDOW),
                bucket: Script::new(TAKE_TOKEN),
            }),
        }
    }

    fn release_later(&self, key: String) -> Slot {
        let counters = self.clone();
        Slot::new(move || {
            let Ok(runtime) = tokio::runtime::Handle::try_current() else {
                tracing::warn!(%key, "slot dropped outside a runtime; its counter frees when it expires");
                return;
            };
            runtime.spawn(async move {
                let released: Result<(), _> = counters
                    .scripts
                    .release
                    .key(&key)
                    .arg(counters.slot_ttl_s)
                    .invoke_async(&mut counters.conn.clone())
                    .await;
                if let Err(e) = released {
                    tracing::warn!(%key, error = %e, "releasing a slot failed; its counter frees when it expires");
                }
            });
        })
    }
}

impl Counters for RedisCounters {
    fn acquire_slot(
        &self,
        key: String,
        limit: u32,
    ) -> BoxFuture<'_, Result<Option<Slot>, CounterError>> {
        Box::pin(async move {
            let taken: i64 = self
                .scripts
                .acquire
                .key(&key)
                .arg(limit)
                .arg(self.slot_ttl_s)
                .invoke_async(&mut self.conn.clone())
                .await
                .map_err(store_error)?;
            Ok((taken == 1).then(|| self.release_later(key)))
        })
    }

    fn admit(
        &self,
        key: String,
        limit: u32,
        window: Duration,
    ) -> BoxFuture<'_, Result<bool, CounterError>> {
        Box::pin(async move {
            let now = now_nanos();
            let window_ns = i128::try_from(window.as_nanos()).unwrap_or(i128::MAX);
            let ttl_s = (window.as_secs() * 2).max(1);
            let admitted: i64 = self
                .scripts
                .window
                .key(format!("{key}:{}", window.as_millis()))
                .arg((now - window_ns).to_string())
                .arg(limit)
                .arg(now.to_string())
                .arg(ttl_s)
                .invoke_async(&mut self.conn.clone())
                .await
                .map_err(store_error)?;
            Ok(admitted == 1)
        })
    }

    fn take_token(
        &self,
        key: String,
        bucket: BucketSpec,
    ) -> BoxFuture<'_, Result<bool, CounterError>> {
        Box::pin(async move {
            let taken: i64 = self
                .scripts
                .bucket
                .key(&key)
                .arg(bucket.rate)
                .arg(bucket.capacity)
                .arg(bucket.expires_ms)
                .arg(now_millis())
                .invoke_async(&mut self.conn.clone())
                .await
                .map_err(store_error)?;
            Ok(taken == 1)
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::gate::admission::counters::conformance;
    use crate::store::redis::test_support::counters;

    macro_rules! redis_counters_conformance {
        ($($name:ident),* $(,)?) => {
            $(
                #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
                async fn $name() {
                    let Some(f) = counters().await else {
                        return;
                    };
                    conformance::$name(f.counters.clone()).await;
                }
            )*
        };
    }

    redis_counters_conformance!(
        slots_are_limited_per_key_and_given_back,
        concurrent_acquires_never_exceed_the_limit,
        rate_windows_count_per_window_and_slide,
        buckets_drain_and_reset_after_expiry,
    );
}
