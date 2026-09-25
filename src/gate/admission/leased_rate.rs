use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::api::dispatch_rate::DispatchRateLimit;
use crate::api::request::InternalRequest;
use crate::clock::now_millis;
use crate::gate::release::Releases;
use crate::gate::{BoxFuture, Gate, Verdict};
use crate::store::Store;
use crate::telemetry::metrics::Metrics;

#[derive(Debug, Clone, Copy)]
struct Bucket {
    tokens: f64,
    last_ms: i64,
    /// The bucket starts full again after this, one second past the lease
    /// that last filled it.
    expires_ms: i64,
}

/// Token buckets shared by every leased-rate gate with the same state key.
#[derive(Default)]
pub struct Buckets(Mutex<HashMap<String, Bucket>>);

/// Enforces a pool-wide dispatch rate leased by an external controller.
///
/// A missing, malformed, mismatched, expired or unreadable lease fails closed
/// (`Refuse`). Wrap it in `wait-on-refuse` at pool level so a controller
/// outage parks workers instead of failing requests.
pub struct LeasedRateGate {
    store: Store,
    control_key: String,
    state_key: String,
    pool_id: String,
    burst_seconds: f64,
    buckets: Arc<Buckets>,
    metrics: Arc<Metrics>,
}

#[derive(Debug, PartialEq)]
enum LeaseError {
    Missing,
    Invalid(String),
}

impl LeasedRateGate {
    pub fn new(
        store: Store,
        control_key: String,
        state_key: String,
        pool_id: String,
        burst_seconds: f64,
        buckets: Arc<Buckets>,
        metrics: Arc<Metrics>,
    ) -> Self {
        metrics.drain_limit(&pool_id, None);
        Self {
            store,
            control_key,
            state_key,
            pool_id,
            burst_seconds,
            buckets,
            metrics,
        }
    }

    async fn lease(&self, now_ms: i64) -> Result<DispatchRateLimit, LeaseError> {
        let stored = self
            .store
            .dispatch_rate(&self.control_key)
            .await
            .map_err(|e| LeaseError::Invalid(e.to_string()))?;
        let limit = stored
            .ok_or(LeaseError::Missing)?
            .map_err(|e| LeaseError::Invalid(e.to_string()))?;
        limit
            .validate_at(now_ms)
            .map_err(|e| LeaseError::Invalid(e.to_string()))?;
        if limit.pool_id != self.pool_id {
            return Err(LeaseError::Invalid(format!(
                "dispatch-rate command is for pool {:?}, want {:?}",
                limit.pool_id, self.pool_id
            )));
        }
        Ok(limit)
    }

    async fn observed_lease(&self, now_ms: i64) -> Option<DispatchRateLimit> {
        match self.lease(now_ms).await {
            Ok(limit) => {
                self.metrics.drain_limit(
                    &self.pool_id,
                    Some((limit.max_admission_rps, limit.valid_until_unix_millis)),
                );
                Some(limit)
            }
            Err(e) => {
                if let LeaseError::Invalid(reason) = e {
                    tracing::warn!(pool = %self.pool_id, %reason, "dispatch-rate lease unusable");
                }
                self.metrics.drain_limit(&self.pool_id, None);
                None
            }
        }
    }

    /// Takes one token if available.
    fn take(&self, rate: f64, valid_until_ms: i64, now_ms: i64) -> bool {
        let capacity = (rate * self.burst_seconds).max(1.0);
        if !capacity.is_finite() {
            return false;
        }
        let Ok(mut buckets) = self.buckets.0.lock() else {
            return false;
        };
        let bucket = buckets.entry(self.state_key.clone()).or_insert(Bucket {
            tokens: capacity,
            last_ms: now_ms,
            expires_ms: 0,
        });
        if now_ms >= bucket.expires_ms {
            bucket.tokens = capacity;
            bucket.last_ms = now_ms;
        }
        let now_ms = now_ms.max(bucket.last_ms);
        let elapsed_s = (now_ms - bucket.last_ms) as f64 / 1000.0;
        bucket.tokens = (bucket.tokens + elapsed_s * rate).min(capacity);
        bucket.last_ms = now_ms;
        bucket.expires_ms = valid_until_ms.saturating_add(1000);
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

impl Gate for LeasedRateGate {
    /// Whether a positive lease is active. The rate itself is enforced by
    /// `apply`.
    fn budget(&self) -> BoxFuture<'_, f64> {
        Box::pin(async move {
            match self.observed_lease(now_millis()).await {
                Some(limit) if limit.max_admission_rps > 0.0 => 1.0,
                _ => 0.0,
            }
        })
    }

    fn apply<'a>(
        &'a self,
        _msg: &'a mut InternalRequest,
        _releases: &'a mut Releases,
    ) -> BoxFuture<'a, Verdict> {
        Box::pin(async move {
            let now_ms = now_millis();
            let Some(limit) = self.observed_lease(now_ms).await else {
                return Verdict::Refuse;
            };
            if limit.max_admission_rps == 0.0 {
                return Verdict::Refuse;
            }
            if self.take(
                limit.max_admission_rps,
                limit.valid_until_unix_millis,
                now_ms,
            ) {
                Verdict::Continue
            } else {
                Verdict::Refuse
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::api::dispatch_rate::{API_VERSION, DispatchRateLimit};
    use crate::clock::now_millis;
    use crate::gate::admission::leased_rate::{Buckets, LeaseError, LeasedRateGate};
    use crate::gate::release::Releases;
    use crate::gate::test_support::request;
    use crate::gate::{Gate, Verdict};
    use crate::store::Store;
    use crate::store::test_support::open;
    use crate::telemetry::metrics::Metrics;

    fn gate(store: &Store, burst: f64) -> LeasedRateGate {
        LeasedRateGate::new(
            store.clone(),
            "ctl".into(),
            "ctl:state".into(),
            "pool".into(),
            burst,
            Arc::new(Buckets::default()),
            Arc::new(Metrics::new().unwrap()),
        )
    }

    fn lease(rps: f64) -> DispatchRateLimit {
        DispatchRateLimit {
            api_version: API_VERSION.into(),
            pool_id: "pool".into(),
            max_admission_rps: rps,
            valid_until_unix_millis: now_millis() + 60_000,
            decision_id: "d1".into(),
        }
    }

    #[tokio::test]
    async fn fails_closed_without_a_valid_lease() {
        let (_dir, store) = open();
        let g = gate(&store, 1.0);
        assert_eq!(g.lease(now_millis()).await, Err(LeaseError::Missing));
        assert_eq!(g.budget().await, 0.0);
        assert_eq!(
            g.apply(&mut request(&[]), &mut Releases::default()).await,
            Verdict::Refuse
        );

        let other_pool = DispatchRateLimit {
            pool_id: "elsewhere".into(),
            ..lease(5.0)
        };
        store
            .set_dispatch_rate("ctl", Some(&other_pool))
            .await
            .unwrap();
        assert_eq!(g.budget().await, 0.0);

        let expired = DispatchRateLimit {
            valid_until_unix_millis: now_millis() - 1,
            ..lease(5.0)
        };
        store
            .set_dispatch_rate("ctl", Some(&expired))
            .await
            .unwrap();
        assert_eq!(
            g.apply(&mut request(&[]), &mut Releases::default()).await,
            Verdict::Refuse
        );

        store
            .set_dispatch_rate("ctl", Some(&lease(0.0)))
            .await
            .unwrap();
        assert_eq!(g.budget().await, 0.0);
        assert_eq!(
            g.apply(&mut request(&[]), &mut Releases::default()).await,
            Verdict::Refuse
        );
    }

    #[tokio::test]
    async fn admits_a_burst_then_refuses() {
        let (_dir, store) = open();
        store
            .set_dispatch_rate("ctl", Some(&lease(3.0)))
            .await
            .unwrap();
        let g = gate(&store, 1.0);
        assert_eq!(g.budget().await, 1.0);
        let mut admitted = 0;
        for _ in 0..10 {
            if g.apply(&mut request(&[]), &mut Releases::default()).await == Verdict::Continue {
                admitted += 1;
            }
        }
        // A 1s bucket at 3 rps holds 3 tokens; refill in this loop is well under one.
        assert_eq!(admitted, 3);
    }

    #[test]
    fn bucket_refills_at_the_leased_rate_and_never_runs_backwards() {
        let dir = tempfile::tempdir().unwrap();
        let (store, _) = Store::open(dir.path(), &crate::store::test_support::options()).unwrap();
        let g = gate(&store, 1.0);
        let until = 1_000_000;
        assert!(g.take(2.0, until, 0));
        assert!(g.take(2.0, until, 0));
        assert!(!g.take(2.0, until, 0));
        assert!(!g.take(2.0, until, 499));
        assert!(g.take(2.0, until, 510));
        // A clock step back must not refill the same interval twice.
        assert!(!g.take(2.0, until, 100));
        assert!(!g.take(2.0, until, 510));
        assert!(g.take(2.0, until, 1010));
        // Past the lease plus a second, the bucket starts full.
        assert!(g.take(2.0, until, until + 1000));
        assert!(g.take(2.0, until, until + 1000));
    }

    #[test]
    fn small_rates_still_allow_one_request() {
        let dir = tempfile::tempdir().unwrap();
        let (store, _) = Store::open(dir.path(), &crate::store::test_support::options()).unwrap();
        let g = gate(&store, 0.1);
        assert!(g.take(0.5, 1_000_000, 0));
        assert!(!g.take(0.5, 1_000_000, 1_000));
        assert!(g.take(0.5, 1_000_000, 2_000));
    }
}
