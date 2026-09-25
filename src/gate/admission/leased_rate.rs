use std::sync::Arc;

use crate::api::dispatch_rate::DispatchRateLimit;
use crate::api::request::InternalRequest;
use crate::boxed::BoxFuture;
use crate::gate::admission::counters::{BucketSpec, Counters};
use crate::gate::release::Releases;
use crate::gate::{Gate, GateError, Verdict};
use crate::store::Store;
use crate::telemetry::metrics::Metrics;

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
    counters: Arc<dyn Counters>,
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
        counters: Arc<dyn Counters>,
        metrics: Arc<Metrics>,
    ) -> Self {
        metrics.drain_limit(&pool_id, None);
        Self {
            store,
            control_key,
            state_key,
            pool_id,
            burst_seconds,
            counters,
            metrics,
        }
    }

    /// The lease, checked against the store's clock.
    async fn lease(&self) -> Result<DispatchRateLimit, LeaseError> {
        let stored = self
            .store
            .dispatch_rate(&self.control_key)
            .await
            .map_err(|e| LeaseError::Invalid(e.to_string()))?;
        let now_ms = stored.now_ms;
        let limit = stored
            .value
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

    async fn observed_lease(&self) -> Option<DispatchRateLimit> {
        match self.lease().await {
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

    /// Takes one token from the bucket every gate with this state key
    /// shares. An unreadable bucket refuses.
    async fn take(&self, rate: f64, valid_until_ms: i64) -> bool {
        let bucket = BucketSpec {
            rate,
            capacity: (rate * self.burst_seconds).max(1.0),
            expires_ms: valid_until_ms.saturating_add(1000),
        };
        match self
            .counters
            .take_token(self.state_key.clone(), bucket)
            .await
        {
            Ok(taken) => taken,
            Err(e) => {
                tracing::warn!(pool = %self.pool_id, error = %e, "dispatch-rate bucket unavailable");
                false
            }
        }
    }
}

impl Gate for LeasedRateGate {
    /// Whether a positive lease is active. The rate itself is enforced by
    /// `apply`.
    fn budget(&self) -> BoxFuture<'_, f64> {
        Box::pin(async move {
            match self.observed_lease().await {
                Some(limit) if limit.max_admission_rps > 0.0 => 1.0,
                _ => 0.0,
            }
        })
    }

    fn apply<'a>(
        &'a self,
        _msg: &'a mut InternalRequest,
        _releases: &'a mut Releases,
    ) -> BoxFuture<'a, Result<Verdict, GateError>> {
        Box::pin(async move {
            let Some(limit) = self.observed_lease().await else {
                return Ok(Verdict::Refuse);
            };
            if limit.max_admission_rps == 0.0 {
                return Ok(Verdict::Refuse);
            }
            if self
                .take(limit.max_admission_rps, limit.valid_until_unix_millis)
                .await
            {
                Ok(Verdict::Continue)
            } else {
                Ok(Verdict::Refuse)
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::api::dispatch_rate::{API_VERSION, DispatchRateLimit};
    use crate::clock::now_millis;
    use crate::gate::admission::counters::local::LocalCounters;
    use crate::gate::admission::leased_rate::{LeaseError, LeasedRateGate};
    use crate::gate::release::Releases;
    use crate::gate::test_support::request;
    use crate::gate::{Gate, Verdict};
    use crate::store::Store;
    use crate::store::embedded::test_support::open;
    use crate::telemetry::metrics::Metrics;

    fn gate(store: &Store, burst: f64) -> LeasedRateGate {
        LeasedRateGate::new(
            store.clone(),
            "ctl".into(),
            "ctl:state".into(),
            "pool".into(),
            burst,
            Arc::new(LocalCounters::default()),
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
        let fixture = open().await;
        let store = fixture.store.clone();
        let g = gate(&store, 1.0);
        assert_eq!(g.lease().await, Err(LeaseError::Missing));
        assert_eq!(g.budget().await, 0.0);
        assert_eq!(
            g.apply(&mut request(&[]), &mut Releases::default())
                .await
                .unwrap(),
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
            g.apply(&mut request(&[]), &mut Releases::default())
                .await
                .unwrap(),
            Verdict::Refuse
        );

        store
            .set_dispatch_rate("ctl", Some(&lease(0.0)))
            .await
            .unwrap();
        assert_eq!(g.budget().await, 0.0);
        assert_eq!(
            g.apply(&mut request(&[]), &mut Releases::default())
                .await
                .unwrap(),
            Verdict::Refuse
        );
    }

    #[tokio::test]
    async fn admits_a_burst_then_refuses() {
        let fixture = open().await;
        let store = fixture.store.clone();
        store
            .set_dispatch_rate("ctl", Some(&lease(3.0)))
            .await
            .unwrap();
        let g = gate(&store, 1.0);
        assert_eq!(g.budget().await, 1.0);
        let mut admitted = 0;
        for _ in 0..10 {
            if g.apply(&mut request(&[]), &mut Releases::default())
                .await
                .unwrap()
                == Verdict::Continue
            {
                admitted += 1;
            }
        }
        // A 1s bucket at 3 rps holds 3 tokens; refill in this loop is well under one.
        assert_eq!(admitted, 3);
    }
}
