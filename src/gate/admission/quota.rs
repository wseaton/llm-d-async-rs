use std::sync::Arc;
use std::time::Duration;

use crate::api::request::InternalRequest;
use crate::api::routing::Classification;
use crate::boxed::BoxFuture;
use crate::gate::admission::GatingMode;
use crate::gate::admission::counters::{CounterError, Counters};
use crate::gate::release::Releases;
use crate::gate::{Gate, GateError, Verdict};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QuotaMode {
    /// At most `limit` admissions per tenant in any sliding `window`.
    RateLimit,
    /// At most `limit` requests per tenant in flight.
    Concurrency,
}

impl QuotaMode {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "rate-limit" => Some(Self::RateLimit),
            "concurrency" => Some(Self::Concurrency),
            _ => None,
        }
    }
}

/// Classifies each request as within (`reserved`) or over (`overflow`) its
/// tenant's quota. The tenant is the metadata value under `attribute`;
/// requests without it are admitted untouched. Gates with the same `prefix`
/// and `attribute` count against the same per-tenant keys,
/// `<prefix><attribute>:<tenant>`. A counter that cannot be read is a
/// [`GateError`].
pub struct QuotaGate {
    attribute: String,
    prefix: String,
    mode: QuotaMode,
    gating: GatingMode,
    limit: u32,
    window: Duration,
    counters: Arc<dyn Counters>,
}

impl QuotaGate {
    pub fn new(
        attribute: String,
        prefix: String,
        mode: QuotaMode,
        gating: GatingMode,
        limit: u32,
        window: Duration,
        counters: Arc<dyn Counters>,
    ) -> Self {
        Self {
            attribute,
            prefix,
            mode,
            gating,
            limit,
            window,
            counters,
        }
    }

    async fn acquire(
        &self,
        tenant: &str,
        releases: &mut Releases,
    ) -> Result<Classification, CounterError> {
        let key = format!("{}{}:{tenant}", self.prefix, self.attribute);
        let admitted = match self.mode {
            QuotaMode::Concurrency => match self.counters.acquire_slot(key, self.limit).await {
                Ok(Some(slot)) => {
                    releases.push(move || drop(slot));
                    Ok(true)
                }
                Ok(None) => Ok(false),
                Err(e) => Err(e),
            },
            QuotaMode::RateLimit => self.counters.admit(key, self.limit, self.window).await,
        };
        Ok(if admitted? {
            Classification::Reserved
        } else {
            Classification::Overflow
        })
    }

    async fn decide(
        &self,
        msg: &mut InternalRequest,
        releases: &mut Releases,
    ) -> Result<Verdict, GateError> {
        let Some(tenant) = msg.request.metadata.get(&self.attribute).cloned() else {
            return Ok(Verdict::Continue);
        };
        let class = self.acquire(&tenant, releases).await?;
        msg.routing.set_classification(Some(class));
        Ok(
            if self.gating == GatingMode::Blocking && class == Classification::Overflow {
                Verdict::Refuse
            } else {
                Verdict::Continue
            },
        )
    }
}

impl Gate for QuotaGate {
    fn budget(&self) -> BoxFuture<'_, f64> {
        Box::pin(async { 1.0 })
    }

    fn apply<'a>(
        &'a self,
        msg: &'a mut InternalRequest,
        releases: &'a mut Releases,
    ) -> BoxFuture<'a, Result<Verdict, GateError>> {
        Box::pin(self.decide(msg, releases))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::api::routing::Classification;
    use crate::gate::Verdict;
    use crate::gate::admission::GatingMode;
    use crate::gate::admission::counters::Counters;
    use crate::gate::admission::counters::local::LocalCounters;
    use crate::gate::admission::quota::{QuotaGate, QuotaMode};
    use crate::gate::release::Releases;
    use crate::gate::test_support::request;

    fn gate(mode: QuotaMode, gating: GatingMode, counters: Arc<dyn Counters>) -> QuotaGate {
        QuotaGate::new(
            "userid".into(),
            "quota:".into(),
            mode,
            gating,
            2,
            Duration::from_secs(10),
            counters,
        )
    }

    fn local() -> Arc<dyn Counters> {
        Arc::new(LocalCounters::default())
    }

    #[tokio::test]
    async fn requests_without_the_attribute_pass_untouched() {
        let g = gate(QuotaMode::Concurrency, GatingMode::Blocking, local());
        let mut msg = request(&[("tenant", "a")]);
        assert_eq!(
            g.decide(&mut msg, &mut Releases::default()).await.unwrap(),
            Verdict::Continue
        );
        assert_eq!(msg.routing.classification(), None);
    }

    #[tokio::test]
    async fn concurrency_blocking_refuses_over_limit_and_frees_on_release() {
        let g = gate(QuotaMode::Concurrency, GatingMode::Blocking, local());
        let mut held = Releases::default();
        for _ in 0..2 {
            let mut msg = request(&[("userid", "a")]);
            assert_eq!(
                g.decide(&mut msg, &mut held).await.unwrap(),
                Verdict::Continue
            );
            assert_eq!(msg.routing.classification(), Some(Classification::Reserved));
        }
        let mut msg = request(&[("userid", "a")]);
        assert_eq!(
            g.decide(&mut msg, &mut Releases::default()).await.unwrap(),
            Verdict::Refuse
        );
        assert_eq!(msg.routing.classification(), Some(Classification::Overflow));
        let mut other = request(&[("userid", "b")]);
        assert_eq!(
            g.decide(&mut other, &mut Releases::default())
                .await
                .unwrap(),
            Verdict::Continue
        );
        drop(held);
        let mut msg = request(&[("userid", "a")]);
        assert_eq!(
            g.decide(&mut msg, &mut Releases::default()).await.unwrap(),
            Verdict::Continue
        );
    }

    #[tokio::test]
    async fn classifying_admits_overflow() {
        let g = gate(QuotaMode::Concurrency, GatingMode::Classifying, local());
        let mut held = Releases::default();
        for want in [
            Classification::Reserved,
            Classification::Reserved,
            Classification::Overflow,
        ] {
            let mut msg = request(&[("userid", "a")]);
            assert_eq!(
                g.decide(&mut msg, &mut held).await.unwrap(),
                Verdict::Continue
            );
            assert_eq!(msg.routing.classification(), Some(want));
        }
    }

    #[tokio::test]
    async fn rate_limit_refuses_past_the_limit() {
        let g = gate(QuotaMode::RateLimit, GatingMode::Blocking, local());
        let mut r = Releases::default();
        assert_eq!(
            g.decide(&mut request(&[("userid", "a")]), &mut r)
                .await
                .unwrap(),
            Verdict::Continue
        );
        assert_eq!(
            g.decide(&mut request(&[("userid", "a")]), &mut r)
                .await
                .unwrap(),
            Verdict::Continue
        );
        assert_eq!(
            g.decide(&mut request(&[("userid", "a")]), &mut r)
                .await
                .unwrap(),
            Verdict::Refuse
        );
        assert!(r.is_empty());
    }

    #[tokio::test]
    async fn gates_with_one_prefix_share_the_quota() {
        let counters = local();
        let a = gate(
            QuotaMode::Concurrency,
            GatingMode::Blocking,
            Arc::clone(&counters),
        );
        let b = gate(
            QuotaMode::Concurrency,
            GatingMode::Blocking,
            Arc::clone(&counters),
        );
        let mut held = Releases::default();
        a.decide(&mut request(&[("userid", "t")]), &mut held)
            .await
            .unwrap();
        b.decide(&mut request(&[("userid", "t")]), &mut held)
            .await
            .unwrap();
        assert_eq!(
            a.decide(&mut request(&[("userid", "t")]), &mut Releases::default())
                .await
                .unwrap(),
            Verdict::Refuse
        );
        let other_prefix = QuotaGate::new(
            "userid".into(),
            "other:".into(),
            QuotaMode::Concurrency,
            GatingMode::Blocking,
            2,
            Duration::from_secs(10),
            counters,
        );
        assert_eq!(
            other_prefix
                .decide(&mut request(&[("userid", "t")]), &mut Releases::default())
                .await
                .unwrap(),
            Verdict::Continue
        );
    }

    #[tokio::test]
    async fn gates_with_one_prefix_and_different_attributes_count_apart() {
        let counters = local();
        let by_user = gate(
            QuotaMode::Concurrency,
            GatingMode::Blocking,
            Arc::clone(&counters),
        );
        let by_team = QuotaGate::new(
            "team".into(),
            "quota:".into(),
            QuotaMode::Concurrency,
            GatingMode::Blocking,
            2,
            Duration::from_secs(10),
            Arc::clone(&counters),
        );
        let mut held = Releases::default();
        for _ in 0..2 {
            by_user
                .decide(&mut request(&[("userid", "x")]), &mut held)
                .await
                .unwrap();
        }
        assert_eq!(
            by_user
                .decide(&mut request(&[("userid", "x")]), &mut Releases::default())
                .await
                .unwrap(),
            Verdict::Refuse
        );
        assert_eq!(
            by_team
                .decide(&mut request(&[("team", "x")]), &mut Releases::default())
                .await
                .unwrap(),
            Verdict::Continue
        );
    }

    #[tokio::test]
    async fn a_failing_counter_is_a_gate_error_through_every_combinator() {
        use std::sync::atomic::Ordering;

        use crate::gate::combinator::composite::CompositeGate;
        use crate::gate::combinator::wait_on_refuse::WaitOnRefuseGate;
        use crate::gate::test_support::FixedGate;
        use crate::gate::{Gate, GateError, SharedGate};

        let Some(f) = crate::store::postgres::test_support::fixture().await else {
            return;
        };
        let quota: SharedGate = Arc::new(gate(
            QuotaMode::Concurrency,
            GatingMode::Classifying,
            Arc::new(f.pg.counters()),
        ));
        f.db.pool.close();

        let mut msg = request(&[("userid", "a")]);
        let failed = quota.apply(&mut msg, &mut Releases::default()).await;
        assert!(matches!(failed, Err(GateError::Counter(_))), "{failed:?}");
        assert_eq!(msg.routing.classification(), None, "not classified");

        let waiting = WaitOnRefuseGate::new(Arc::clone(&quota));
        let failed = waiting
            .apply(&mut request(&[("userid", "a")]), &mut Releases::default())
            .await;
        assert!(failed.is_err(), "{failed:?}");

        let before = Arc::new(FixedGate::new(1.0, Verdict::Continue));
        let chain = CompositeGate::new(vec![before.clone(), quota]);
        let mut releases = Releases::default();
        let failed = chain
            .apply(&mut request(&[("userid", "a")]), &mut releases)
            .await;
        assert!(failed.is_err(), "{failed:?}");
        assert_eq!(before.held.load(Ordering::SeqCst), 0, "chain released");
    }
}
