use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::api::request::InternalRequest;
use crate::api::routing::Classification;
use crate::gate::admission::GatingMode;
use crate::gate::release::Releases;
use crate::gate::{BoxFuture, Gate, Verdict};

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

/// Per-tenant counters. Every quota gate built with the same prefix and mode
/// shares one, so several queues can draw on one tenant quota.
#[derive(Default)]
pub struct QuotaState {
    in_flight: Mutex<HashMap<String, usize>>,
    admissions: Mutex<RateWindows>,
}

#[derive(Default)]
struct RateWindows {
    by_tenant: HashMap<String, VecDeque<Instant>>,
    last_sweep: Option<Instant>,
}

impl RateWindows {
    fn admit(&mut self, tenant: &str, limit: usize, window: Duration, now: Instant) -> bool {
        let cutoff = now.checked_sub(window);
        let prune = |q: &mut VecDeque<Instant>| {
            while q.front().is_some_and(|t| Some(*t) <= cutoff) {
                q.pop_front();
            }
        };
        if self
            .last_sweep
            .is_none_or(|at| now.duration_since(at) >= window)
        {
            self.by_tenant.retain(|_, q| {
                prune(q);
                !q.is_empty()
            });
            self.last_sweep = Some(now);
        }
        let q = self.by_tenant.entry(tenant.to_owned()).or_default();
        prune(q);
        if q.len() >= limit {
            return false;
        }
        q.push_back(now);
        true
    }
}

/// Classifies each request as within (`reserved`) or over (`overflow`) its
/// tenant's quota. The tenant is the metadata value under `attribute`;
/// requests without it are admitted untouched.
pub struct QuotaGate {
    attribute: String,
    mode: QuotaMode,
    gating: GatingMode,
    limit: usize,
    window: Duration,
    state: Arc<QuotaState>,
}

impl QuotaGate {
    pub fn new(
        attribute: String,
        mode: QuotaMode,
        gating: GatingMode,
        limit: usize,
        window: Duration,
        state: Arc<QuotaState>,
    ) -> Self {
        Self {
            attribute,
            mode,
            gating,
            limit,
            window,
            state,
        }
    }

    fn acquire(&self, tenant: &str, releases: &mut Releases, now: Instant) -> Classification {
        match self.mode {
            QuotaMode::Concurrency => {
                let Ok(mut in_flight) = self.state.in_flight.lock() else {
                    return Classification::Overflow;
                };
                let count = in_flight.entry(tenant.to_owned()).or_default();
                if *count >= self.limit {
                    return Classification::Overflow;
                }
                *count += 1;
                let state = Arc::clone(&self.state);
                let tenant = tenant.to_owned();
                releases.push(move || {
                    if let Ok(mut in_flight) = state.in_flight.lock()
                        && let Some(count) = in_flight.get_mut(&tenant)
                    {
                        *count = count.saturating_sub(1);
                        if *count == 0 {
                            in_flight.remove(&tenant);
                        }
                    }
                });
                Classification::Reserved
            }
            QuotaMode::RateLimit => {
                let Ok(mut windows) = self.state.admissions.lock() else {
                    return Classification::Overflow;
                };
                if windows.admit(tenant, self.limit, self.window, now) {
                    Classification::Reserved
                } else {
                    Classification::Overflow
                }
            }
        }
    }

    fn decide(&self, msg: &mut InternalRequest, releases: &mut Releases, now: Instant) -> Verdict {
        let Some(tenant) = msg.request.metadata.get(&self.attribute).cloned() else {
            return Verdict::Continue;
        };
        let class = self.acquire(&tenant, releases, now);
        msg.routing.set_classification(Some(class));
        if self.gating == GatingMode::Blocking && class == Classification::Overflow {
            Verdict::Refuse
        } else {
            Verdict::Continue
        }
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
    ) -> BoxFuture<'a, Verdict> {
        Box::pin(async move { self.decide(msg, releases, Instant::now()) })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use crate::api::routing::Classification;
    use crate::gate::Verdict;
    use crate::gate::admission::GatingMode;
    use crate::gate::admission::quota::{QuotaGate, QuotaMode, QuotaState};
    use crate::gate::release::Releases;
    use crate::gate::test_support::request;

    fn gate(mode: QuotaMode, gating: GatingMode, state: Arc<QuotaState>) -> QuotaGate {
        QuotaGate::new(
            "userid".into(),
            mode,
            gating,
            2,
            Duration::from_secs(10),
            state,
        )
    }

    #[test]
    fn requests_without_the_attribute_pass_untouched() {
        let g = gate(QuotaMode::Concurrency, GatingMode::Blocking, Arc::default());
        let mut msg = request(&[("tenant", "a")]);
        assert_eq!(
            g.decide(&mut msg, &mut Releases::default(), Instant::now()),
            Verdict::Continue
        );
        assert_eq!(msg.routing.classification(), None);
    }

    #[test]
    fn concurrency_blocking_refuses_over_limit_and_frees_on_release() {
        let g = gate(QuotaMode::Concurrency, GatingMode::Blocking, Arc::default());
        let now = Instant::now();
        let mut held = Releases::default();
        for _ in 0..2 {
            let mut msg = request(&[("userid", "a")]);
            assert_eq!(g.decide(&mut msg, &mut held, now), Verdict::Continue);
            assert_eq!(msg.routing.classification(), Some(Classification::Reserved));
        }
        let mut msg = request(&[("userid", "a")]);
        assert_eq!(
            g.decide(&mut msg, &mut Releases::default(), now),
            Verdict::Refuse
        );
        assert_eq!(msg.routing.classification(), Some(Classification::Overflow));
        // Another tenant has its own quota.
        let mut other = request(&[("userid", "b")]);
        assert_eq!(
            g.decide(&mut other, &mut Releases::default(), now),
            Verdict::Continue
        );
        drop(held);
        let mut msg = request(&[("userid", "a")]);
        assert_eq!(
            g.decide(&mut msg, &mut Releases::default(), now),
            Verdict::Continue
        );
    }

    #[test]
    fn classifying_admits_overflow() {
        let g = gate(
            QuotaMode::Concurrency,
            GatingMode::Classifying,
            Arc::default(),
        );
        let now = Instant::now();
        let mut held = Releases::default();
        for want in [
            Classification::Reserved,
            Classification::Reserved,
            Classification::Overflow,
        ] {
            let mut msg = request(&[("userid", "a")]);
            assert_eq!(g.decide(&mut msg, &mut held, now), Verdict::Continue);
            assert_eq!(msg.routing.classification(), Some(want));
        }
    }

    #[test]
    fn rate_limit_slides() {
        let g = gate(QuotaMode::RateLimit, GatingMode::Blocking, Arc::default());
        let t0 = Instant::now();
        let mut r = Releases::default();
        let mut decide = |at: Instant| g.decide(&mut request(&[("userid", "a")]), &mut r, at);
        assert_eq!(decide(t0), Verdict::Continue);
        assert_eq!(decide(t0 + Duration::from_secs(5)), Verdict::Continue);
        assert_eq!(decide(t0 + Duration::from_secs(9)), Verdict::Refuse);
        assert_eq!(decide(t0 + Duration::from_secs(10)), Verdict::Continue);
        assert_eq!(decide(t0 + Duration::from_secs(11)), Verdict::Refuse);
        assert_eq!(decide(t0 + Duration::from_secs(15)), Verdict::Continue);
    }

    #[test]
    fn gates_with_shared_state_share_the_quota() {
        let state = Arc::new(QuotaState::default());
        let a = gate(
            QuotaMode::Concurrency,
            GatingMode::Blocking,
            Arc::clone(&state),
        );
        let b = gate(QuotaMode::Concurrency, GatingMode::Blocking, state);
        let now = Instant::now();
        let mut held = Releases::default();
        a.decide(&mut request(&[("userid", "t")]), &mut held, now);
        b.decide(&mut request(&[("userid", "t")]), &mut held, now);
        assert_eq!(
            a.decide(
                &mut request(&[("userid", "t")]),
                &mut Releases::default(),
                now
            ),
            Verdict::Refuse
        );
    }

    #[test]
    fn idle_rate_windows_are_swept() {
        let state = Arc::new(QuotaState::default());
        let g = gate(
            QuotaMode::RateLimit,
            GatingMode::Blocking,
            Arc::clone(&state),
        );
        let t0 = Instant::now();
        g.decide(
            &mut request(&[("userid", "gone")]),
            &mut Releases::default(),
            t0,
        );
        g.decide(
            &mut request(&[("userid", "x")]),
            &mut Releases::default(),
            t0 + Duration::from_secs(20),
        );
        let windows = state.admissions.lock().unwrap();
        assert!(!windows.by_tenant.contains_key("gone"));
    }
}
