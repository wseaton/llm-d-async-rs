//! Dispatch gates: capacity and admission control.
//!
//! A gate runs at one of two levels. A queue-level gate decides, before a
//! request is claimed, whether it may enter its pool's merged channel. A
//! pool-level gate runs inside the worker right before dispatch and may park
//! the worker (`Wait`).
//!
//! Budget gates report the fraction of capacity available in [0, 1];
//! admission gates return a per-request [`Verdict`].

pub mod admission;
pub mod combinator;
pub mod constant;
pub mod factory;
pub mod metric;
pub mod release;

use std::sync::Arc;

use crate::api::request::InternalRequest;
use crate::api::result::ResultMessage;
use crate::boxed::BoxFuture;
use crate::gate::release::Releases;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// Dispatch the request.
    Continue,
    /// Finish the request now, with the given result or a GATE_DROPPED one.
    Drop(Option<ResultMessage>),
    /// Return the request to its queue.
    Refuse,
    /// Park the worker and ask again (pool-level gates only).
    Wait,
}

pub trait Gate: Send + Sync {
    /// Fraction of capacity available for new requests, in [0, 1].
    fn budget(&self) -> BoxFuture<'_, f64>;

    /// Decides one request. Reservations the gate takes are pushed onto
    /// `releases` and given back when the request finishes or is requeued.
    fn apply<'a>(
        &'a self,
        msg: &'a mut InternalRequest,
        releases: &'a mut Releases,
    ) -> BoxFuture<'a, Verdict>;
}

pub type SharedGate = Arc<dyn Gate>;

/// The verdict of a pure budget gate: refuse when no capacity is left.
pub fn budget_verdict(budget: f64) -> Verdict {
    if budget <= 0.0 {
        Verdict::Refuse
    } else {
        Verdict::Continue
    }
}

/// Runs gates in order and stops at the first non-continue verdict, giving
/// back every reservation the chain took.
pub async fn apply_chain(
    gates: &[SharedGate],
    msg: &mut InternalRequest,
    releases: &mut Releases,
) -> Verdict {
    let snapshot = releases.len();
    for gate in gates {
        let verdict = gate.apply(msg, releases).await;
        if verdict != Verdict::Continue {
            releases.release_from(snapshot);
            return verdict;
        }
    }
    Verdict::Continue
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::api::request::{InternalRequest, RequestMessage};
    use crate::api::routing::InternalRouting;
    use crate::boxed::BoxFuture;
    use crate::gate::release::Releases;
    use crate::gate::{Gate, Verdict};

    pub fn request(metadata: &[(&str, &str)]) -> InternalRequest {
        InternalRequest {
            routing: InternalRouting::default(),
            request: RequestMessage {
                id: "r".into(),
                created: 0,
                deadline: i64::MAX,
                metadata: metadata
                    .iter()
                    .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                    .collect::<BTreeMap<_, _>>(),
                headers: BTreeMap::new(),
                endpoint: String::new(),
                model: String::new(),
            },
            payload: crate::store::staging::StagedPayload::Inline(Vec::new())
                .info(crate::api::payload::JSON_CONTENT_TYPE),
        }
    }

    /// A gate with a fixed answer that counts outstanding reservations.
    pub struct FixedGate {
        pub budget: f64,
        pub verdict: Verdict,
        pub held: Arc<AtomicUsize>,
    }

    impl FixedGate {
        pub fn new(budget: f64, verdict: Verdict) -> Self {
            Self {
                budget,
                verdict,
                held: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl Gate for FixedGate {
        fn budget(&self) -> BoxFuture<'_, f64> {
            Box::pin(async move { self.budget })
        }

        fn apply<'a>(
            &'a self,
            _msg: &'a mut InternalRequest,
            releases: &'a mut Releases,
        ) -> BoxFuture<'a, Verdict> {
            Box::pin(async move {
                self.held.fetch_add(1, Ordering::SeqCst);
                let held = Arc::clone(&self.held);
                releases.push(move || {
                    held.fetch_sub(1, Ordering::SeqCst);
                });
                self.verdict.clone()
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use crate::gate::release::Releases;
    use crate::gate::test_support::{FixedGate, request};
    use crate::gate::{SharedGate, Verdict, apply_chain, budget_verdict};

    #[tokio::test]
    async fn chain_rolls_back_only_its_own_reservations() {
        let first = Arc::new(FixedGate::new(1.0, Verdict::Continue));
        let second = Arc::new(FixedGate::new(1.0, Verdict::Refuse));
        let outside = Arc::new(FixedGate::new(1.0, Verdict::Continue));
        let mut releases = Releases::default();
        let mut msg = request(&[]);
        use crate::gate::Gate;
        outside.apply(&mut msg, &mut releases).await;
        let chain: Vec<SharedGate> = vec![first.clone(), second.clone()];
        assert_eq!(
            apply_chain(&chain, &mut msg, &mut releases).await,
            Verdict::Refuse
        );
        assert_eq!(first.held.load(Ordering::SeqCst), 0);
        assert_eq!(second.held.load(Ordering::SeqCst), 0);
        assert_eq!(outside.held.load(Ordering::SeqCst), 1);
        drop(releases);
        assert_eq!(outside.held.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn chain_keeps_reservations_on_continue() {
        let a = Arc::new(FixedGate::new(1.0, Verdict::Continue));
        let chain: Vec<SharedGate> = vec![a.clone(), a.clone()];
        let mut releases = Releases::default();
        assert_eq!(
            apply_chain(&chain, &mut request(&[]), &mut releases).await,
            Verdict::Continue
        );
        assert_eq!(a.held.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn budget_verdicts() {
        assert_eq!(budget_verdict(0.0), Verdict::Refuse);
        assert_eq!(budget_verdict(-1.0), Verdict::Refuse);
        assert_eq!(budget_verdict(0.01), Verdict::Continue);
    }
}
