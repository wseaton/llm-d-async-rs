use crate::api::request::InternalRequest;
use crate::boxed::BoxFuture;
use crate::gate::release::Releases;
use crate::gate::{Gate, SharedGate, Verdict, apply_chain};

/// All inner gates must admit; the budget is the smallest inner budget.
pub struct CompositeGate {
    gates: Vec<SharedGate>,
}

impl CompositeGate {
    pub fn new(gates: Vec<SharedGate>) -> Self {
        Self { gates }
    }
}

impl Gate for CompositeGate {
    fn budget(&self) -> BoxFuture<'_, f64> {
        Box::pin(async move {
            let mut min: f64 = 1.0;
            for gate in &self.gates {
                min = min.min(gate.budget().await);
            }
            min
        })
    }

    fn apply<'a>(
        &'a self,
        msg: &'a mut InternalRequest,
        releases: &'a mut Releases,
    ) -> BoxFuture<'a, Verdict> {
        Box::pin(apply_chain(&self.gates, msg, releases))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::gate::combinator::composite::CompositeGate;
    use crate::gate::release::Releases;
    use crate::gate::test_support::{FixedGate, request};
    use crate::gate::{Gate, Verdict};

    #[tokio::test]
    async fn budget_is_the_minimum() {
        let g = CompositeGate::new(vec![
            Arc::new(FixedGate::new(0.7, Verdict::Continue)),
            Arc::new(FixedGate::new(0.3, Verdict::Continue)),
        ]);
        assert_eq!(g.budget().await, 0.3);
        assert_eq!(CompositeGate::new(vec![]).budget().await, 1.0);
    }

    #[tokio::test]
    async fn first_non_continue_wins() {
        let g = CompositeGate::new(vec![
            Arc::new(FixedGate::new(1.0, Verdict::Continue)),
            Arc::new(FixedGate::new(1.0, Verdict::Wait)),
            Arc::new(FixedGate::new(1.0, Verdict::Refuse)),
        ]);
        let mut r = Releases::default();
        assert_eq!(g.apply(&mut request(&[]), &mut r).await, Verdict::Wait);
        assert!(r.is_empty());
    }
}
