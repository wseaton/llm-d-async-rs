use crate::api::request::InternalRequest;
use crate::boxed::BoxFuture;
use crate::gate::release::Releases;
use crate::gate::{Gate, Verdict, budget_verdict};
use crate::store::Store;

/// Reads its budget from a store key an operator or controller sets through
/// the admin API. A missing or unparsable value is full budget; a store error
/// closes the gate.
pub struct BudgetKeyGate {
    store: Store,
    key: String,
}

impl BudgetKeyGate {
    pub fn new(store: Store, key: String) -> Self {
        Self { store, key }
    }
}

fn parse_budget(raw: &[u8]) -> f64 {
    match std::str::from_utf8(raw)
        .ok()
        .and_then(|s| s.trim().parse::<f64>().ok())
    {
        Some(v) if v.is_finite() => v.clamp(0.0, 1.0),
        _ => 1.0,
    }
}

impl Gate for BudgetKeyGate {
    fn budget(&self) -> BoxFuture<'_, f64> {
        Box::pin(async move {
            match self.store.budget(&self.key).await {
                Ok(None) => 1.0,
                Ok(Some(raw)) => parse_budget(&raw),
                Err(e) => {
                    tracing::error!(key = %self.key, error = %e, "failed to read dispatch budget");
                    0.0
                }
            }
        })
    }

    fn apply<'a>(
        &'a self,
        _msg: &'a mut InternalRequest,
        _releases: &'a mut Releases,
    ) -> BoxFuture<'a, Verdict> {
        Box::pin(async move { budget_verdict(self.budget().await) })
    }
}

#[cfg(test)]
mod tests {
    use crate::gate::Gate;
    use crate::gate::admission::budget_key::{BudgetKeyGate, parse_budget};
    use crate::store::embedded::test_support::open;

    #[test]
    fn parses_and_clamps() {
        assert_eq!(parse_budget(b"0.25"), 0.25);
        assert_eq!(parse_budget(b" 2 "), 1.0);
        assert_eq!(parse_budget(b"-1"), 0.0);
        assert_eq!(parse_budget(b"garbage"), 1.0);
        assert_eq!(parse_budget(b"NaN"), 1.0);
        assert_eq!(parse_budget(&[0xff]), 1.0);
    }

    #[tokio::test]
    async fn reads_the_store() {
        let fixture = open().await;
        let store = fixture.store.clone();
        let g = BudgetKeyGate::new(store.clone(), "k".into());
        assert_eq!(g.budget().await, 1.0);
        store.set_budget("k", Some(b"0".to_vec())).await.unwrap();
        assert_eq!(g.budget().await, 0.0);
    }
}
