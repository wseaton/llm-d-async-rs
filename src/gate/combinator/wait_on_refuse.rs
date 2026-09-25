use crate::api::request::InternalRequest;
use crate::gate::release::Releases;
use crate::gate::{BoxFuture, Gate, SharedGate, Verdict};

/// Turns the inner gate's `Refuse` into `Wait`, so a pool-level gate parks
/// the worker instead of returning the request to its queue.
pub struct WaitOnRefuseGate {
    inner: SharedGate,
}

impl WaitOnRefuseGate {
    pub fn new(inner: SharedGate) -> Self {
        Self { inner }
    }
}

impl Gate for WaitOnRefuseGate {
    fn budget(&self) -> BoxFuture<'_, f64> {
        self.inner.budget()
    }

    fn apply<'a>(
        &'a self,
        msg: &'a mut InternalRequest,
        releases: &'a mut Releases,
    ) -> BoxFuture<'a, Verdict> {
        Box::pin(async move {
            match self.inner.apply(msg, releases).await {
                Verdict::Refuse => Verdict::Wait,
                other => other,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::gate::combinator::wait_on_refuse::WaitOnRefuseGate;
    use crate::gate::release::Releases;
    use crate::gate::test_support::{FixedGate, request};
    use crate::gate::{Gate, Verdict};

    #[tokio::test]
    async fn refuse_becomes_wait() {
        for (inner, want) in [
            (Verdict::Refuse, Verdict::Wait),
            (Verdict::Continue, Verdict::Continue),
            (Verdict::Drop(None), Verdict::Drop(None)),
        ] {
            let g = WaitOnRefuseGate::new(Arc::new(FixedGate::new(0.4, inner)));
            assert_eq!(
                g.apply(&mut request(&[]), &mut Releases::default()).await,
                want
            );
            assert_eq!(g.budget().await, 0.4);
        }
    }
}
