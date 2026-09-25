use std::sync::Arc;

use tokio::sync::Semaphore;

use crate::api::request::InternalRequest;
use crate::boxed::BoxFuture;
use crate::gate::admission::GatingMode;
use crate::gate::release::Releases;
use crate::gate::{Gate, Verdict};

/// Caps requests in flight through this gate in this process.
///
/// Blocking mode waits for a slot; classifying mode refuses when full.
pub struct LocalConcurrencyGate {
    limit: usize,
    mode: GatingMode,
    slots: Arc<Semaphore>,
}

impl LocalConcurrencyGate {
    pub fn new(limit: usize, mode: GatingMode) -> Self {
        Self {
            limit,
            mode,
            slots: Arc::new(Semaphore::new(limit)),
        }
    }
}

impl Gate for LocalConcurrencyGate {
    fn budget(&self) -> BoxFuture<'_, f64> {
        Box::pin(async move {
            if self.limit == 0 {
                return 0.0;
            }
            self.slots.available_permits() as f64 / self.limit as f64
        })
    }

    fn apply<'a>(
        &'a self,
        _msg: &'a mut InternalRequest,
        releases: &'a mut Releases,
    ) -> BoxFuture<'a, Verdict> {
        Box::pin(async move {
            let permit = match self.mode {
                GatingMode::Blocking => Arc::clone(&self.slots).acquire_owned().await.ok(),
                GatingMode::Classifying => Arc::clone(&self.slots).try_acquire_owned().ok(),
            };
            match permit {
                Some(permit) => {
                    releases.push(move || drop(permit));
                    Verdict::Continue
                }
                None => Verdict::Refuse,
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::gate::admission::GatingMode;
    use crate::gate::admission::local_concurrency::LocalConcurrencyGate;
    use crate::gate::release::Releases;
    use crate::gate::test_support::request;
    use crate::gate::{Gate, Verdict};

    #[tokio::test]
    async fn classifying_refuses_when_full() {
        let g = LocalConcurrencyGate::new(2, GatingMode::Classifying);
        let mut a = Releases::default();
        let mut b = Releases::default();
        assert_eq!(g.apply(&mut request(&[]), &mut a).await, Verdict::Continue);
        assert_eq!(g.budget().await, 0.5);
        assert_eq!(g.apply(&mut request(&[]), &mut b).await, Verdict::Continue);
        assert_eq!(g.budget().await, 0.0);
        assert_eq!(
            g.apply(&mut request(&[]), &mut Releases::default()).await,
            Verdict::Refuse
        );
        drop(a);
        assert_eq!(g.budget().await, 0.5);
    }

    #[tokio::test]
    async fn blocking_waits_for_a_slot() {
        let g = LocalConcurrencyGate::new(1, GatingMode::Blocking);
        let mut held = Releases::default();
        g.apply(&mut request(&[]), &mut held).await;
        let mut msg = request(&[]);
        let mut r = Releases::default();
        let waiting =
            tokio::time::timeout(Duration::from_millis(50), g.apply(&mut msg, &mut r)).await;
        assert!(waiting.is_err(), "must block while full");
        drop(held);
        let mut r = Releases::default();
        assert_eq!(g.apply(&mut request(&[]), &mut r).await, Verdict::Continue);
    }
}
