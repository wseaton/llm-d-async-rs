use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::mpsc::UnboundedSender;

use crate::store::queue::{ClaimRef, Outcome};

/// An outcome on its way to the writer, holding the share of the outcome
/// byte budget its result takes until it is written.
pub struct Pending {
    pub outcome: Outcome,
    pub reserved: Option<OwnedSemaphorePermit>,
}

pub type OutcomeSender = UnboundedSender<Pending>;

/// Ownership of one claimed request. Finish it with [`ClaimGuard::finish`];
/// dropping it unfinished releases the request back to its queue.
pub struct ClaimGuard {
    claim: Option<ClaimRef>,
    outcomes: OutcomeSender,
}

impl ClaimGuard {
    pub fn new(claim: ClaimRef, outcomes: OutcomeSender) -> Self {
        Self {
            claim: Some(claim),
            outcomes,
        }
    }

    pub fn claim_id(&self) -> u64 {
        self.claim.as_ref().map_or(0, |c| c.claim_id)
    }

    pub fn finish(
        mut self,
        reserved: Option<OwnedSemaphorePermit>,
        outcome: impl FnOnce(ClaimRef) -> Outcome,
    ) {
        if let Some(claim) = self.claim.take() {
            let _ = self.outcomes.send(Pending {
                outcome: outcome(claim),
                reserved,
            });
        }
    }
}

impl Drop for ClaimGuard {
    fn drop(&mut self) {
        if let Some(claim) = self.claim.take() {
            let _ = self.outcomes.send(Pending {
                outcome: Outcome::Release { claim },
                reserved: None,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;

    use crate::dispatch::claim::ClaimGuard;
    use crate::store::queue::{ClaimRef, Outcome};

    fn claim() -> ClaimRef {
        ClaimRef {
            generation: "g".into(),
            claim_id: 7,
        }
    }

    #[test]
    fn dropping_releases() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        drop(ClaimGuard::new(claim(), tx));
        assert!(
            matches!(rx.try_recv().map(|p| p.outcome), Ok(Outcome::Release { claim: c }) if c.claim_id == 7)
        );
    }

    #[test]
    fn finishing_sends_exactly_one_outcome() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        ClaimGuard::new(claim(), tx).finish(None, |claim| Outcome::Retry {
            claim,
            envelope: crate::store::test_support::envelope("a", "b", "q", 1),
            due_ms: 5,
        });
        assert!(matches!(
            rx.try_recv().map(|p| p.outcome),
            Ok(Outcome::Retry { due_ms: 5, .. })
        ));
        assert!(rx.try_recv().is_err());
    }
}
