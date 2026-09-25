use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

use crate::clock::now_millis;
use crate::dispatch::claim::Pending;
use crate::store::Store;
use crate::store::queue::Outcome;

const MAX_BATCH: usize = 1024;
/// Result bytes one batch may carry.
const MAX_BATCH_BYTES: usize = 16 << 20;
const FIRST_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(5);
const KIB: usize = 1024;
/// Result bytes that may wait for the writer across every worker.
pub const OUTCOME_BUDGET_BYTES: usize = 256 << 20;

/// Result bytes that may wait for the writer at once. A worker with a result
/// to hand over waits for room, so responses larger or faster than the store
/// can write hold workers back instead of piling up in memory.
#[derive(Clone)]
pub struct OutcomeBudget {
    kib: Arc<Semaphore>,
    total_kib: u32,
}

impl OutcomeBudget {
    pub fn new(bytes: usize) -> Self {
        let total_kib = u32::try_from(bytes / KIB).unwrap_or(u32::MAX).max(1);
        Self {
            kib: Arc::new(Semaphore::new(total_kib as usize)),
            total_kib,
        }
    }

    /// Waits for room for an outcome of `bytes`; one larger than the whole
    /// budget waits for all of it.
    pub async fn reserve(&self, bytes: usize) -> Option<OwnedSemaphorePermit> {
        let kib = u32::try_from(bytes.div_ceil(KIB))
            .unwrap_or(u32::MAX)
            .clamp(1, self.total_kib);
        Arc::clone(&self.kib).acquire_many_owned(kib).await.ok()
    }
}

/// Result bytes an outcome carries to the store.
pub fn weight(outcome: &Outcome) -> usize {
    match outcome {
        Outcome::Finish { result, .. } => result.payload.len(),
        Outcome::Retry { .. } | Outcome::Release { .. } => 0,
    }
}

/// Writes claim outcomes to the store in batches until every sender is gone.
/// A batch that fails is retried until it is written: dropping it would
/// strand its claims until the next restart.
pub async fn run(store: Store, mut outcomes: UnboundedReceiver<Pending>, results: Arc<Notify>) {
    while let Some(first) = outcomes.recv().await {
        let mut bytes = weight(&first.outcome);
        let mut batch = vec![first];
        while batch.len() < MAX_BATCH && bytes < MAX_BATCH_BYTES {
            match outcomes.try_recv() {
                Ok(pending) => {
                    bytes += weight(&pending.outcome);
                    batch.push(pending);
                }
                Err(_) => break,
            }
        }
        let (batch, reserved): (Vec<Outcome>, Vec<Option<OwnedSemaphorePermit>>) =
            batch.into_iter().map(|p| (p.outcome, p.reserved)).unzip();
        let batch: Arc<[Outcome]> = batch.into();
        let mut backoff = FIRST_BACKOFF;
        loop {
            match store.apply_outcomes(Arc::clone(&batch), now_millis()).await {
                Ok(applied) => {
                    if applied.results_written > 0 {
                        results.notify_waiters();
                    }
                    if applied.fenced > 0 {
                        tracing::debug!(
                            fenced = applied.fenced,
                            "dropped outcomes of stale claims"
                        );
                    }
                    break;
                }
                Err(e) => {
                    tracing::error!(error = %e, size = batch.len(), retry_in = ?backoff, "failed to write outcomes");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
            }
        }
        drop(reserved);
    }
}
