use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;
use tokio::sync::mpsc::UnboundedReceiver;

use crate::clock::now_millis;
use crate::store::Store;
use crate::store::queue::Outcome;

const MAX_BATCH: usize = 1024;
const FIRST_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// Writes claim outcomes to the store in batches until every sender is gone.
/// A batch that fails is retried until it is written: dropping it would
/// strand its claims until the next restart.
pub async fn run(store: Store, mut outcomes: UnboundedReceiver<Outcome>, results: Arc<Notify>) {
    while let Some(first) = outcomes.recv().await {
        let mut batch = vec![first];
        while batch.len() < MAX_BATCH {
            match outcomes.try_recv() {
                Ok(outcome) => batch.push(outcome),
                Err(_) => break,
            }
        }
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
    }
}
