use std::time::Duration;

use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::clock::now_millis;
use crate::store::Store;

const PROMOTE_BATCH: usize = 1000;
const SWEEP_INTERVAL: Duration = Duration::from_secs(60);

/// Returns retries to their queues once their backoff has passed.
pub async fn promote_retries(store: Store, interval: Duration, cancel: CancellationToken) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            _ = ticker.tick() => {}
        }
        loop {
            match store.promote_due_retries(now_millis(), PROMOTE_BATCH).await {
                Ok(moved) if moved == PROMOTE_BATCH => continue,
                Ok(_) => break,
                Err(e) => {
                    tracing::error!(error = %e, "failed to promote due retries");
                    break;
                }
            }
        }
    }
}

/// Deletes expired markers, results, and blobs.
pub async fn sweep(store: Store, cancel: CancellationToken) {
    let mut ticker = tokio::time::interval(SWEEP_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            _ = ticker.tick() => {}
        }
        let now = now_millis();
        if let Err(e) = store.sweep_request_markers(now).await {
            tracing::error!(error = %e, "failed to sweep request markers");
        }
        if let Err(e) = store.sweep_results(now).await {
            tracing::error!(error = %e, "failed to sweep results");
        }
    }
}
