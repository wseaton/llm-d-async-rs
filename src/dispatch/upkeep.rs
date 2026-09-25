use std::time::Duration;

use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::clock::now_millis;
use crate::store::Store;

const PROMOTE_BATCH: usize = 1000;
const SWEEP_INTERVAL: Duration = Duration::from_secs(60);
/// Orphan collection lists every blob, so it runs rarely.
const ORPHAN_INTERVAL: Duration = Duration::from_secs(15 * 60);
/// A blob this young may belong to an upload whose reference has not
/// committed yet.
const ORPHAN_GRACE_MS: i64 = 60 * 60 * 1000;

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
        if let Err(e) = store.sweep(now_millis()).await {
            tracing::error!(error = %e, "failed to sweep expired state");
        }
    }
}

/// Deletes blobs nothing references: uploads whose submission failed, and
/// deletes that failed after their reference was dropped.
pub async fn collect_orphans(store: Store, cancel: CancellationToken) {
    let mut ticker = tokio::time::interval(ORPHAN_INTERVAL);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker.tick().await;
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            _ = ticker.tick() => {}
        }
        match store
            .collect_orphans(now_millis().saturating_sub(ORPHAN_GRACE_MS))
            .await
        {
            Ok(0) => {}
            Ok(n) => tracing::info!(blobs = n, "deleted orphaned blobs"),
            Err(e) => tracing::error!(error = %e, "failed to collect orphaned blobs"),
        }
    }
}
