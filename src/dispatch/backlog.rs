use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::clock::now_millis;
use crate::dispatch::queues::{Queues, labels_of};
use crate::store::Store;
use crate::telemetry::deadline_proximity::BOUNDS_SECS;
use crate::telemetry::metrics::{Metrics, QueueLabels};

/// Publishes each queue's depth and deadline-proximity histogram, and drops
/// the snapshots of queues that went away.
pub async fn run(
    store: Store,
    metrics: Arc<Metrics>,
    queues: Arc<Queues>,
    interval: Duration,
    cancel: CancellationToken,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut previous: BTreeSet<QueueLabels> = BTreeSet::new();
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            _ = ticker.tick() => {}
        }
        let mut current = BTreeSet::new();
        let mut failed = false;
        for config in queues.snapshot().await {
            let labels = labels_of(&config);
            match store
                .backlog(
                    config.queue_name.clone(),
                    now_millis(),
                    BOUNDS_SECS.to_vec(),
                )
                .await
            {
                Ok(backlog) => {
                    metrics.broker_backlog(&labels, backlog.depth, true);
                    metrics.deadline_proximity(&labels, &backlog.cumulative);
                }
                Err(e) => {
                    tracing::error!(queue = %config.queue_name, error = %e, "failed to read queue backlog");
                    failed = true;
                    metrics.broker_backlog(&labels, 0, false);
                    metrics.deadline_proximity(&labels, &[0; BOUNDS_SECS.len()]);
                }
            }
            current.insert(labels);
        }
        if !failed {
            for gone in previous.difference(&current) {
                metrics.remove_queue_snapshots(gone);
            }
            previous = current;
        }
    }
}
