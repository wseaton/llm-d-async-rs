use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::config::pools::WorkerPools;
use crate::config::transport::TransportConfig;
use crate::dispatch::queues::Queues;
use crate::telemetry::metrics::Metrics;

/// Re-reads the transport config file every `interval` and applies changes
/// to its queues. Anything that fails to read, parse or apply leaves the
/// last good queue set running. Only `queues` may change; other fields
/// need a restart.
pub async fn watch(
    path: PathBuf,
    interval: Duration,
    mut last_good: TransportConfig,
    pools: WorkerPools,
    queues: Arc<Queues>,
    metrics: Arc<Metrics>,
    cancel: CancellationToken,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            _ = ticker.tick() => {}
        }
        let data = match tokio::fs::read(&path).await {
            Ok(data) => data,
            Err(e) => {
                metrics.queue_config_reload(false);
                tracing::error!(path = %path.display(), error = %e, "failed to read queues config; keeping last good queues");
                continue;
            }
        };
        let config = match TransportConfig::parse(&data, &pools, true) {
            Ok(config) => config,
            Err(e) => {
                metrics.queue_config_reload(false);
                tracing::error!(error = %e, "failed to parse queues config; keeping last good queues");
                continue;
            }
        };
        if config == last_good {
            continue;
        }
        let fixed = |c: &TransportConfig| {
            (
                c.result_queue_name.clone(),
                c.poll_interval_ms,
                c.batch_size,
            )
        };
        if fixed(&config) != fixed(&last_good) {
            metrics.queue_config_reload(false);
            tracing::error!("non-queue transport fields changed; restart required");
            continue;
        }
        match queues.apply(config.queues.clone()).await {
            Ok(change) => {
                metrics.queue_config_reload(true);
                tracing::info!(
                    added = change.added,
                    removed = change.removed,
                    "applied queues config reload"
                );
                last_good = config;
            }
            Err(e) => {
                metrics.queue_config_reload(false);
                tracing::error!(error = %e, "failed to apply queues config; keeping last good queues");
            }
        }
    }
}
