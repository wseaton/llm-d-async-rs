//! Merge policies: fan the queues of each worker pool into one channel the
//! pool's workers read. Pools are isolated; a saturated pool blocks only its
//! own queues.

pub mod headers;
pub mod random_robin;
pub mod stamp;
pub mod tier_priority;

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::config::merge_policy::MergePolicyConfig;
use crate::dispatch::message::{Claimed, Dispatch, SourceMeta};
use crate::telemetry::metrics::Metrics;

/// The merged channel of one pool, shared by its workers.
pub type DispatchReceiver = Arc<Mutex<mpsc::Receiver<Dispatch>>>;

/// One queue feeding a pool. Dropping the consumer's sender removes it.
pub struct Source {
    pub meta: Arc<SourceMeta>,
    pub rx: mpsc::Receiver<Claimed>,
}

#[derive(Debug, thiserror::Error)]
#[error("worker pool {0:?} not found")]
pub struct UnknownPool(pub String);

/// Adds sources to the running merges.
#[derive(Clone)]
pub struct MergeHandle {
    pools: Arc<HashMap<String, mpsc::UnboundedSender<Source>>>,
}

impl MergeHandle {
    pub fn add(&self, pool: &str, source: Source) -> Result<(), UnknownPool> {
        self.pools
            .get(pool)
            .ok_or_else(|| UnknownPool(pool.to_owned()))?
            .send(source)
            .map_err(|_| UnknownPool(pool.to_owned()))
    }
}

pub struct PoolSpec {
    pub id: String,
    /// Merged channel capacity.
    pub buffer: usize,
}

/// Starts one merge task per pool. They run until `cancel`.
pub fn start(
    config: &MergePolicyConfig,
    pools: &[PoolSpec],
    metrics: &Arc<Metrics>,
    cancel: &CancellationToken,
    tasks: &TaskTracker,
) -> (MergeHandle, HashMap<String, DispatchReceiver>) {
    let mut controls = HashMap::new();
    let mut receivers = HashMap::new();
    for pool in pools {
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let (merged_tx, merged_rx) = mpsc::channel(pool.buffer.max(1));
        controls.insert(pool.id.clone(), control_tx);
        receivers.insert(pool.id.clone(), Arc::new(Mutex::new(merged_rx)));
        let metrics = Arc::clone(metrics);
        let cancel = cancel.clone();
        match config {
            MergePolicyConfig::RandomRobin { fairness } => {
                tasks.spawn(random_robin::run(
                    control_rx,
                    merged_tx,
                    fairness.clone(),
                    metrics,
                    cancel,
                ));
            }
            MergePolicyConfig::TierPriority {
                priority_header,
                tier_label,
                objective_header,
                lane_objectives,
                fairness,
            } => {
                let stamping = tier_priority::Stamping {
                    priority_header: priority_header.clone(),
                    tier_label: tier_label.clone(),
                    objective_header: objective_header.clone(),
                    lane_objectives: lane_objectives.clone(),
                    fairness: fairness.clone(),
                };
                tasks.spawn(tier_priority::run(
                    control_rx,
                    merged_tx,
                    stamping,
                    metrics,
                    cancel,
                    tasks.clone(),
                ));
            }
        }
    }
    (
        MergeHandle {
            pools: Arc::new(controls),
        },
        receivers,
    )
}
