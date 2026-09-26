use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::transport::QueueConfig;
use crate::dispatch::claim::OutcomeSender;
use crate::dispatch::consumer::Consumer;
use crate::dispatch::message::SourceMeta;
use crate::gate::factory::{GateConfigError, GateFactory};
use crate::merge::{MergeHandle, Source, UnknownPool};
use crate::store::Store;
use crate::store::signal::ResultSignal;
use crate::telemetry::metrics::{Metrics, QueueLabels};

#[derive(Debug, thiserror::Error)]
pub enum QueueError {
    #[error("queue {queue:?}: {source}")]
    Gate {
        queue: String,
        source: GateConfigError,
    },
    #[error(transparent)]
    Pool(#[from] UnknownPool),
    #[error("queues are stopped")]
    Stopped,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct QueueChange {
    pub added: usize,
    pub removed: usize,
}

struct Running {
    config: QueueConfig,
    cancel: CancellationToken,
    task: JoinHandle<()>,
}

struct State {
    running: BTreeMap<String, Running>,
    /// Queue IDs in config order; the first is the default submit queue.
    order: Vec<String>,
    outcomes: Option<OutcomeSender>,
}

/// The live set of queues and their consumers.
pub struct Queues {
    store: Store,
    metrics: Arc<Metrics>,
    factory: Arc<GateFactory>,
    merge: MergeHandle,
    results: Arc<ResultSignal>,
    poll_interval: Duration,
    batch_size: usize,
    consume: CancellationToken,
    state: Mutex<State>,
}

pub fn labels_of(config: &QueueConfig) -> QueueLabels {
    QueueLabels::new(&config.id, &config.queue_name, &config.worker_pool_id)
}

impl Queues {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: Store,
        metrics: Arc<Metrics>,
        factory: Arc<GateFactory>,
        merge: MergeHandle,
        outcomes: OutcomeSender,
        results: Arc<ResultSignal>,
        poll_interval: Duration,
        batch_size: usize,
        consume: CancellationToken,
    ) -> Self {
        Self {
            store,
            metrics,
            factory,
            merge,
            results,
            poll_interval,
            batch_size,
            consume,
            state: Mutex::new(State {
                running: BTreeMap::new(),
                order: Vec::new(),
                outcomes: Some(outcomes),
            }),
        }
    }

    /// Replaces the queue set. Unchanged queues keep their consumer and gate.
    /// Every new gate is built before anything changes, so a bad config
    /// leaves the running set as it was. A removed queue's pending requests
    /// stay in the store.
    pub async fn apply(&self, configs: Vec<QueueConfig>) -> Result<QueueChange, QueueError> {
        let mut state = self.state.lock().await;
        let outcomes = state.outcomes.clone().ok_or(QueueError::Stopped)?;

        let mut prepared = Vec::new();
        for config in &configs {
            if state
                .running
                .get(&config.id)
                .is_some_and(|r| r.config == *config)
            {
                continue;
            }
            let labels = labels_of(config);
            let gate = self
                .factory
                .create(&config.gate_type, &config.gate_params, &labels)
                .map_err(|source| QueueError::Gate {
                    queue: config.queue_name.clone(),
                    source,
                })?;
            prepared.push((config.clone(), labels, gate));
        }

        let wanted: BTreeMap<&str, &QueueConfig> =
            configs.iter().map(|c| (c.id.as_str(), c)).collect();
        let stale: Vec<String> = state
            .running
            .iter()
            .filter(|(id, r)| wanted.get(id.as_str()).is_none_or(|c| **c != r.config))
            .map(|(id, _)| id.clone())
            .collect();
        let mut change = QueueChange::default();
        for id in stale {
            let Some(old) = state.running.remove(&id) else {
                continue;
            };
            old.cancel.cancel();
            if let Err(e) = old.task.await {
                tracing::error!(queue = %old.config.queue_name, error = %e, "queue consumer failed");
            }
            let keeps_labels = wanted.get(id.as_str()).is_some_and(|c| {
                c.queue_name == old.config.queue_name
                    && c.worker_pool_id == old.config.worker_pool_id
            });
            if !keeps_labels {
                self.metrics.remove_queue_snapshots(&labels_of(&old.config));
            }
            change.removed += 1;
        }

        for (config, labels, gate) in prepared {
            let (tx, rx) = mpsc::channel(1);
            let meta = Arc::new(SourceMeta {
                labels: labels.clone(),
                igw_base_url: config.igw_base_url.clone(),
                request_path: config.request_path_url.clone(),
                inference_objective: config.inference_objective.clone(),
                render_url: config.render_url.clone().filter(|_| config.resumable),
            });
            self.merge
                .add(&config.worker_pool_id, Source { meta, rx })?;
            let cancel = self.consume.child_token();
            let consumer = Consumer {
                store: self.store.clone(),
                metrics: Arc::clone(&self.metrics),
                config: config.clone(),
                labels,
                gate,
                outcomes: outcomes.clone(),
                results: Arc::clone(&self.results),
                tx,
                poll_interval: self.poll_interval,
                batch_size: self.batch_size,
            };
            let task = tokio::spawn(consumer.run(cancel.clone()));
            state.running.insert(
                config.id.clone(),
                Running {
                    config,
                    cancel,
                    task,
                },
            );
            change.added += 1;
        }
        state.order = configs.iter().map(|c| c.id.clone()).collect();
        Ok(change)
    }

    /// Stops every consumer. Afterwards `apply` fails.
    pub async fn stop(&self) {
        let mut state = self.state.lock().await;
        state.outcomes = None;
        let running = std::mem::take(&mut state.running);
        for r in running.values() {
            r.cancel.cancel();
        }
        for (_, r) in running {
            if let Err(e) = r.task.await {
                tracing::error!(queue = %r.config.queue_name, error = %e, "queue consumer failed");
            }
        }
    }

    /// The running queue configs, in config order.
    pub async fn snapshot(&self) -> Vec<QueueConfig> {
        let state = self.state.lock().await;
        state
            .order
            .iter()
            .filter_map(|id| state.running.get(id).map(|r| r.config.clone()))
            .collect()
    }
}
