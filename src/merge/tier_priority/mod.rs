//! Strict-priority lanes by (classification, tier), round-robin across
//! queues within a lane:
//!
//! ```text
//!  0 reserved-interactive > 1 reserved-async > 2 reserved-batch >
//!  3 overflow-interactive > 4 overflow-async > 5 overflow-batch
//! ```
//!
//! A missing tier counts as batch; a missing classification as overflow.

pub mod lanes;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use reqwest::header::HeaderName;
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::api::request::InternalRequest;
use crate::api::routing::{Classification, Tier};
use crate::config::merge_policy::Fairness;
use crate::dispatch::message::{Claimed, Dispatch, SourceMeta};
use crate::merge::Source;
use crate::merge::stamp::base_headers;
use crate::merge::tier_priority::lanes::Lanes;
use crate::telemetry::metrics::Metrics;

/// Requests buffered per lane before that lane's queues block.
const LANE_CAPACITY: usize = 1000;

pub struct Stamping {
    pub priority_header: Option<HeaderName>,
    pub tier_label: String,
    pub objective_header: HeaderName,
    pub lane_objectives: BTreeMap<String, String>,
    pub fairness: Fairness,
}

pub fn lane_of(envelope: &InternalRequest, tier_label: &str) -> (usize, String) {
    let tier = envelope.routing.tier(tier_label);
    let class = envelope
        .routing
        .classification()
        .unwrap_or(Classification::Overflow);
    let tier_index = match tier {
        Tier::Interactive => 0,
        Tier::Async => 1,
        Tier::Batch => 2,
    };
    let class_index = match class {
        Classification::Reserved => 0,
        Classification::Overflow => 1,
    };
    (
        class_index * 3 + tier_index,
        format!("{}-{}", class.as_str(), tier.as_str()),
    )
}

type Item = (Arc<SourceMeta>, Claimed);

struct Shared {
    lanes: Mutex<Lanes<Item>>,
    /// Signalled when a lane frees a slot.
    space: Notify,
    /// Signalled when an item arrives.
    items: Notify,
}

impl Shared {
    fn lanes(&self) -> std::sync::MutexGuard<'_, Lanes<Item>> {
        self.lanes.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Queues `item`, waiting while its lane is full. False on cancel.
    async fn push(
        &self,
        lane: usize,
        source: u64,
        mut item: Item,
        cancel: &CancellationToken,
    ) -> bool {
        loop {
            let space = self.space.notified();
            tokio::pin!(space);
            space.as_mut().enable();
            match self.lanes().try_push(lane, source, item) {
                Ok(()) => {
                    self.items.notify_one();
                    return true;
                }
                Err(back) => item = back,
            }
            tokio::select! {
                () = cancel.cancelled() => return false,
                () = &mut space => {}
            }
        }
    }

    /// The next item by priority, waiting while empty. `None` on cancel.
    async fn pop(&self, cancel: &CancellationToken) -> Option<Item> {
        loop {
            let items = self.items.notified();
            tokio::pin!(items);
            items.as_mut().enable();
            let next = self.lanes().pop();
            if let Some(item) = next {
                self.space.notify_waiters();
                return Some(item);
            }
            tokio::select! {
                () = cancel.cancelled() => return None,
                () = &mut items => {}
            }
        }
    }
}

async fn read_source(
    shared: Arc<Shared>,
    source_id: u64,
    mut source: Source,
    tier_label: String,
    cancel: CancellationToken,
) {
    loop {
        let claimed = tokio::select! {
            () = cancel.cancelled() => return,
            claimed = source.rx.recv() => match claimed {
                Some(c) => c,
                None => return,
            },
        };
        let (lane, _) = lane_of(&claimed.envelope, &tier_label);
        if !shared
            .push(
                lane,
                source_id,
                (Arc::clone(&source.meta), claimed),
                &cancel,
            )
            .await
        {
            return;
        }
    }
}

pub async fn run(
    mut control: mpsc::UnboundedReceiver<Source>,
    merged: mpsc::Sender<Dispatch>,
    stamping: Stamping,
    metrics: Arc<Metrics>,
    cancel: CancellationToken,
    tasks: TaskTracker,
) {
    let shared = Arc::new(Shared {
        lanes: Mutex::new(Lanes::new(LANE_CAPACITY)),
        space: Notify::new(),
        items: Notify::new(),
    });
    let mut next_source = 0u64;
    loop {
        let (meta, claimed) = tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            Some(source) = control.recv() => {
                tasks.spawn(read_source(
                    Arc::clone(&shared),
                    next_source,
                    source,
                    stamping.tier_label.clone(),
                    cancel.clone(),
                ));
                next_source += 1;
                continue;
            }
            item = shared.pop(&cancel) => match item {
                Some(item) => item,
                None => return,
            },
        };
        let mut headers = base_headers(&meta, &claimed.envelope, &stamping.fairness);
        let (lane, lane_key) = lane_of(&claimed.envelope, &stamping.tier_label);
        if let Some(h) = &stamping.priority_header {
            headers.set(h.as_str(), &lane.to_string());
        }
        if let Some(objective) = stamping
            .lane_objectives
            .get(&lane_key)
            .filter(|o| !o.is_empty())
        {
            headers.set(stamping.objective_header.as_str(), objective);
        }
        let labels = meta.labels.clone();
        let dispatch = Dispatch {
            url: meta.url_for(&claimed.envelope),
            headers,
            source: meta,
            claimed,
        };
        metrics.queue_depth_inc(&labels);
        let sent = tokio::select! {
            biased;
            () = cancel.cancelled() => false,
            sent = merged.send(dispatch) => sent.is_ok(),
        };
        if !sent {
            metrics.queue_depth_dec(&labels);
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::api::routing::Classification;
    use crate::merge::tier_priority::lane_of;
    use crate::store::test_support::envelope;

    #[test]
    fn lanes_by_classification_and_tier() {
        let cases = [
            (
                Some(Classification::Reserved),
                Some("interactive"),
                0,
                "reserved-interactive",
            ),
            (
                Some(Classification::Reserved),
                Some("async"),
                1,
                "reserved-async",
            ),
            (Some(Classification::Reserved), None, 2, "reserved-batch"),
            (None, Some("interactive"), 3, "overflow-interactive"),
            (
                Some(Classification::Overflow),
                Some("async"),
                4,
                "overflow-async",
            ),
            (None, Some("gold"), 5, "overflow-batch"),
        ];
        for (class, tier, index, key) in cases {
            let mut env = envelope("a", "t", "q", 1);
            env.routing.set_classification(class);
            if let Some(t) = tier {
                env.routing.labels.insert("sla".into(), t.into());
            }
            assert_eq!(lane_of(&env, "sla"), (index, key.to_owned()));
        }
    }
}
