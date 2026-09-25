use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt, StreamMap};
use tokio_util::sync::CancellationToken;

use crate::config::merge_policy::Fairness;
use crate::dispatch::message::{Claimed, Dispatch, SourceMeta};
use crate::merge::Source;
use crate::merge::stamp::base_headers;
use crate::telemetry::metrics::Metrics;

type Tagged = Pin<Box<dyn Stream<Item = (Arc<SourceMeta>, Claimed)> + Send>>;

/// Takes the next request from a randomly chosen ready queue. `StreamMap`
/// starts every poll at a random stream, and drops a stream once its queue's
/// sender is gone.
pub async fn run(
    mut control: mpsc::UnboundedReceiver<Source>,
    merged: mpsc::Sender<Dispatch>,
    fairness: Fairness,
    metrics: Arc<Metrics>,
    cancel: CancellationToken,
) {
    let mut sources: StreamMap<u64, Tagged> = StreamMap::new();
    let mut next_id = 0u64;
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            Some(source) = control.recv() => {
                let meta = source.meta;
                let stream = ReceiverStream::new(source.rx).map(move |c| (Arc::clone(&meta), c));
                sources.insert(next_id, Box::pin(stream));
                next_id += 1;
            }
            Some((_, (meta, claimed))) = sources.next(), if !sources.is_empty() => {
                let dispatch = Dispatch {
                    url: meta.url_for(&claimed.envelope),
                    headers: base_headers(&meta, &claimed.envelope, &fairness),
                    source: meta,
                    claimed,
                };
                let labels = dispatch.source.labels.clone();
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
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Instant;

    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use crate::config::merge_policy::Fairness;
    use crate::dispatch::claim::{ClaimGuard, OutcomeSender};
    use crate::dispatch::message::{Claimed, Dispatch, SourceMeta};
    use crate::gate::release::Releases;
    use crate::merge::Source;
    use crate::merge::random_robin::run;
    use crate::store::queue::{ClaimRef, Outcome};
    use crate::store::test_support::envelope;
    use crate::telemetry::metrics::{Metrics, QueueLabels};

    struct Rig {
        control: mpsc::UnboundedSender<Source>,
        merged: mpsc::Receiver<Dispatch>,
        outcomes: OutcomeSender,
        _outcomes_rx: mpsc::UnboundedReceiver<Outcome>,
        metrics: Arc<Metrics>,
        cancel: CancellationToken,
    }

    impl Rig {
        fn start(buffer: usize) -> Self {
            let (control, control_rx) = mpsc::unbounded_channel();
            let (merged_tx, merged) = mpsc::channel(buffer);
            let (outcomes, _outcomes_rx) = mpsc::unbounded_channel();
            let metrics = Arc::new(Metrics::new().unwrap());
            let cancel = CancellationToken::new();
            tokio::spawn(run(
                control_rx,
                merged_tx,
                Fairness {
                    header: None,
                    attribute: "userid".into(),
                },
                Arc::clone(&metrics),
                cancel.clone(),
            ));
            Self {
                control,
                merged,
                outcomes,
                _outcomes_rx,
                metrics,
                cancel,
            }
        }

        /// Adds queue `name`; its requests go to `http://<name>`.
        fn source(&self, name: &str, capacity: usize) -> mpsc::Sender<Claimed> {
            let (tx, rx) = mpsc::channel(capacity);
            self.control
                .send(Source {
                    meta: Arc::new(SourceMeta {
                        labels: QueueLabels::new(name, name, "p"),
                        igw_base_url: format!("http://{name}"),
                        request_path: "/v1/completions".into(),
                        inference_objective: String::new(),
                    }),
                    rx,
                })
                .unwrap();
            tx
        }

        fn claimed(&self, id: &str) -> Claimed {
            Claimed {
                envelope: envelope(id, id, "q", i64::MAX / 1000),
                guard: ClaimGuard::new(
                    ClaimRef {
                        generation: id.into(),
                        claim_id: 1,
                    },
                    self.outcomes.clone(),
                ),
                releases: Releases::default(),
                ingested: Instant::now(),
            }
        }

        async fn next(&mut self) -> Dispatch {
            tokio::time::timeout(std::time::Duration::from_secs(5), self.merged.recv())
                .await
                .expect("a dispatch in time")
                .expect("merge running")
        }
    }

    #[tokio::test]
    async fn concurrent_queues_each_deliver_every_request_exactly_once() {
        let mut rig = Rig::start(16);
        let queues = ["a", "b", "c", "d"];
        let per_queue = 250;
        for name in queues {
            let tx = rig.source(name, 8);
            let claims: Vec<Claimed> = (0..per_queue)
                .map(|i| rig.claimed(&format!("{name}-{i}")))
                .collect();
            tokio::spawn(async move {
                for c in claims {
                    tx.send(c).await.unwrap();
                }
            });
        }
        let mut seen: BTreeMap<String, usize> = BTreeMap::new();
        for _ in 0..queues.len() * per_queue {
            let d = rig.next().await;
            let queue = d.source.labels.queue_name.clone();
            assert!(
                d.claimed
                    .envelope
                    .request
                    .id
                    .starts_with(&format!("{queue}-")),
                "dispatched with its own queue's source"
            );
            assert_eq!(d.url, format!("http://{queue}/v1/completions"));
            *seen.entry(d.claimed.envelope.request.id).or_default() += 1;
        }
        assert_eq!(seen.len(), queues.len() * per_queue, "no request lost");
        assert!(seen.values().all(|n| *n == 1), "no request duplicated");
        for name in queues {
            assert_eq!(
                rig.metrics
                    .sample("async_queue_depth", &[("queue_id", name)])
                    .unwrap_or(0.0),
                per_queue as f64,
                "{name}: merged but not yet taken by a worker"
            );
        }
    }

    #[tokio::test]
    async fn ready_queues_are_served_alternately_not_drained_in_turn() {
        let mut rig = Rig::start(1);
        let a = rig.source("a", 64);
        let b = rig.source("b", 64);
        for i in 0..40 {
            a.send(rig.claimed(&format!("a-{i}"))).await.unwrap();
            b.send(rig.claimed(&format!("b-{i}"))).await.unwrap();
        }
        let mut first_40 = BTreeMap::<String, usize>::new();
        for _ in 0..40 {
            let d = rig.next().await;
            *first_40
                .entry(d.source.labels.queue_name.clone())
                .or_default() += 1;
        }
        assert_eq!(first_40.len(), 2, "both queues served early: {first_40:?}");
        assert!(
            first_40.values().all(|n| *n >= 5),
            "neither queue starved: {first_40:?}"
        );
    }

    #[tokio::test]
    async fn a_queue_added_later_and_a_removed_queue_do_not_stall_the_rest() {
        let mut rig = Rig::start(4);
        let a = rig.source("a", 4);
        a.send(rig.claimed("a-0")).await.unwrap();
        assert_eq!(rig.next().await.claimed.envelope.request.id, "a-0");
        drop(a);
        let b = rig.source("b", 4);
        b.send(rig.claimed("b-0")).await.unwrap();
        assert_eq!(rig.next().await.claimed.envelope.request.id, "b-0");
    }

    #[tokio::test]
    async fn cancel_stops_the_merge() {
        let mut rig = Rig::start(4);
        let _a = rig.source("a", 4);
        rig.cancel.cancel();
        let closed = tokio::time::timeout(std::time::Duration::from_secs(5), rig.merged.recv())
            .await
            .expect("merge stopped in time");
        assert!(closed.is_none(), "merged channel closed");
    }
}
