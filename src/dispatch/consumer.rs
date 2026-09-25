use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

use tokio::sync::{Notify, mpsc};
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::api::request::InternalRequest;
use crate::api::result::ResultMessage;
use crate::api::routing::Classification;
use crate::clock::now_millis;
use crate::config::transport::QueueConfig;
use crate::dispatch::claim::{ClaimGuard, OutcomeSender};
use crate::dispatch::message::Claimed;
use crate::gate::release::Releases;
use crate::gate::{SharedGate, Verdict};
use crate::store::error::StoreError;
use crate::store::queue::{Admission, Admitted};
use crate::store::{Membership, Store};
use crate::telemetry::metrics::{GateReason, Metrics, QueueLabels};

/// A decided row: its admission and, for a claim, the request and the gate
/// reservations it keeps.
type Pending = (Admission, Option<(InternalRequest, Releases)>);

/// Polls one queue: each tick it reads the gate's budget, peeks up to
/// `batch_size × budget` requests in deadline order, gates each one, and
/// claims the admitted ones for the merge policy. Refused requests stay where
/// they are; peeking never moves them.
pub struct Consumer {
    pub store: Store,
    pub metrics: Arc<Metrics>,
    pub config: QueueConfig,
    pub labels: QueueLabels,
    pub gate: SharedGate,
    pub outcomes: OutcomeSender,
    pub results: Arc<Notify>,
    pub tx: mpsc::Sender<Claimed>,
    pub poll_interval: Duration,
    pub batch_size: usize,
}

impl Consumer {
    pub async fn run(self, cancel: CancellationToken) {
        self.metrics.init_gate_decisions(&self.labels);
        let _membership = tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            m = Membership::join(self.store.clone(), self.config.queue_name.clone()) => m,
        };
        let mut ticker = tokio::time::interval(self.poll_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                biased;
                () = cancel.cancelled() => return,
                _ = ticker.tick() => {}
            }
            if let Err(e) = self.poll(&cancel).await {
                tracing::error!(queue = %self.config.queue_name, error = %e, "queue poll failed");
            }
        }
    }

    /// Fills in routing a request picks up from the queue that serves it.
    fn stamp(&self, envelope: &mut InternalRequest) {
        let routing = &mut envelope.routing;
        if routing.request_queue_name.is_empty() {
            routing.request_queue_name = self.config.queue_name.clone();
        }
        if routing.queue_id.is_empty() {
            routing.queue_id = self.config.id.clone();
        }
        if !routing.result_routing_resolved {
            if !self.config.result_queue_name.is_empty() {
                routing.result_queue_name = self.config.result_queue_name.clone();
            }
            routing.result_ttl_seconds = self.config.result_ttl_seconds;
            routing.result_routing_resolved = true;
        }
        for (k, v) in &self.config.labels {
            routing.labels.insert(k.clone(), v.clone());
        }
    }

    fn refused(&self, envelope: &InternalRequest) {
        let reason = if envelope.routing.classification() == Some(Classification::Overflow) {
            GateReason::QuotaExhausted
        } else {
            GateReason::GateClosed
        };
        self.metrics.gate_decision(&self.labels, reason);
    }

    async fn poll(&self, cancel: &CancellationToken) -> Result<(), StoreError> {
        let queue = &self.config.queue_name;
        let budget = self.gate.budget().await;
        self.metrics.dispatch_budget(&self.labels, budget);
        let batch = (self.batch_size as f64 * budget).floor() as usize;
        if batch == 0 {
            // The budget held work back before any request was peeked; count
            // it, but only when work is actually waiting.
            if self.store.has_pending(queue.clone(), now_millis()).await? {
                self.metrics
                    .gate_decision(&self.labels, GateReason::GateClosed);
            }
            return Ok(());
        }

        let now_ms = now_millis();
        let mut pending: Vec<Pending> = Vec::new();
        for peeked in self.store.peek(queue.clone(), batch, now_ms).await? {
            let mut envelope = match peeked.envelope {
                Ok(envelope) => envelope,
                Err(e) => {
                    tracing::error!(%queue, error = %e, "discarding undecodable request");
                    pending.push((Admission::Discard { key: peeked.key }, None));
                    continue;
                }
            };
            self.stamp(&mut envelope);

            let finish = |envelope: InternalRequest, result: ResultMessage| {
                let admission = Admission::Finish {
                    key: peeked.key,
                    envelope: Box::new(envelope),
                    result: Box::new(result),
                };
                (admission, None)
            };
            if envelope.request.expired_at(now_ms) {
                tracing::info!(id = %envelope.request.id, "deadline expired in queue");
                self.metrics.exceeded_deadline(&self.labels);
                let result = ResultMessage::deadline_exceeded(&envelope);
                pending.push(finish(envelope, result));
                continue;
            }
            if peeked.cancelled {
                let result = ResultMessage::cancelled(&envelope);
                pending.push(finish(envelope, result));
                continue;
            }

            let mut releases = Releases::default();
            let verdict = {
                let apply = self.gate.apply(&mut envelope, &mut releases);
                tokio::pin!(apply);
                match std::future::poll_fn(|cx| Poll::Ready(apply.as_mut().poll(cx))).await {
                    Poll::Ready(verdict) => verdict,
                    Poll::Pending => {
                        // A blocking gate may be waiting on capacity held by
                        // rows of this very poll: send those first.
                        if !self
                            .flush(std::mem::take(&mut pending), now_ms, cancel)
                            .await?
                        {
                            return Ok(());
                        }
                        tokio::select! {
                            biased;
                            () = cancel.cancelled() => return Ok(()),
                            verdict = &mut apply => verdict,
                        }
                    }
                }
            };
            match verdict {
                Err(e) => {
                    tracing::error!(id = %envelope.request.id, error = %e, "gating failed");
                    self.metrics.gate_decision(&self.labels, GateReason::Error);
                }
                Ok(Verdict::Refuse | Verdict::Wait) => self.refused(&envelope),
                Ok(Verdict::Drop(result)) => {
                    self.metrics
                        .gate_decision(&self.labels, GateReason::Dropped);
                    let result =
                        result.map_or_else(|| ResultMessage::gate_dropped(&envelope), |r| *r);
                    pending.push(finish(envelope, result));
                }
                Ok(Verdict::Continue) => {
                    let admission = Admission::Claim {
                        key: peeked.key,
                        generation: envelope.generation_key(),
                    };
                    pending.push((admission, Some((envelope, releases))));
                }
            }
        }
        self.flush(pending, now_ms, cancel).await?;
        Ok(())
    }

    /// Admits `pending` in one transaction and sends the claims on. False
    /// when the consumer is stopping.
    async fn flush(
        &self,
        pending: Vec<Pending>,
        now_ms: i64,
        cancel: &CancellationToken,
    ) -> Result<bool, StoreError> {
        if pending.is_empty() {
            return Ok(true);
        }
        let (admissions, requests): (Vec<_>, Vec<_>) = pending.into_iter().unzip();
        let admitted = self
            .store
            .admit(self.config.queue_name.clone(), admissions, now_ms)
            .await?;
        if admitted.contains(&Admitted::Finished) {
            self.results.notify_waiters();
        }
        // Every claim gets its guard before the first send, so any claim left
        // unsent (shutdown, removed queue) is released when dropped.
        let claimed: Vec<Claimed> = admitted
            .into_iter()
            .zip(requests)
            .filter_map(|(outcome, request)| match (outcome, request) {
                (Admitted::Claimed(claim), Some((envelope, releases))) => Some(Claimed {
                    envelope,
                    guard: ClaimGuard::new(claim, self.outcomes.clone()),
                    releases,
                    ingested: Instant::now(),
                }),
                _ => None,
            })
            .collect();
        for claimed in claimed {
            let sent = tokio::select! {
                biased;
                () = cancel.cancelled() => false,
                sent = self.tx.send(claimed) => sent.is_ok(),
            };
            if !sent {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use serde_json::json;
    use tokio::sync::{Notify, mpsc};
    use tokio_util::sync::CancellationToken;

    use crate::clock::now_millis;
    use crate::dispatch::consumer::Consumer;
    use crate::dispatch::message::Claimed;
    use crate::gate::admission::GatingMode;
    use crate::gate::admission::counters::Counters;
    use crate::gate::admission::counters::local::LocalCounters;
    use crate::gate::admission::quota::{QuotaGate, QuotaMode};
    use crate::gate::test_support::FixedGate;
    use crate::gate::{SharedGate, Verdict};
    use crate::store::Store;
    use crate::store::embedded::test_support::{Fixture, open};
    use crate::store::queue::Outcome;
    use crate::store::test_support::new_request;
    use crate::telemetry::metrics::{Metrics, QueueLabels};

    struct Rig {
        consumer: Consumer,
        claims: mpsc::Receiver<Claimed>,
        _outcomes: mpsc::UnboundedReceiver<Outcome>,
        _f: Fixture,
    }

    impl Rig {
        async fn new(gate: SharedGate, config: serde_json::Value) -> Self {
            let f = open().await;
            let (outcomes, outcomes_rx) = mpsc::unbounded_channel();
            let (tx, claims) = mpsc::channel(64);
            let mut config = config;
            config["queue_name"] = json!("q");
            config["igw_base_url"] = json!("http://gw");
            let consumer = Consumer {
                store: Arc::clone(&f.store),
                metrics: Arc::new(Metrics::new().unwrap()),
                config: serde_json::from_value(config).unwrap(),
                labels: QueueLabels::new("q", "q", "p"),
                gate,
                outcomes,
                results: Arc::new(Notify::new()),
                tx,
                poll_interval: Duration::from_millis(100),
                batch_size: 10,
            };
            consumer.metrics.init_gate_decisions(&consumer.labels);
            Self {
                consumer,
                claims,
                _outcomes: outcomes_rx,
                _f: f,
            }
        }

        fn store(&self) -> &Store {
            &self.consumer.store
        }

        async fn submit(&self, id: &str, deadline: i64, metadata: &[(&str, &str)]) {
            let mut req = new_request(id, "q", deadline);
            for (k, v) in metadata {
                req.envelope
                    .request
                    .metadata
                    .insert((*k).into(), (*v).into());
            }
            self.store().submit(vec![req]).await.unwrap();
        }

        async fn poll(&self) {
            self.consumer.poll(&CancellationToken::new()).await.unwrap();
        }

        fn claimed(&mut self) -> Vec<Claimed> {
            let mut out = Vec::new();
            while let Ok(c) = self.claims.try_recv() {
                out.push(c);
            }
            out
        }

        async fn queued(&self) -> usize {
            self.store()
                .peek("q".into(), 100, now_millis())
                .await
                .unwrap()
                .len()
        }

        async fn results(&self) -> Vec<serde_json::Value> {
            let mut out = Vec::new();
            while let Some(r) = self
                .store()
                .pop_result("results".into(), now_millis())
                .await
                .unwrap()
            {
                out.push(serde_json::from_str(&r).unwrap());
            }
            out
        }

        fn gate_decisions(&self, reason: &str) -> f64 {
            self.consumer
                .metrics
                .sample(
                    "async_gate_decisions_total",
                    &[("queue_id", "q"), ("reason", reason)],
                )
                .unwrap()
        }
    }

    fn secs_from_now(secs: i64) -> i64 {
        now_millis() / 1000 + secs
    }

    fn fixed(budget: f64, verdict: Verdict) -> SharedGate {
        Arc::new(FixedGate::new(budget, verdict))
    }

    #[tokio::test]
    async fn a_partial_budget_scales_the_batch() {
        let mut rig = Rig::new(fixed(0.3, Verdict::Continue), json!({})).await;
        for i in 0..10 {
            rig.submit(&format!("r{i}"), secs_from_now(60 + i), &[])
                .await;
        }
        rig.poll().await;
        let ids: Vec<String> = rig
            .claimed()
            .into_iter()
            .map(|c| c.envelope.request.id)
            .collect();
        assert_eq!(
            ids,
            ["r0", "r1", "r2"],
            "floor(10 x 0.3), earliest deadline first"
        );
        assert_eq!(rig.queued().await, 7);
        assert_eq!(
            rig.consumer
                .metrics
                .sample("async_dispatch_budget", &[("queue_id", "q")]),
            Some(0.3)
        );
    }

    #[tokio::test]
    async fn a_zero_budget_counts_gate_closed_only_when_work_waits() {
        let mut rig = Rig::new(fixed(0.0, Verdict::Continue), json!({})).await;
        rig.poll().await;
        assert_eq!(rig.gate_decisions("gate_closed"), 0.0, "nothing waiting");
        rig.submit("r", secs_from_now(60), &[]).await;
        rig.poll().await;
        assert_eq!(rig.gate_decisions("gate_closed"), 1.0);
        assert!(rig.claimed().is_empty());
        assert_eq!(rig.queued().await, 1);
    }

    #[tokio::test]
    async fn refused_requests_stay_queued_and_count_their_reason() {
        let counters: Arc<dyn Counters> = Arc::new(LocalCounters::default());
        let quota = QuotaGate::new(
            "userid".into(),
            "quota:".into(),
            QuotaMode::Concurrency,
            GatingMode::Blocking,
            1,
            Duration::from_secs(10),
            counters,
        );
        let mut rig = Rig::new(Arc::new(quota), json!({})).await;
        rig.submit("tenant-1", secs_from_now(60), &[("userid", "t")])
            .await;
        rig.submit("tenant-2", secs_from_now(61), &[("userid", "t")])
            .await;
        rig.submit("anonymous", secs_from_now(62), &[]).await;
        rig.poll().await;
        let claimed = rig.claimed();
        let ids: Vec<&str> = claimed
            .iter()
            .map(|c| c.envelope.request.id.as_str())
            .collect();
        assert_eq!(ids, ["tenant-1", "anonymous"]);
        assert_eq!(rig.gate_decisions("quota_exhausted"), 1.0);
        assert_eq!(rig.gate_decisions("gate_closed"), 0.0);
        let queued = rig
            .store()
            .peek("q".into(), 10, now_millis())
            .await
            .unwrap();
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].envelope.as_ref().unwrap().request.id, "tenant-2");

        drop(claimed);
        rig.poll().await;
        assert_eq!(rig.claimed().len(), 1, "the slot came back with the claim");
    }

    #[tokio::test]
    async fn a_closed_gate_counts_gate_closed() {
        let mut rig = Rig::new(fixed(1.0, Verdict::Refuse), json!({})).await;
        rig.submit("r", secs_from_now(60), &[]).await;
        rig.poll().await;
        assert!(rig.claimed().is_empty());
        assert_eq!(rig.queued().await, 1);
        assert_eq!(rig.gate_decisions("gate_closed"), 1.0);
    }

    #[tokio::test]
    async fn a_dropped_request_finishes_with_gate_dropped() {
        let mut rig = Rig::new(fixed(1.0, Verdict::Drop(None)), json!({})).await;
        rig.submit("r", secs_from_now(60), &[]).await;
        rig.poll().await;
        assert!(rig.claimed().is_empty());
        assert_eq!(rig.queued().await, 0);
        let results = rig.results().await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["id"], "r");
        assert_eq!(results[0]["error_code"], "GATE_DROPPED");
        assert_eq!(rig.gate_decisions("dropped"), 1.0);
    }

    #[tokio::test]
    async fn expired_and_cancelled_requests_finish_without_a_claim() {
        let gate = Arc::new(FixedGate::new(1.0, Verdict::Continue));
        let mut rig = Rig::new(gate.clone(), json!({})).await;
        rig.submit("expired", secs_from_now(-1), &[]).await;
        rig.submit("cancelled", secs_from_now(60), &[]).await;
        rig.store()
            .cancel(vec!["cancelled".into()], now_millis())
            .await
            .unwrap();
        rig.poll().await;
        assert!(rig.claimed().is_empty());
        assert_eq!(rig.queued().await, 0);
        assert_eq!(
            gate.held.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "never gated"
        );
        let mut codes: Vec<(String, String)> = rig
            .results()
            .await
            .iter()
            .map(|r| {
                (
                    r["id"].as_str().unwrap().to_owned(),
                    r["error_code"].as_str().unwrap().to_owned(),
                )
            })
            .collect();
        codes.sort();
        assert_eq!(
            codes,
            [
                ("cancelled".to_owned(), "CANCELLED".to_owned()),
                ("expired".to_owned(), "DEADLINE_EXCEEDED".to_owned()),
            ]
        );
    }

    #[tokio::test]
    async fn claims_carry_the_queues_routing() {
        let mut rig = Rig::new(
            fixed(1.0, Verdict::Continue),
            json!({"id": "qid", "result_queue_name": "custom", "result_ttl_seconds": 30,
                   "labels": {"tier": "batch"}}),
        )
        .await;
        rig.submit("r", secs_from_now(60), &[]).await;
        rig.poll().await;
        let claimed = rig.claimed();
        let routing = &claimed[0].envelope.routing;
        assert_eq!(routing.queue_id, "qid");
        assert_eq!(routing.result_queue_name, "custom");
        assert_eq!(routing.result_ttl_seconds, 30);
        assert!(routing.result_routing_resolved);
        assert_eq!(routing.labels["tier"], "batch");
    }

    #[tokio::test]
    async fn a_gate_error_leaves_the_request_queued() {
        let Some(pg) = crate::store::postgres::test_support::fixture().await else {
            return;
        };
        let quota = QuotaGate::new(
            "userid".into(),
            "quota:".into(),
            QuotaMode::Concurrency,
            GatingMode::Blocking,
            1,
            Duration::from_secs(10),
            Arc::new(pg.pg.counters()),
        );
        pg.db.pool.close();
        let mut rig = Rig::new(Arc::new(quota), json!({})).await;
        rig.submit("r", secs_from_now(60), &[("userid", "t")]).await;
        rig.poll().await;
        assert!(rig.claimed().is_empty());
        assert_eq!(rig.queued().await, 1);
        assert!(rig.results().await.is_empty());
        assert_eq!(rig.gate_decisions("error"), 1.0);
    }
}
