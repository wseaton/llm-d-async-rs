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
                Verdict::Refuse | Verdict::Wait => self.refused(&envelope),
                Verdict::Drop(result) => {
                    self.metrics
                        .gate_decision(&self.labels, GateReason::Dropped);
                    let result = result.unwrap_or_else(|| ResultMessage::gate_dropped(&envelope));
                    pending.push(finish(envelope, result));
                }
                Verdict::Continue => {
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
