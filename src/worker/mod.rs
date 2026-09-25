//! Workers: take dispatches from a pool's merged channel, pass the pool gate,
//! call the inference gateway, and turn the response into an outcome.

pub mod backoff;
pub mod client;
pub mod usage;

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::time::{Instant as TokioInstant, sleep, sleep_until, timeout_at};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use crate::api::request::InternalRequest;
use crate::api::result::{ErrorCode, ResultMessage};
use crate::api::routing::Classification;
use crate::clock::now_millis;
use crate::dispatch::message::{Claimed, Dispatch};
use crate::gate::release::Releases;
use crate::gate::{SharedGate, Verdict};
use crate::merge::DispatchReceiver;
use crate::merge::headers::Headers;
use crate::store::blob::BlobKey;
use crate::store::requests::Outcome;
use crate::store::{PayloadBody, Store};
use crate::telemetry::metrics::{GateReason, Metrics, QueueLabels};
use crate::telemetry::propagation;
use crate::worker::backoff::{
    first_gate_wait, jittered, next_gate_wait, retry_backoff_secs, with_retry_after,
};
use crate::worker::client::{ClientError, InferenceClient, ResponseBody};
use crate::worker::usage::parse_usage;

const CANCEL_CHECK_INTERVAL: Duration = Duration::from_secs(1);
/// Backoff after a failed cancellation or payload read.
const STORE_ERROR_RETRY_SECS: f64 = 1.0;

/// How one dispatch ends.
enum End {
    Finish(ResultMessage),
    /// Back to the queue after this many seconds.
    Retry(f64),
    /// Back to the queue now, untouched.
    Release,
}

pub struct Worker {
    pub store: Store,
    pub metrics: Arc<Metrics>,
    pub client: Arc<InferenceClient>,
    pub pool: String,
    pub pool_gate: Option<SharedGate>,
    pub request_timeout: Duration,
    /// 0 waits at the pool gate until the request deadline.
    pub gate_wait_timeout: Duration,
    /// Stop taking new dispatches.
    pub consume: CancellationToken,
    /// Abort in-flight work; it returns to its queue.
    pub drain: CancellationToken,
}

struct Inflight<'a> {
    metrics: &'a Metrics,
    labels: &'a QueueLabels,
}

impl<'a> Inflight<'a> {
    fn start(metrics: &'a Metrics, labels: &'a QueueLabels) -> Self {
        metrics.inflight_inc(labels);
        Self { metrics, labels }
    }
}

impl Drop for Inflight<'_> {
    fn drop(&mut self) {
        self.metrics.inflight_dec(self.labels);
    }
}

fn gate_reason(envelope: &InternalRequest) -> GateReason {
    if envelope.routing.classification() == Some(Classification::Overflow) {
        GateReason::QuotaExhausted
    } else {
        GateReason::GateClosed
    }
}

/// The instant `deadline_s` (Unix seconds) falls on.
fn deadline_instant(deadline_s: i64) -> TokioInstant {
    let remaining_ms = deadline_s.saturating_mul(1000).saturating_sub(now_millis());
    TokioInstant::now() + Duration::from_millis(u64::try_from(remaining_ms).unwrap_or(0))
}

fn payload_model(payload: &PayloadBody) -> Option<String> {
    #[derive(serde::Deserialize)]
    struct Model {
        model: String,
    }
    match payload {
        PayloadBody::Inline(bytes) => serde_json::from_slice::<Model>(bytes).ok().map(|m| m.model),
        PayloadBody::File { .. } => None,
    }
}

impl Worker {
    pub async fn run(self: Arc<Self>, rx: DispatchReceiver) {
        loop {
            let next = tokio::select! {
                biased;
                () = self.consume.cancelled() => break,
                next = async { rx.lock().await.recv().await } => next,
            };
            match next {
                Some(dispatch) => self.process(dispatch).await,
                None => return,
            }
        }
        // Buffered dispatches go back to their queues as they are dropped.
        loop {
            let next = rx.lock().await.recv().await;
            match next {
                Some(dispatch) => self.metrics.queue_depth_dec(&dispatch.source.labels),
                None => return,
            }
        }
    }

    async fn process(&self, dispatch: Dispatch) {
        let Dispatch {
            claimed,
            source,
            url,
            headers,
        } = dispatch;
        let Claimed {
            mut envelope,
            guard,
            releases,
            ingested,
        } = claimed;
        let labels = &source.labels;
        self.metrics.queue_depth_dec(labels);
        self.metrics
            .queue_residence(labels, ingested.elapsed().as_millis() as f64);
        if envelope.routing.retry_count == 0 {
            self.metrics.async_request(labels);
        }

        let end = self.decide(&mut envelope, &url, &headers, labels).await;
        match end {
            End::Finish(result) => guard.finish(|claim| Outcome::Finish {
                claim,
                envelope,
                result,
            }),
            End::Retry(secs) => {
                let due_ms = now_millis().saturating_add((secs * 1000.0) as i64);
                guard.finish(|claim| Outcome::Retry {
                    claim,
                    envelope,
                    due_ms,
                });
            }
            End::Release => drop(guard),
        }
        drop(releases);
    }

    async fn decide(
        &self,
        envelope: &mut InternalRequest,
        url: &str,
        headers: &Headers,
        labels: &QueueLabels,
    ) -> End {
        let mut next_cancel_check = None;
        if let Some(end) = self.check_cancelled(envelope, &mut next_cancel_check).await {
            return end;
        }
        let deadline = envelope.request.deadline;
        if deadline <= 0 {
            self.metrics.failed(labels);
            return End::Finish(ResultMessage::error(
                envelope,
                ErrorCode::InvalidRequest,
                "Failed: deadline is missing or invalid (Unix seconds).",
            ));
        }
        if envelope.request.expired_at(now_millis()) {
            self.metrics.exceeded_deadline(labels);
            return End::Finish(ResultMessage::deadline_exceeded(envelope));
        }

        let mut pool_releases = Releases::default();
        if let Some(gate) = &self.pool_gate
            && let Some(end) = self
                .pass_pool_gate(
                    gate,
                    envelope,
                    &mut pool_releases,
                    &mut next_cancel_check,
                    labels,
                )
                .await
        {
            return end;
        }
        if let Some(end) = self.check_cancelled(envelope, &mut next_cancel_check).await {
            return end;
        }
        let _inflight = Inflight::start(&self.metrics, labels);
        self.send(envelope, url, headers, labels).await
    }

    /// `Some` when the request was cancelled, or when cancellation could not
    /// be checked (the request is retried). Checks at most once a second.
    async fn check_cancelled(
        &self,
        envelope: &InternalRequest,
        next_check: &mut Option<Instant>,
    ) -> Option<End> {
        let now = Instant::now();
        if next_check.is_some_and(|at| now < at) {
            return None;
        }
        *next_check = Some(now + CANCEL_CHECK_INTERVAL);
        let checked = self
            .store
            .is_cancelled(
                envelope.request.id.clone(),
                envelope.routing.request_token.clone(),
                now_millis(),
            )
            .await;
        match checked {
            Ok(false) => None,
            Ok(true) => Some(End::Finish(ResultMessage::cancelled(envelope))),
            Err(e) => {
                tracing::error!(id = %envelope.request.id, error = %e, "failed to check request cancellation");
                Some(End::Retry(STORE_ERROR_RETRY_SECS))
            }
        }
    }

    /// `None` once the gate admits; otherwise how the dispatch ends.
    async fn pass_pool_gate(
        &self,
        gate: &SharedGate,
        envelope: &mut InternalRequest,
        releases: &mut Releases,
        next_cancel_check: &mut Option<Instant>,
        labels: &QueueLabels,
    ) -> Option<End> {
        let pool_labels = QueueLabels::pool(&self.pool);
        let message_deadline = deadline_instant(envelope.request.deadline);
        let gate_deadline = if self.gate_wait_timeout.is_zero() {
            message_deadline
        } else {
            message_deadline.min(TokioInstant::now() + self.gate_wait_timeout)
        };
        let mut wait = first_gate_wait();
        let mut wait_recorded = false;
        loop {
            if let Some(end) = self.check_cancelled(envelope, next_cancel_check).await {
                return Some(end);
            }
            let verdict = tokio::select! {
                biased;
                () = self.drain.cancelled() => return Some(End::Release),
                () = sleep_until(gate_deadline) => return Some(self.gate_timed_out(envelope, labels)),
                verdict = gate.apply(envelope, releases) => verdict,
            };
            match verdict {
                Verdict::Continue => return None,
                Verdict::Drop(result) => {
                    self.metrics
                        .gate_decision(&pool_labels, GateReason::Dropped);
                    return Some(End::Finish(
                        result.unwrap_or_else(|| ResultMessage::gate_dropped(envelope)),
                    ));
                }
                Verdict::Refuse => {
                    self.metrics
                        .gate_decision(&pool_labels, gate_reason(envelope));
                    return Some(End::Retry(0.0));
                }
                Verdict::Wait => {
                    if !wait_recorded {
                        self.metrics
                            .gate_decision(&pool_labels, gate_reason(envelope));
                        wait_recorded = true;
                    }
                    tokio::select! {
                        biased;
                        () = self.drain.cancelled() => return Some(End::Release),
                        () = sleep_until(gate_deadline) => return Some(self.gate_timed_out(envelope, labels)),
                        () = sleep(jittered(wait, rand::random())) => wait = next_gate_wait(wait),
                    }
                }
            }
        }
    }

    /// Only the request's own deadline is terminal; the gate wait timeout
    /// returns it to its queue.
    fn gate_timed_out(&self, envelope: &InternalRequest, labels: &QueueLabels) -> End {
        if envelope.request.expired_at(now_millis()) {
            self.metrics.exceeded_deadline(labels);
            End::Finish(ResultMessage::deadline_exceeded(envelope))
        } else {
            self.metrics.gate_wait_requeue(labels);
            End::Retry(0.0)
        }
    }

    async fn send(
        &self,
        envelope: &mut InternalRequest,
        url: &str,
        headers: &Headers,
        labels: &QueueLabels,
    ) -> End {
        let id = envelope.request.id.clone();
        let retry_count = envelope.routing.retry_count;
        let span = tracing::info_span!(
            "process-request",
            gen_ai.request.id = %id,
            request.id = %id,
            llm_d.async.retry_count = retry_count,
            retry.count = retry_count,
            llm_d.async.queue.id = %labels.queue_id,
            queue.id = %labels.queue_id,
            llm_d.async.queue.name = %labels.queue_name,
            queue.name = %labels.queue_name,
            gen_ai.request.model = tracing::field::Empty,
            llm_d.async.error.category = tracing::field::Empty,
            error.category = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        propagation::set_parent_from_metadata(&span, &envelope.request.metadata);
        self.send_in_span(envelope, url, headers, labels, &span)
            .instrument(span.clone())
            .await
    }

    async fn send_in_span(
        &self,
        envelope: &mut InternalRequest,
        url: &str,
        headers: &Headers,
        labels: &QueueLabels,
        span: &tracing::Span,
    ) -> End {
        let request_deadline = deadline_instant(envelope.request.deadline)
            .min(TokioInstant::now() + self.request_timeout);

        let payload = match self.store.open_payload(envelope).await {
            Ok(Some(payload)) => payload,
            Ok(None) => {
                self.metrics.failed(labels);
                return End::Finish(ResultMessage::error(
                    envelope,
                    ErrorCode::PayloadUnavailable,
                    "request payload is missing",
                ));
            }
            Err(e) => {
                tracing::error!(error = %e, "failed to open request payload");
                return End::Retry(STORE_ERROR_RETRY_SECS);
            }
        };
        if !envelope.request.model.is_empty() {
            span.record("gen_ai.request.model", envelope.request.model.as_str());
        } else if !span.is_disabled()
            && let Some(model) = payload_model(&payload)
        {
            span.record("gen_ai.request.model", model.as_str());
        }

        let mut header_map = match headers.to_header_map() {
            Ok(map) => map,
            Err(e) => {
                self.record_error(span, "UNKNOWN");
                self.metrics.failed(labels);
                return End::Finish(ResultMessage::error(
                    envelope,
                    ErrorCode::InferenceError,
                    format!("Failed to send request to inference: {e}"),
                ));
            }
        };
        propagation::inject_current(&mut header_map);
        let Some(result_key) = BlobKey::result(&envelope.routing.request_token) else {
            self.metrics.failed(labels);
            return End::Finish(ResultMessage::error(
                envelope,
                ErrorCode::InvalidRequest,
                "request token cannot name a result blob",
            ));
        };

        self.metrics.dispatched(labels);
        tracing::debug!(%url, "sending inference request");
        let started = Instant::now();
        let sent = tokio::select! {
            biased;
            () = self.drain.cancelled() => return End::Release,
            sent = timeout_at(
                request_deadline,
                self.client.send(url, header_map, payload, &result_key),
            ) => sent,
        };
        self.metrics
            .inference_latency(labels, started.elapsed().as_millis() as f64);

        match sent {
            Err(_elapsed) => {
                self.metrics.exceeded_deadline(labels);
                End::Finish(ResultMessage::deadline_exceeded(envelope))
            }
            Ok(Ok(response)) => {
                self.metrics.succeeded(labels);
                match response.body {
                    ResponseBody::Inline(body) => {
                        if (200..300).contains(&response.status)
                            && let Some((input, output)) = parse_usage(&body, url)
                        {
                            self.metrics.tokens(labels, input, output);
                        }
                        End::Finish(ResultMessage::http(envelope, response.status, &body))
                    }
                    ResponseBody::Stored(stored) => End::Finish(ResultMessage::http_by_reference(
                        envelope,
                        response.status,
                        stored,
                    )),
                }
            }
            Ok(Err(error)) => self.failed(envelope, labels, span, *error),
        }
    }

    fn record_error(&self, span: &tracing::Span, category: &str) {
        span.record("llm_d.async.error.category", category);
        span.record("error.category", category);
    }

    fn failed(
        &self,
        envelope: &mut InternalRequest,
        labels: &QueueLabels,
        span: &tracing::Span,
        error: ClientError,
    ) -> End {
        self.record_error(span, error.category.as_str());
        if error.category.fatal() {
            span.record("otel.status_code", "ERROR");
            tracing::warn!(error = %error, "inference request failed");
            self.metrics.failed(labels);
            return End::Finish(if error.status > 0 {
                ResultMessage::http(envelope, error.status, &error.body)
            } else {
                ResultMessage::error(
                    envelope,
                    ErrorCode::InferenceError,
                    format!("Failed to send request to inference: {error}"),
                )
            });
        }
        if error.category.sheddable() {
            self.metrics.shed(labels);
        }
        self.retry(envelope, labels, &error)
    }

    /// Schedules another attempt, or finishes the request with its last
    /// response when the deadline leaves no room.
    fn retry(
        &self,
        envelope: &mut InternalRequest,
        labels: &QueueLabels,
        last: &ClientError,
    ) -> End {
        let secs_left = envelope.request.secs_left(now_millis());
        if secs_left <= 0 {
            self.metrics.exceeded_deadline(labels);
            return End::Finish(if last.status > 0 {
                ResultMessage::http(envelope, last.status, &last.body)
            } else {
                ResultMessage::deadline_exceeded(envelope)
            });
        }
        let backoff =
            retry_backoff_secs(envelope.routing.retry_count + 1, secs_left, rand::random());
        let backoff = with_retry_after(backoff, last.retry_after, secs_left, rand::random());
        envelope.routing.retry_count += 1;
        self.metrics.retry(labels);
        End::Retry(backoff)
    }
}
