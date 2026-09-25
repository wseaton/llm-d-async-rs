//! Workers: take dispatches from a pool's merged channel, pass the pool gate,
//! call the inference gateway, and turn the response into an outcome.

pub mod backoff;
pub mod client;
pub mod usage;

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
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
use crate::store::Store;
use crate::store::blob::key::BlobKey;
use crate::store::queue::Outcome;
use crate::store::queue::PayloadBody;
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

/// Where one dispatch goes and what it sends.
struct Dispatching<'a> {
    url: &'a str,
    headers: &'a Headers,
    labels: &'a QueueLabels,
    /// The claim this dispatch runs under; it names the result blob.
    claim_id: u64,
    /// The inline request body that came with the claim, taken at send.
    payload: Option<Bytes>,
}

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
        PayloadBody::Blob(_) => None,
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
            payload,
        } = claimed;
        let labels = &source.labels;
        self.metrics.queue_depth_dec(labels);
        self.metrics
            .queue_residence(labels, ingested.elapsed().as_millis() as f64);
        if envelope.routing.retry_count == 0 {
            self.metrics.async_request(labels);
        }

        let mut dispatching = Dispatching {
            url: &url,
            headers: &headers,
            labels,
            claim_id: guard.claim_id(),
            payload,
        };
        let end = self.decide(&mut envelope, &mut dispatching).await;
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

    async fn decide(&self, envelope: &mut InternalRequest, d: &mut Dispatching<'_>) -> End {
        let labels = d.labels;
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
        self.send(envelope, d).await
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
                Err(e) => {
                    tracing::error!(id = %envelope.request.id, error = %e, "pool gating failed");
                    self.metrics.gate_decision(&pool_labels, GateReason::Error);
                    return Some(End::Finish(ResultMessage::gate_error(envelope, &e)));
                }
                Ok(Verdict::Continue) => return None,
                Ok(Verdict::Drop(result)) => {
                    self.metrics
                        .gate_decision(&pool_labels, GateReason::Dropped);
                    return Some(End::Finish(
                        result.map_or_else(|| ResultMessage::gate_dropped(envelope), |r| *r),
                    ));
                }
                Ok(Verdict::Refuse) => {
                    self.metrics
                        .gate_decision(&pool_labels, gate_reason(envelope));
                    return Some(End::Retry(0.0));
                }
                Ok(Verdict::Wait) => {
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

    async fn send(&self, envelope: &mut InternalRequest, d: &mut Dispatching<'_>) -> End {
        let labels = d.labels;
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
        self.send_in_span(envelope, d, &span)
            .instrument(span.clone())
            .await
    }

    async fn send_in_span(
        &self,
        envelope: &mut InternalRequest,
        d: &mut Dispatching<'_>,
        span: &tracing::Span,
    ) -> End {
        let (url, headers, labels) = (d.url, d.headers, d.labels);
        let request_deadline = deadline_instant(envelope.request.deadline)
            .min(TokioInstant::now() + self.request_timeout);

        let opened = match d.payload.take() {
            Some(bytes) => Ok(Some(PayloadBody::Inline(bytes))),
            None => self.store.open_payload(envelope).await,
        };
        let payload = match opened {
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
        let Some(result_key) = BlobKey::result(&envelope.routing.request_token, d.claim_id) else {
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
            sent = timeout_at(
                request_deadline,
                self.client.send(url, header_map, payload, &result_key),
            ) => sent,
            () = self.drain.cancelled() => return End::Release,
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

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use axum::http::StatusCode;
    use axum::response::{IntoResponse, Response};
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    use crate::api::request::InternalRequest;
    use crate::api::result::ErrorCode;
    use crate::boxed::BoxFuture;
    use crate::clock::now_millis;
    use crate::dispatch::claim::{ClaimGuard, OutcomeSender};
    use crate::dispatch::message::{Claimed, Dispatch, SourceMeta};
    use crate::gate::admission::GatingMode;
    use crate::gate::admission::quota::{QuotaGate, QuotaMode};
    use crate::gate::release::Releases;
    use crate::gate::test_support::FixedGate;
    use crate::gate::{SharedGate, Verdict};
    use crate::merge::headers::Headers;
    use crate::store::Store;
    use crate::store::blob::BlobStore;
    use crate::store::blob::local::LocalBlobs;
    use crate::store::queue::{Admission, Admitted, ClaimRef, Outcome};
    use crate::store::test_support::{envelope, new_request};
    use crate::telemetry::metrics::{Metrics, QueueLabels};
    use crate::worker::Worker;
    use crate::worker::client::InferenceClient;

    const POOL: &str = "p";

    /// A stand-in inference gateway answering the `n`th request with
    /// `respond(n)`.
    struct Gateway {
        url: String,
        hits: Arc<AtomicUsize>,
    }

    impl Gateway {
        async fn start(
            respond: impl Fn(usize) -> BoxFuture<'static, Response> + Send + Sync + 'static,
        ) -> Self {
            let hits = Arc::new(AtomicUsize::new(0));
            let respond = Arc::new(respond);
            let counter = Arc::clone(&hits);
            let app = axum::Router::new().fallback(move || {
                let n = counter.fetch_add(1, Ordering::SeqCst);
                respond(n)
            });
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}/v1/completions", listener.local_addr().unwrap());
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            Self { url, hits }
        }

        async fn replying(
            status: StatusCode,
            headers: &[(&'static str, &str)],
            body: &str,
        ) -> Self {
            let headers: Vec<(&'static str, String)> =
                headers.iter().map(|(k, v)| (*k, (*v).to_owned())).collect();
            let body = body.to_owned();
            Self::start(move |_| {
                let mut response = (status, body.clone()).into_response();
                for (k, v) in &headers {
                    response.headers_mut().insert(*k, v.parse().unwrap());
                }
                Box::pin(async move { response })
            })
            .await
        }

        async fn hanging() -> Self {
            Self::start(|_| Box::pin(std::future::pending())).await
        }

        fn hits(&self) -> usize {
            self.hits.load(Ordering::SeqCst)
        }
    }

    struct Rig {
        store: Store,
        blobs: BlobStore,
        metrics: Arc<Metrics>,
        outcomes_tx: OutcomeSender,
        outcomes: mpsc::UnboundedReceiver<Outcome>,
        _keep: Box<dyn Any + Send>,
    }

    impl Rig {
        async fn embedded() -> Self {
            let f = crate::store::embedded::test_support::open().await;
            let blobs = BlobStore::new(Arc::new(
                LocalBlobs::open(&f.dir.path().join("blobs")).unwrap(),
            ));
            Self::new(Arc::clone(&f.store), blobs, Box::new(f))
        }

        fn new(store: Store, blobs: BlobStore, keep: Box<dyn Any + Send>) -> Self {
            let (outcomes_tx, outcomes) = mpsc::unbounded_channel();
            Self {
                store,
                blobs,
                metrics: Arc::new(Metrics::new().unwrap()),
                outcomes_tx,
                outcomes,
                _keep: keep,
            }
        }

        fn worker(&self, pool_gate: Option<SharedGate>) -> Worker {
            Worker {
                store: Arc::clone(&self.store),
                metrics: Arc::clone(&self.metrics),
                client: Arc::new(InferenceClient::new(
                    reqwest::Client::new(),
                    self.blobs.clone(),
                )),
                pool: POOL.into(),
                pool_gate,
                request_timeout: Duration::from_secs(30),
                gate_wait_timeout: Duration::ZERO,
                consume: CancellationToken::new(),
                drain: CancellationToken::new(),
            }
        }

        /// Submits `id` to queue `q` and claims it, as a consumer would.
        async fn dispatch(&self, id: &str, deadline: i64, url: &str) -> Dispatch {
            self.store
                .submit(vec![new_request(id, "q", deadline)])
                .await
                .unwrap();
            let head = self
                .store
                .peek("q".into(), 1, now_millis())
                .await
                .unwrap()
                .remove(0);
            let envelope = head.envelope.unwrap();
            let admitted = self
                .store
                .admit(
                    "q".into(),
                    vec![Admission::Claim {
                        key: head.key,
                        generation: envelope.generation_key(),
                    }],
                    now_millis(),
                )
                .await
                .unwrap();
            let Some(Admitted::Claimed { claim, payload }) = admitted.into_iter().next() else {
                panic!("not claimed")
            };
            self.dispatch_of(envelope, claim, payload, url)
        }

        fn dispatch_of(
            &self,
            envelope: InternalRequest,
            claim: ClaimRef,
            payload: Option<bytes::Bytes>,
            url: &str,
        ) -> Dispatch {
            Dispatch {
                claimed: Claimed {
                    envelope,
                    guard: ClaimGuard::new(claim, self.outcomes_tx.clone()),
                    releases: Releases::default(),
                    ingested: Instant::now(),
                    payload,
                },
                source: Arc::new(SourceMeta {
                    labels: QueueLabels::new("q", "q", POOL),
                    igw_base_url: String::new(),
                    request_path: String::new(),
                    inference_objective: String::new(),
                }),
                url: url.into(),
                headers: Headers::default(),
            }
        }

        fn outcome(&mut self) -> Outcome {
            let outcome = self.outcomes.try_recv().expect("an outcome");
            assert!(self.outcomes.try_recv().is_err(), "exactly one outcome");
            outcome
        }

        fn gate_decisions(&self, reason: &str) -> f64 {
            self.metrics
                .sample(
                    "async_gate_decisions_total",
                    &[("pool_name", POOL), ("queue_id", ""), ("reason", reason)],
                )
                .unwrap_or(0.0)
        }
    }

    fn secs_from_now(secs: i64) -> i64 {
        now_millis() / 1000 + secs
    }

    fn finished(outcome: Outcome) -> crate::api::result::ResultMessage {
        match outcome {
            Outcome::Finish { result, .. } => result,
            other => panic!("want Finish, got {other:?}"),
        }
    }

    /// The request back in its queue, and when it comes due.
    fn retried(outcome: Outcome) -> (InternalRequest, i64) {
        match outcome {
            Outcome::Retry {
                envelope, due_ms, ..
            } => (envelope, due_ms),
            other => panic!("want Retry, got {other:?}"),
        }
    }

    async fn process(worker: &Worker, dispatch: Dispatch) {
        tokio::time::timeout(Duration::from_secs(10), worker.process(dispatch))
            .await
            .expect("the dispatch finished");
    }

    #[tokio::test]
    async fn a_success_finishes_with_the_inline_response() {
        let mut rig = Rig::embedded().await;
        let gw = Gateway::replying(
            StatusCode::OK,
            &[("content-type", "application/json")],
            r#"{"ok":true}"#,
        )
        .await;
        let worker = rig.worker(None);
        process(&worker, rig.dispatch("r", secs_from_now(60), &gw.url).await).await;
        let result = finished(rig.outcome());
        assert_eq!(
            (result.status_code, result.payload.as_str()),
            (200, r#"{"ok":true}"#)
        );
        assert_eq!(result.error_code, None);
        assert_eq!(gw.hits(), 1);
    }

    #[tokio::test]
    async fn a_server_error_retries_with_exponential_backoff() {
        let mut rig = Rig::embedded().await;
        let gw = Gateway::replying(StatusCode::SERVICE_UNAVAILABLE, &[], "busy").await;
        let worker = rig.worker(None);
        let before = now_millis();
        process(
            &worker,
            rig.dispatch("r", secs_from_now(600), &gw.url).await,
        )
        .await;
        let after = now_millis();
        let (envelope, due_ms) = retried(rig.outcome());
        assert_eq!(envelope.routing.retry_count, 1);
        assert!(
            (before + 2_000..=after + 4_000).contains(&due_ms),
            "first retry is due in [2s, 4s): {}",
            due_ms - before
        );
        assert_eq!(
            rig.metrics.sample(
                "async_request_retries_total",
                &[("queue_id", "q"), ("pool_name", POOL)]
            ),
            Some(1.0)
        );
    }

    #[tokio::test]
    async fn a_rate_limit_honors_a_longer_retry_after() {
        let mut rig = Rig::embedded().await;
        let gw = Gateway::replying(
            StatusCode::TOO_MANY_REQUESTS,
            &[("retry-after", "30")],
            "slow down",
        )
        .await;
        let worker = rig.worker(None);
        let before = now_millis();
        process(
            &worker,
            rig.dispatch("r", secs_from_now(600), &gw.url).await,
        )
        .await;
        let after = now_millis();
        let (_, due_ms) = retried(rig.outcome());
        assert!(
            (before + 30_000..=after + 37_500).contains(&due_ms),
            "Retry-After 30s plus under 25% jitter: {}",
            due_ms - before
        );
    }

    #[tokio::test]
    async fn a_client_error_finishes_with_the_response() {
        let mut rig = Rig::embedded().await;
        let gw = Gateway::replying(StatusCode::BAD_REQUEST, &[], "bad voice").await;
        let worker = rig.worker(None);
        process(&worker, rig.dispatch("r", secs_from_now(60), &gw.url).await).await;
        let result = finished(rig.outcome());
        assert_eq!(
            (result.status_code, result.payload.as_str()),
            (400, "bad voice")
        );
        assert_eq!(gw.hits(), 1, "not retried");
    }

    #[tokio::test]
    async fn a_retry_past_the_deadline_keeps_the_last_response() {
        let mut rig = Rig::embedded().await;
        let gw = Gateway::replying(StatusCode::SERVICE_UNAVAILABLE, &[], "busy").await;
        let worker = rig.worker(None);
        // Start 100-500ms into a second so the deadline, the next whole
        // second, is under a second away but not yet reached.
        while !(100..500).contains(&(now_millis() % 1000)) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        process(&worker, rig.dispatch("r", secs_from_now(1), &gw.url).await).await;
        let result = finished(rig.outcome());
        assert_eq!((result.status_code, result.payload.as_str()), (503, "busy"));
        assert_eq!(result.error_code, None);
    }

    #[tokio::test]
    async fn an_attempt_past_the_request_timeout_is_deadline_exceeded() {
        let mut rig = Rig::embedded().await;
        let gw = Gateway::hanging().await;
        let mut worker = rig.worker(None);
        worker.request_timeout = Duration::from_millis(200);
        process(
            &worker,
            rig.dispatch("r", secs_from_now(600), &gw.url).await,
        )
        .await;
        let result = finished(rig.outcome());
        assert_eq!(result.error_code, Some(ErrorCode::DeadlineExceeded));
        assert_eq!(result.status_code, 0);
        assert_eq!(gw.hits(), 1);
    }

    #[tokio::test]
    async fn a_cancelled_request_is_never_sent() {
        let mut rig = Rig::embedded().await;
        let gw = Gateway::replying(StatusCode::OK, &[], "{}").await;
        let worker = rig.worker(None);
        let dispatch = rig.dispatch("r", secs_from_now(60), &gw.url).await;
        rig.store
            .cancel(vec!["r".into()], now_millis())
            .await
            .unwrap();
        process(&worker, dispatch).await;
        let result = finished(rig.outcome());
        assert_eq!(result.error_code, Some(ErrorCode::Cancelled));
        assert_eq!(result.error_message, "cancelled");
        assert_eq!(gw.hits(), 0);
    }

    #[tokio::test]
    async fn a_pool_gate_refusal_requeues_at_once_and_sends_nothing() {
        let mut rig = Rig::embedded().await;
        let gw = Gateway::replying(StatusCode::OK, &[], "{}").await;
        let worker = rig.worker(Some(Arc::new(FixedGate::new(0.0, Verdict::Refuse))));
        let before = now_millis();
        process(&worker, rig.dispatch("r", secs_from_now(60), &gw.url).await).await;
        let (envelope, due_ms) = retried(rig.outcome());
        assert!((before..=now_millis()).contains(&due_ms), "no backoff");
        assert_eq!(envelope.routing.retry_count, 0, "a refusal is not a retry");
        assert_eq!(gw.hits(), 0);
        assert_eq!(rig.gate_decisions("gate_closed"), 1.0);
    }

    #[tokio::test]
    async fn a_pool_gate_drop_finishes_the_request() {
        let mut rig = Rig::embedded().await;
        let gw = Gateway::replying(StatusCode::OK, &[], "{}").await;
        let worker = rig.worker(Some(Arc::new(FixedGate::new(0.0, Verdict::Drop(None)))));
        process(&worker, rig.dispatch("r", secs_from_now(60), &gw.url).await).await;
        let result = finished(rig.outcome());
        assert_eq!(result.error_code, Some(ErrorCode::GateDropped));
        assert_eq!(gw.hits(), 0);
        assert_eq!(rig.gate_decisions("dropped"), 1.0);
    }

    #[tokio::test]
    async fn a_cancellation_is_seen_while_waiting_at_the_pool_gate() {
        let mut rig = Rig::embedded().await;
        let gw = Gateway::replying(StatusCode::OK, &[], "{}").await;
        let worker = rig.worker(Some(Arc::new(FixedGate::new(0.0, Verdict::Wait))));
        let dispatch = rig.dispatch("r", secs_from_now(60), &gw.url).await;
        let store = Arc::clone(&rig.store);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            store.cancel(vec!["r".into()], now_millis()).await.unwrap();
        });
        process(&worker, dispatch).await;
        assert_eq!(
            finished(rig.outcome()).error_code,
            Some(ErrorCode::Cancelled)
        );
        assert_eq!(gw.hits(), 0);
        assert_eq!(
            rig.gate_decisions("gate_closed"),
            1.0,
            "one wait, counted once"
        );
    }

    #[tokio::test]
    async fn the_deadline_passing_at_the_pool_gate_is_deadline_exceeded() {
        let mut rig = Rig::embedded().await;
        let gw = Gateway::replying(StatusCode::OK, &[], "{}").await;
        let worker = rig.worker(Some(Arc::new(FixedGate::new(0.0, Verdict::Wait))));
        process(&worker, rig.dispatch("r", secs_from_now(2), &gw.url).await).await;
        let result = finished(rig.outcome());
        assert_eq!(result.error_code, Some(ErrorCode::DeadlineExceeded));
        assert_eq!(result.error_message, "deadline exceeded");
        assert_eq!(gw.hits(), 0);
    }

    #[tokio::test]
    async fn the_gate_wait_timeout_requeues_without_a_result() {
        let mut rig = Rig::embedded().await;
        let gw = Gateway::replying(StatusCode::OK, &[], "{}").await;
        let mut worker = rig.worker(Some(Arc::new(FixedGate::new(0.0, Verdict::Wait))));
        worker.gate_wait_timeout = Duration::from_millis(300);
        let started = Instant::now();
        process(
            &worker,
            rig.dispatch("r", secs_from_now(600), &gw.url).await,
        )
        .await;
        assert!(started.elapsed() >= Duration::from_millis(300));
        let (envelope, due_ms) = retried(rig.outcome());
        assert!(due_ms <= now_millis(), "due at once");
        assert_eq!(envelope.routing.retry_count, 0);
        assert_eq!(gw.hits(), 0);
        assert_eq!(
            rig.metrics.sample(
                "async_gate_wait_requeues_total",
                &[("queue_id", "q"), ("pool_name", POOL)]
            ),
            Some(1.0)
        );
    }

    #[tokio::test]
    async fn draining_while_waiting_at_the_pool_gate_releases_the_claim() {
        let mut rig = Rig::embedded().await;
        let gw = Gateway::replying(StatusCode::OK, &[], "{}").await;
        let worker = rig.worker(Some(Arc::new(FixedGate::new(0.0, Verdict::Wait))));
        let drain = worker.drain.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            drain.cancel();
        });
        process(
            &worker,
            rig.dispatch("r", secs_from_now(600), &gw.url).await,
        )
        .await;
        assert!(matches!(rig.outcome(), Outcome::Release { .. }));
        assert_eq!(gw.hits(), 0);
    }

    #[tokio::test]
    async fn buffered_dispatches_are_released_when_consumption_stops() {
        let mut rig = Rig::embedded().await;
        let gw = Gateway::replying(StatusCode::OK, &[], "{}").await;
        let worker = Arc::new(rig.worker(None));
        let (tx, rx) = mpsc::channel(8);
        for i in 0..3 {
            let d = rig
                .dispatch(&format!("r{i}"), secs_from_now(60), &gw.url)
                .await;
            tx.send(d).await.unwrap();
        }
        drop(tx);
        worker.consume.cancel();
        tokio::time::timeout(
            Duration::from_secs(5),
            Arc::clone(&worker).run(Arc::new(tokio::sync::Mutex::new(rx))),
        )
        .await
        .unwrap();
        for _ in 0..3 {
            assert!(matches!(
                rig.outcomes.try_recv(),
                Ok(Outcome::Release { .. })
            ));
        }
        assert!(rig.outcomes.try_recv().is_err());
        assert_eq!(gw.hits(), 0);
    }

    #[tokio::test]
    async fn a_pool_gate_error_finishes_with_gate_error() {
        let Some(pg) = crate::store::postgres::test_support::fixture().await else {
            return;
        };
        let mut rig = Rig::embedded().await;
        let gw = Gateway::replying(StatusCode::OK, &[], "{}").await;
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
        let worker = rig.worker(Some(Arc::new(quota)));
        let mut dispatch = rig.dispatch("r", secs_from_now(60), &gw.url).await;
        dispatch
            .claimed
            .envelope
            .request
            .metadata
            .insert("userid".into(), "t".into());
        process(&worker, dispatch).await;
        let result = finished(rig.outcome());
        assert_eq!(result.error_code, Some(ErrorCode::GateError));
        assert!(
            result.error_message.starts_with("Pool gating error: "),
            "{}",
            result.error_message
        );
        assert_eq!(gw.hits(), 0);
        assert_eq!(rig.gate_decisions("error"), 1.0);
    }

    #[tokio::test]
    async fn a_failed_cancellation_check_retries_after_a_second() {
        let Some(pg) = crate::store::postgres::test_support::fixture().await else {
            return;
        };
        let gw = Gateway::replying(StatusCode::OK, &[], "{}").await;
        let store: Store = pg.pg.clone();
        let blobs = crate::store::postgres::test_support::chunk_blobs(&pg.db);
        pg.db.pool.close();
        let mut rig = Rig::new(store, blobs, Box::new(pg));
        let worker = rig.worker(None);
        let claim = ClaimRef {
            generation: "g".into(),
            claim_id: 1,
        };
        let dispatch = rig.dispatch_of(
            envelope("r", "t", "q", secs_from_now(60)),
            claim,
            None,
            &gw.url,
        );
        let before = now_millis();
        process(&worker, dispatch).await;
        let (_, due_ms) = retried(rig.outcome());
        assert!(
            (before + 1_000..=now_millis() + 1_000).contains(&due_ms),
            "{}",
            due_ms - before
        );
        assert_eq!(gw.hits(), 0);
    }
}
