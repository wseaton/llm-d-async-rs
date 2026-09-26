//! OpenAI-compatible `/v1/chat/completions` and `/v1/completions`. Each call
//! is submitted to a queue, the connection is held until its result, and the
//! result is the answer, so an unmodified client gets queueing, priority,
//! deadlines and resumption. A `stream: true` call is submitted without
//! streaming; it gets keepalive comments while it waits and then the whole
//! response as events. A caller that goes away cancels its request, which
//! only works before it is dispatched.
//!
//! Request headers:
//! - `x-llm-d-async-queue`: the queue (default: the first configured one).
//! - `x-llm-d-async-timeout`: the deadline, as a duration from now in Go
//!   syntax (default `--facade-timeout`).
//! - `x-llm-d-inference-objective` and `x-llm-d-inference-fairness-id` go on
//!   to the gateway with the request.

pub mod events;

use std::collections::BTreeMap;
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::value::RawValue;
use serde_json::{Map, Value, json};
use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::api::headers;
use crate::api::request::{ResultDelivery, SubmitRequest};
use crate::api::result::{ErrorCode, ResultMessage};
use crate::clock::now_millis;
use crate::config::duration::parse_duration;
use crate::server::AppState;
use crate::server::error::ApiError;
use crate::server::requests::submit_json;
use crate::server::results::long_poll;
use crate::store::error::StoreError;
use crate::worker::vllm::Api;

pub const QUEUE_HEADER: &str = "x-llm-d-async-queue";
pub const TIMEOUT_HEADER: &str = "x-llm-d-async-timeout";
const FORWARDED: [&str; 2] = [headers::OBJECTIVE, headers::FAIRNESS_ID];
const KEEPALIVE: Duration = Duration::from_secs(10);
const LEASE_MS: i64 = 60_000;
/// How long past the deadline to wait for the result the processor writes
/// when the deadline passes.
const GRACE: Duration = Duration::from_secs(30);
const POLL: Duration = Duration::from_secs(60);

pub async fn chat(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    serve(state, Api::Chat, headers, body).await
}

pub async fn completions(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    serve(state, Api::Completions, headers, body).await
}

fn path(api: Api) -> &'static str {
    match api {
        Api::Chat => "/v1/chat/completions",
        Api::Completions => "/v1/completions",
    }
}

/// vLLM's `ErrorResponse`.
fn error(status: StatusCode, kind: &str, message: impl Into<String>) -> Value {
    json!({"error": {"message": message.into(), "type": kind, "param": null,
                     "code": status.as_u16()}})
}

fn error_response(status: StatusCode, kind: &str, message: impl Into<String>) -> Response {
    (status, axum::Json(error(status, kind, message))).into_response()
}

#[derive(Deserialize)]
struct Caller {
    #[serde(default)]
    stream: Option<bool>,
    #[serde(default)]
    stream_options: Option<StreamOptions>,
    #[serde(default)]
    model: Option<String>,
}

#[derive(Deserialize)]
struct StreamOptions {
    #[serde(default)]
    include_usage: Option<bool>,
}

/// How a `stream: true` caller wants its events.
#[derive(Debug, Clone, Copy)]
struct Streaming {
    include_usage: bool,
}

/// The submission for the caller's `body`, and how to stream the answer if
/// the caller asked for a stream.
fn prepare(
    api: Api,
    headers: &HeaderMap,
    body: &[u8],
    default_timeout: Duration,
) -> Result<(SubmitRequest, Option<Streaming>), Box<Response>> {
    let bad = |message: String| {
        Box::new(error_response(
            StatusCode::BAD_REQUEST,
            "BadRequestError",
            message,
        ))
    };
    let caller: Caller =
        serde_json::from_slice(body).map_err(|e| bad(format!("invalid JSON body: {e}")))?;
    let header = |name: &str| {
        headers
            .get(name)
            .map(|v| v.to_str().map(str::to_owned))
            .transpose()
            .map_err(|_| bad(format!("{name} is not visible ASCII")))
    };
    let timeout = match header(TIMEOUT_HEADER)? {
        Some(value) => parse_duration(&value)
            .ok()
            .filter(|d| !d.is_zero())
            .ok_or_else(|| {
                bad(format!(
                    "{TIMEOUT_HEADER}: {value:?} is not a positive duration"
                ))
            })?,
        None => default_timeout,
    };
    let mut forwarded = BTreeMap::new();
    for name in FORWARDED {
        if let Some(value) = header(name)? {
            forwarded.insert(name.to_owned(), value);
        }
    }
    let streaming = caller.stream.unwrap_or(false).then(|| Streaming {
        include_usage: caller
            .stream_options
            .and_then(|o| o.include_usage)
            .unwrap_or(false),
    });
    let payload: Box<RawValue> = if streaming.is_some() {
        let mut fields: Map<String, Value> =
            serde_json::from_slice(body).map_err(|e| bad(format!("invalid JSON body: {e}")))?;
        fields.insert("stream".into(), false.into());
        fields.remove("stream_options");
        serde_json::value::to_raw_value(&fields).map_err(|e| bad(e.to_string()))?
    } else {
        serde_json::from_slice(body).map_err(|e| bad(format!("invalid JSON body: {e}")))?
    };
    let deadline_ms =
        now_millis().saturating_add(i64::try_from(timeout.as_millis()).unwrap_or(i64::MAX));
    let submission = SubmitRequest {
        id: format!("facade-{:032x}", rand::random::<u128>()),
        created: now_millis() / 1000,
        deadline: deadline_ms.saturating_add(999) / 1000,
        payload: Some(payload),
        metadata: BTreeMap::new(),
        headers: forwarded,
        endpoint: path(api).to_owned(),
        model: caller.model.unwrap_or_default(),
        request_queue_name: header(QUEUE_HEADER)?.unwrap_or_default(),
        result_queue_name: String::new(),
        result_delivery: ResultDelivery::Request,
    };
    Ok((submission, streaming))
}

/// Cancels the request unless its result was delivered.
struct Pending {
    state: AppState,
    id: String,
    delivered: bool,
}

impl Pending {
    fn delivered(&mut self) {
        self.delivered = true;
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        if self.delivered {
            return;
        }
        let store = self.state.store.clone();
        let id = std::mem::take(&mut self.id);
        tracing::info!(%id, "the caller went away; cancelling its request");
        tokio::spawn(async move {
            if let Err(e) = store.cancel(vec![id.clone()], now_millis()).await {
                tracing::warn!(%id, error = %e, "cancelling an abandoned request failed");
            }
        });
    }
}

/// Claims the result on `route`, waiting until `until`.
async fn wait(
    state: &AppState,
    route: &str,
    until: Instant,
) -> Result<Option<ResultMessage>, StoreError> {
    let owner = format!("{:032x}", rand::random::<u128>());
    loop {
        let left = until.saturating_duration_since(Instant::now());
        let claimed = long_poll(state, route, left.min(POLL), || {
            state
                .store
                .claim_result(route.to_owned(), owner.clone(), LEASE_MS, now_millis())
        })
        .await?;
        if let Some(claim) = claimed {
            let result = serde_json::from_str(&claim.body)?;
            if let Err(e) = state
                .store
                .ack_result(route.to_owned(), claim.claim_id, owner, now_millis())
                .await
            {
                tracing::warn!(%route, error = %e, "acknowledging a delivered result failed");
            }
            return Ok(Some(result));
        }
        if Instant::now() >= until {
            return Ok(None);
        }
    }
}

/// The HTTP status and body a result answers with.
fn answer(result: &ResultMessage) -> (StatusCode, Value) {
    if result.status_code > 0 {
        let status = StatusCode::from_u16(result.status_code).unwrap_or(StatusCode::BAD_GATEWAY);
        if !result.payload_ref.is_empty() {
            return (
                StatusCode::BAD_GATEWAY,
                error(
                    StatusCode::BAD_GATEWAY,
                    "InternalServerError",
                    "the response is not JSON",
                ),
            );
        }
        return match serde_json::from_str(&result.payload) {
            Ok(body) => (status, body),
            Err(_) => (
                StatusCode::BAD_GATEWAY,
                error(
                    StatusCode::BAD_GATEWAY,
                    "InternalServerError",
                    "the response is not JSON",
                ),
            ),
        };
    }
    let (status, kind) = match result.error_code {
        Some(ErrorCode::DeadlineExceeded) => (StatusCode::GATEWAY_TIMEOUT, "TimeoutError"),
        Some(ErrorCode::GateDropped) => (StatusCode::TOO_MANY_REQUESTS, "RateLimitError"),
        Some(ErrorCode::Cancelled) => (StatusCode::CONFLICT, "CancelledError"),
        Some(ErrorCode::GateError) => (StatusCode::SERVICE_UNAVAILABLE, "ServiceUnavailableError"),
        Some(ErrorCode::InvalidRequest) => (StatusCode::BAD_REQUEST, "BadRequestError"),
        Some(ErrorCode::InferenceError) | None => (StatusCode::BAD_GATEWAY, "InternalServerError"),
        Some(ErrorCode::PayloadUnavailable) => {
            (StatusCode::INTERNAL_SERVER_ERROR, "InternalServerError")
        }
    };
    (status, error(status, kind, result.error_message.clone()))
}

fn timed_out() -> (StatusCode, Value) {
    (
        StatusCode::GATEWAY_TIMEOUT,
        error(
            StatusCode::GATEWAY_TIMEOUT,
            "TimeoutError",
            "no result before the deadline",
        ),
    )
}

fn submit_failed(e: ApiError) -> Response {
    let status = match e {
        ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
        ApiError::TooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };
    error_response(status, "BadRequestError", e.to_string())
}

async fn serve(state: AppState, api: Api, headers: HeaderMap, body: Bytes) -> Response {
    let (submission, streaming) = match prepare(api, &headers, &body, state.facade_timeout) {
        Ok(prepared) => prepared,
        Err(response) => return *response,
    };
    let until = Instant::now()
        + Duration::from_millis(
            u64::try_from(
                submission
                    .deadline
                    .saturating_mul(1000)
                    .saturating_sub(now_millis()),
            )
            .unwrap_or(0),
        )
        + GRACE;
    let id = submission.id.clone();
    let submitted = match submit_json(&state, submission).await {
        Ok(submitted) => submitted,
        Err(e) => return submit_failed(e),
    };
    let Some(route) = submitted.result_route else {
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalServerError",
            "the submission has no result route",
        );
    };
    let pending = Pending {
        state: state.clone(),
        id,
        delivered: false,
    };
    match streaming {
        None => respond(pending, &route, until).await,
        Some(streaming) => stream(pending, api, route, until, streaming),
    }
}

async fn respond(mut pending: Pending, route: &str, until: Instant) -> Response {
    let (status, body) = match wait(&pending.state, route, until).await {
        Ok(Some(result)) => {
            pending.delivered();
            answer(&result)
        }
        Ok(None) => timed_out(),
        Err(e) => {
            tracing::error!(%route, error = %e, "waiting for a result failed");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "ServiceUnavailableError",
                    e.to_string(),
                ),
            )
        }
    };
    (status, axum::Json(body)).into_response()
}

fn stream(
    mut pending: Pending,
    api: Api,
    route: String,
    until: Instant,
    streaming: Streaming,
) -> Response {
    let (tx, rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(8);
    tokio::spawn(async move {
        let state = pending.state.clone();
        let waited = wait(&state, &route, until);
        tokio::pin!(waited);
        let mut keepalive = tokio::time::interval_at(Instant::now() + KEEPALIVE, KEEPALIVE);
        let waited = loop {
            tokio::select! {
                waited = &mut waited => break waited,
                () = tx.closed() => return,
                _ = keepalive.tick() => {
                    if tx.send(Ok(Bytes::from_static(b": keepalive\n\n"))).await.is_err() {
                        return;
                    }
                }
            }
        };
        let (status, body) = match waited {
            Ok(Some(result)) => {
                pending.delivered();
                answer(&result)
            }
            Ok(None) => timed_out(),
            Err(e) => (
                StatusCode::SERVICE_UNAVAILABLE,
                error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "ServiceUnavailableError",
                    e.to_string(),
                ),
            ),
        };
        let chunks = status
            .is_success()
            .then(|| events::chunks(api, &body, streaming.include_usage))
            .flatten();
        let mut out = String::new();
        match chunks {
            Some(chunks) => chunks.iter().for_each(|c| out += &events::event(c)),
            None if status.is_success() => {
                let e = error(
                    StatusCode::BAD_GATEWAY,
                    "InternalServerError",
                    "unusable response",
                );
                out += &events::event(&e);
            }
            None => out += &events::event(&body),
        }
        out += events::DONE;
        let _ = tx.send(Ok(Bytes::from(out))).await;
    });
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::from_stream(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        ))
        .unwrap_or_else(|e| {
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalServerError",
                e.to_string(),
            )
        })
}
