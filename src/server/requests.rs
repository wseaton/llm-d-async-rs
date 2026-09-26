use std::collections::BTreeMap;

use axum::Json;
use axum::extract::{FromRequest, Multipart, Path, Query, Request, State};
use axum::http::{StatusCode, header};
use serde::{Deserialize, Serialize};

use crate::api::payload::JSON_CONTENT_TYPE;
use crate::api::request::{InternalRequest, ResultDelivery, SubmitRequest};
use crate::api::routing::{
    InternalRouting, RESERVED_ROUTE_PREFIX, is_request_route, request_route,
};
use crate::clock::now_millis;
use crate::dispatch::queues::labels_of;
use crate::server::AppState;
use crate::server::error::ApiError;
use crate::store::blob::BlobStore;
use crate::store::blob::key::BlobKey;
use crate::store::queue::{NewRequest, RequestStatus};
use crate::store::staging::{PayloadSink, StagedPayload};

/// Largest `request` part of a multipart submission.
const MAX_ENVELOPE_BYTES: usize = 1 << 20;
const MAX_BATCH: usize = 1000;
const DEFAULT_PAYLOAD_CONTENT_TYPE: &str = "application/octet-stream";

#[derive(Debug, Serialize, Deserialize)]
pub struct Submitted {
    pub id: String,
    pub request_token: String,
    /// The route to claim the result from, for a result delivered by request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_route: Option<String>,
}

fn new_token() -> String {
    format!("{:032x}", rand::random::<u128>())
}

fn bad(e: impl std::fmt::Display) -> ApiError {
    ApiError::BadRequest(e.to_string())
}

/// Blobs staged for submissions that have not committed yet. Dropped before
/// [`StagedBlobs::committed`], it deletes them, so a failed or abandoned
/// submission (a client that disconnects mid-request) leaves nothing behind.
/// An upload cut off before it finished discards itself.
struct StagedBlobs {
    blobs: BlobStore,
    keys: Vec<BlobKey>,
}

impl StagedBlobs {
    fn new(state: &AppState) -> Self {
        Self {
            blobs: state.store.blobs().clone(),
            keys: Vec::new(),
        }
    }

    fn track(&mut self, payload: &StagedPayload) {
        if let StagedPayload::Blob { key, .. } = payload {
            self.keys.push(key.clone());
        }
    }

    fn committed(mut self) {
        self.keys.clear();
    }
}

impl Drop for StagedBlobs {
    fn drop(&mut self) {
        self.blobs.spawn_remove(std::mem::take(&mut self.keys));
    }
}

fn sink(state: &AppState, token: &str) -> PayloadSink {
    PayloadSink::new(
        state.store.blobs().clone(),
        token,
        state.limits.inline,
        state.limits.max,
    )
}

/// Validates a submission and resolves its routing.
async fn envelope(
    state: &AppState,
    sub: &SubmitRequest,
    token: &str,
) -> Result<InternalRequest, ApiError> {
    if sub.id.is_empty() {
        return Err(bad("request ID is required"));
    }
    if sub.id.contains('\0') {
        return Err(bad("request ID must not contain NUL"));
    }
    if sub.deadline <= 0 {
        return Err(bad(
            "deadline is required and must be a positive Unix timestamp",
        ));
    }
    if sub.message().expired_at(now_millis()) {
        return Err(bad("deadline has already expired"));
    }
    let queues = state.queues.snapshot().await;
    let queue = if sub.request_queue_name.is_empty() {
        queues
            .first()
            .map(|q| q.queue_name.clone())
            .ok_or_else(|| bad("no queues are configured"))?
    } else if queues
        .iter()
        .any(|q| q.queue_name == sub.request_queue_name)
    {
        sub.request_queue_name.clone()
    } else {
        return Err(bad(format!(
            "unknown request_queue_name {:?}",
            sub.request_queue_name
        )));
    };
    if sub.result_queue_name.starts_with(RESERVED_ROUTE_PREFIX) {
        return Err(bad(format!(
            "result_queue_name must not start with {RESERVED_ROUTE_PREFIX:?}"
        )));
    }
    let mut routing = InternalRouting {
        request_token: token.to_owned(),
        request_queue_name: queue.clone(),
        result_queue_name: if sub.result_queue_name.is_empty() {
            state.default_result_queue.clone()
        } else {
            sub.result_queue_name.clone()
        },
        ..Default::default()
    };
    if sub.result_delivery == ResultDelivery::Request {
        let queue_ttl = queues
            .iter()
            .find(|q| q.queue_name == queue)
            .map_or(0, |q| q.result_ttl_seconds);
        routing.result_queue_name = request_route(&sub.id);
        routing.result_ttl_seconds = if queue_ttl > 0 {
            queue_ttl
        } else {
            state.request_result_ttl.as_secs().max(1)
        };
        routing.result_routing_resolved = true;
    }
    Ok(InternalRequest {
        routing,
        request: sub.message(),
        payload: StagedPayload::Inline(Vec::new()).info(JSON_CONTENT_TYPE),
        progress: None,
    })
}

/// A JSON submission: the payload is the inline `payload` value, sent
/// upstream as `application/json`. An absent payload is JSON `null`.
async fn from_json(
    state: &AppState,
    sub: SubmitRequest,
    staged: &mut StagedBlobs,
) -> Result<NewRequest, ApiError> {
    let token = new_token();
    let mut envelope = envelope(state, &sub, &token).await?;
    let raw = sub.payload.as_ref().map_or("null", |p| p.get());
    let mut sink = sink(state, &token);
    sink.push(raw.as_bytes()).await?;
    let payload = sink.finish().await?;
    staged.track(&payload);
    envelope.payload = payload.info(JSON_CONTENT_TYPE);
    Ok(NewRequest { envelope, payload })
}

/// A multipart submission: a `request` part holding the JSON envelope and a
/// `payload` part streamed to storage as opaque bytes of any content type.
async fn from_multipart(
    state: &AppState,
    mut multipart: Multipart,
    staged: &mut StagedBlobs,
) -> Result<NewRequest, ApiError> {
    let token = new_token();
    let mut sub: Option<SubmitRequest> = None;
    let mut payload: Option<(StagedPayload, String)> = None;
    while let Some(mut field) = multipart.next_field().await.map_err(bad)? {
        match field.name() {
            Some("request") => {
                if sub.is_some() {
                    return Err(bad("duplicate request part"));
                }
                let mut bytes = Vec::new();
                while let Some(chunk) = field.chunk().await.map_err(bad)? {
                    bytes.extend_from_slice(&chunk);
                    if bytes.len() > MAX_ENVELOPE_BYTES {
                        return Err(ApiError::TooLarge("request part is too large".into()));
                    }
                }
                sub = Some(serde_json::from_slice(&bytes).map_err(bad)?);
            }
            Some("payload") => {
                if payload.is_some() {
                    return Err(bad("duplicate payload part"));
                }
                let content_type = field
                    .content_type()
                    .unwrap_or(DEFAULT_PAYLOAD_CONTENT_TYPE)
                    .to_owned();
                let mut sink = sink(state, &token);
                while let Some(chunk) = field.chunk().await.map_err(bad)? {
                    sink.push(&chunk).await?;
                }
                let staged_payload = sink.finish().await?;
                staged.track(&staged_payload);
                payload = Some((staged_payload, content_type));
            }
            other => return Err(bad(format!("unexpected part {other:?}"))),
        }
    }
    let sub = sub.ok_or_else(|| bad("missing request part"))?;
    if sub.payload.is_some() {
        return Err(bad("payload must not be inline in a multipart submission"));
    }
    let (payload, content_type) = payload.ok_or_else(|| bad("missing payload part"))?;
    let mut envelope = envelope(state, &sub, &token).await?;
    envelope.payload = payload.info(&content_type);
    Ok(NewRequest { envelope, payload })
}

fn submitted(r: &NewRequest) -> Submitted {
    let route = &r.envelope.routing.result_queue_name;
    Submitted {
        id: r.envelope.request.id.clone(),
        request_token: r.envelope.routing.request_token.clone(),
        result_route: is_request_route(route).then(|| route.clone()),
    }
}

/// Commits submissions in a task of its own: a client that disconnects now
/// cannot separate the commit from the fate of its staged blobs. A failed
/// commit may still have landed (a lost reply), so its blobs are left for
/// orphan collection, which deletes them only if nothing references them.
async fn commit(
    state: &AppState,
    requests: Vec<NewRequest>,
    staged: StagedBlobs,
) -> Result<(), ApiError> {
    let store = state.store.clone();
    tokio::spawn(async move {
        let submitted = store.submit(requests).await;
        staged.committed();
        submitted.map_err(ApiError::from)
    })
    .await
    .map_err(|e| ApiError::Internal(e.to_string()))?
}

pub async fn submit(
    State(state): State<AppState>,
    request: Request,
) -> Result<(StatusCode, Json<Submitted>), ApiError> {
    let multipart = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.to_ascii_lowercase().starts_with("multipart/form-data"));
    let mut staged = StagedBlobs::new(&state);
    let new = if multipart {
        let multipart = Multipart::from_request(request, &state)
            .await
            .map_err(bad)?;
        from_multipart(&state, multipart, &mut staged).await?
    } else {
        let body = axum::body::to_bytes(request.into_body(), state.limits.json_body)
            .await
            .map_err(|e| ApiError::TooLarge(e.to_string()))?;
        let sub: SubmitRequest = serde_json::from_slice(&body).map_err(bad)?;
        from_json(&state, sub, &mut staged).await?
    };
    let response = submitted(&new);
    commit(&state, vec![new], staged).await?;
    Ok((StatusCode::ACCEPTED, Json(response)))
}

/// Submits many JSON requests in one transaction: all or none.
pub async fn submit_batch(
    State(state): State<AppState>,
    request: Request,
) -> Result<(StatusCode, Json<Vec<Submitted>>), ApiError> {
    let body = axum::body::to_bytes(request.into_body(), state.limits.json_body)
        .await
        .map_err(|e| ApiError::TooLarge(e.to_string()))?;
    let subs: Vec<SubmitRequest> = serde_json::from_slice(&body).map_err(bad)?;
    if subs.len() > MAX_BATCH {
        return Err(bad(format!("at most {MAX_BATCH} requests per batch")));
    }
    let mut staged = StagedBlobs::new(&state);
    let mut prepared = Vec::with_capacity(subs.len());
    for sub in subs {
        prepared.push(from_json(&state, sub, &mut staged).await?);
    }
    let response = prepared.iter().map(submitted).collect();
    commit(&state, prepared, staged).await?;
    Ok((StatusCode::ACCEPTED, Json(response)))
}

#[derive(Deserialize)]
pub struct CancelBody {
    ids: Vec<String>,
}

pub async fn cancel(
    State(state): State<AppState>,
    Json(body): Json<CancelBody>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let marked = state.store.cancel(body.ids, now_millis()).await?;
    Ok(Json(serde_json::json!({ "cancelled": marked })))
}

#[derive(Deserialize)]
pub struct TokenQuery {
    #[serde(default)]
    request_token: Option<String>,
}

#[derive(Serialize)]
pub struct Status {
    id: String,
    status: RequestStatus,
}

/// Where request `id` stands: its submission `request_token`, or without one
/// its live submission. Once it is done, its result waits on its result
/// route.
pub async fn status(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<TokenQuery>,
) -> Result<Json<Status>, ApiError> {
    let status = state
        .store
        .request_status(id.clone(), q.request_token)
        .await?;
    Ok(Json(Status { id, status }))
}

#[derive(Serialize)]
pub struct QueueStatus {
    id: String,
    queue_name: String,
    worker_pool_id: String,
    depth: u64,
    labels: BTreeMap<String, String>,
}

pub async fn list_queues(
    State(state): State<AppState>,
) -> Result<Json<Vec<QueueStatus>>, ApiError> {
    let mut out = Vec::new();
    for config in state.queues.snapshot().await {
        let backlog = state
            .store
            .backlog(config.queue_name.clone(), now_millis(), Vec::new())
            .await?;
        let labels = labels_of(&config);
        out.push(QueueStatus {
            id: labels.queue_id,
            queue_name: labels.queue_name,
            worker_pool_id: labels.pool,
            depth: backlog.depth,
            labels: config.labels,
        });
    }
    Ok(Json(out))
}
