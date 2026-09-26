use std::future::Future;
use std::time::Duration;

use axum::Json;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use tokio::time::Instant;

use crate::clock::now_millis;
use crate::server::AppState;
use crate::server::error::ApiError;
use crate::store::blob::key::BlobKey;
use crate::store::error::StoreError;
use crate::store::queue::AckOutcome;

const MAX_WAIT: Duration = Duration::from_secs(60);
/// Also re-check this often while long-polling: a lapsed result lease makes
/// a result claimable without any write to signal it.
const RECHECK: Duration = Duration::from_secs(1);
const DEFAULT_LEASE_MS: u64 = 5 * 60 * 1000;

#[derive(Deserialize)]
pub struct WaitQuery {
    #[serde(default)]
    wait_ms: u64,
    #[serde(default)]
    lease_ms: Option<u64>,
}

/// Retries `attempt` until it finds something or `wait` elapses, waking on
/// every result write to `route`.
async fn long_poll<T, F, Fut>(
    state: &AppState,
    route: &str,
    wait: Duration,
    mut attempt: F,
) -> Result<Option<T>, StoreError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<Option<T>, StoreError>>,
{
    let deadline = Instant::now() + wait.min(MAX_WAIT);
    let watch = state.results.watch(route);
    loop {
        let written = watch.notify().notified();
        tokio::pin!(written);
        written.as_mut().enable();
        if let Some(found) = attempt().await? {
            return Ok(Some(found));
        }
        let now = Instant::now();
        if now >= deadline {
            return Ok(None);
        }
        tokio::select! {
            () = &mut written => {}
            () = tokio::time::sleep_until(deadline.min(now + RECHECK)) => {}
        }
    }
}

fn raw_json(body: String) -> Result<Box<RawValue>, ApiError> {
    RawValue::from_string(body)
        .map_err(|e| ApiError::Internal(format!("stored result is not JSON: {e}")))
}

/// Destructively takes the oldest result. 204 when none arrived in time.
pub async fn pop(
    State(state): State<AppState>,
    Path(route): Path<String>,
    Query(q): Query<WaitQuery>,
) -> Result<Response, ApiError> {
    let found = long_poll(&state, &route, Duration::from_millis(q.wait_ms), || {
        state.store.pop_result(route.clone(), now_millis())
    })
    .await?;
    Ok(match found {
        Some(body) => Json(raw_json(body)?).into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    })
}

#[derive(Serialize)]
pub struct Claim {
    claim_id: u64,
    owner_token: String,
    lease_ms: u64,
    result: Box<RawValue>,
}

fn lease_ms(requested: Option<u64>) -> Result<i64, ApiError> {
    let ms = requested.unwrap_or(DEFAULT_LEASE_MS);
    if ms == 0 {
        return Err(ApiError::BadRequest("lease_ms must be positive".into()));
    }
    i64::try_from(ms).map_err(|_| ApiError::BadRequest("lease_ms is too large".into()))
}

/// Leases the oldest result. It stays stored until acknowledged and is
/// redelivered if the lease lapses first.
pub async fn claim(
    State(state): State<AppState>,
    Path(route): Path<String>,
    Query(q): Query<WaitQuery>,
) -> Result<Response, ApiError> {
    let lease = lease_ms(q.lease_ms)?;
    let owner = format!("{:032x}", rand::random::<u128>());
    let found = long_poll(&state, &route, Duration::from_millis(q.wait_ms), || {
        state
            .store
            .claim_result(route.clone(), owner.clone(), lease, now_millis())
    })
    .await?;
    let Some(claim) = found else {
        return Ok(StatusCode::NO_CONTENT.into_response());
    };
    Ok(Json(Claim {
        claim_id: claim.claim_id,
        owner_token: owner,
        lease_ms: lease.unsigned_abs(),
        result: raw_json(claim.body)?,
    })
    .into_response())
}

#[derive(Deserialize)]
pub struct OwnerBody {
    owner_token: String,
    #[serde(default)]
    lease_ms: Option<u64>,
}

pub async fn renew(
    State(state): State<AppState>,
    Path((route, claim_id)): Path<(String, u64)>,
    Json(body): Json<OwnerBody>,
) -> Result<StatusCode, ApiError> {
    let lease = lease_ms(body.lease_ms)?;
    if state
        .store
        .renew_result(route, claim_id, body.owner_token, lease, now_millis())
        .await?
    {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::Conflict("result delivery ownership lost".into()))
    }
}

pub async fn ack(
    State(state): State<AppState>,
    Path((route, claim_id)): Path<(String, u64)>,
    Json(body): Json<OwnerBody>,
) -> Result<StatusCode, ApiError> {
    match state
        .store
        .ack_result(route, claim_id, body.owner_token, now_millis())
        .await?
    {
        AckOutcome::Acked | AckOutcome::AlreadyAcked => Ok(StatusCode::NO_CONTENT),
        AckOutcome::OwnershipLost => {
            Err(ApiError::Conflict("result delivery ownership lost".into()))
        }
    }
}

pub async fn depth(
    State(state): State<AppState>,
    Path(route): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let depth = state.store.result_depth(route, now_millis()).await?;
    Ok(Json(serde_json::json!({ "depth": depth })))
}

/// Streams a result body stored by reference.
pub async fn blob(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Response, ApiError> {
    let key = BlobKey::parse_result_name(&name)
        .ok_or_else(|| ApiError::NotFound("no such result body".into()))?;
    let (body, content_type) = state
        .store
        .open_result_blob(key, now_millis())
        .await?
        .ok_or_else(|| ApiError::NotFound("no such result body".into()))?;
    let content_type = if content_type.is_empty() {
        "application/octet-stream".to_owned()
    } else {
        content_type
    };
    Response::builder()
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONTENT_LENGTH, body.size)
        .body(Body::from_stream(body.stream))
        .map_err(|e| ApiError::Internal(e.to_string()))
}
