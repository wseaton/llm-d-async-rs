use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;

use crate::api::dispatch_rate::DispatchRateLimit;
use crate::clock::now_millis;
use crate::server::AppState;
use crate::server::error::ApiError;

/// Sets the budget a `budget-key` gate reads. Values are clamped to [0, 1]
/// when read.
pub async fn put_budget(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(budget): Json<f64>,
) -> Result<StatusCode, ApiError> {
    if !budget.is_finite() {
        return Err(ApiError::BadRequest(
            "budget must be a finite number".into(),
        ));
    }
    state
        .store
        .set_budget(&key, Some(budget.to_string().into_bytes()))
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn get_budget(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let raw = state
        .store
        .budget(&key)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("no budget {key:?}")))?;
    Ok(Json(
        serde_json::json!({ "budget": String::from_utf8_lossy(&raw) }),
    ))
}

pub async fn delete_budget(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<StatusCode, ApiError> {
    state.store.set_budget(&key, None).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Stores a leased dispatch-rate command for `leased-rate` gates. Invalid or
/// already-expired commands are rejected.
pub async fn put_dispatch_rate(
    State(state): State<AppState>,
    Path(key): Path<String>,
    Json(limit): Json<DispatchRateLimit>,
) -> Result<StatusCode, ApiError> {
    limit
        .validate_at(now_millis())
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;
    state.store.set_dispatch_rate(&key, Some(&limit)).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn get_dispatch_rate(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<Json<DispatchRateLimit>, ApiError> {
    match state.store.dispatch_rate(&key).await?.value {
        Some(Ok(limit)) => Ok(Json(limit)),
        Some(Err(e)) => Err(ApiError::Internal(format!(
            "stored command is corrupt: {e}"
        ))),
        None => Err(ApiError::NotFound(format!("no dispatch rate {key:?}"))),
    }
}

pub async fn delete_dispatch_rate(
    State(state): State<AppState>,
    Path(key): Path<String>,
) -> Result<StatusCode, ApiError> {
    state.store.set_dispatch_rate(&key, None).await?;
    Ok(StatusCode::NO_CONTENT)
}
