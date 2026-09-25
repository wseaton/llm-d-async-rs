use std::sync::atomic::Ordering;
use std::time::Duration;

use axum::Json;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;

use crate::server::AppState;

const CHECK_TIMEOUT: Duration = Duration::from_secs(3);

pub async fn healthz() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok" }))
}

pub async fn readyz(State(state): State<AppState>) -> impl IntoResponse {
    if !state.ready.load(Ordering::Relaxed) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "status": "not ready" })),
        );
    }
    let error = match tokio::time::timeout(CHECK_TIMEOUT, state.store.ping()).await {
        Ok(Ok(())) => None,
        Ok(Err(e)) => Some(e.to_string()),
        Err(_) => Some("store check timed out".to_owned()),
    };
    match error {
        None => (
            StatusCode::OK,
            Json(serde_json::json!({ "status": "ready" })),
        ),
        Some(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "status": "not ready", "error": error })),
        ),
    }
}

pub async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    match state.metrics.render() {
        Ok(text) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
            text,
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            [(header::CONTENT_TYPE, "text/plain")],
            e.to_string(),
        ),
    }
}
