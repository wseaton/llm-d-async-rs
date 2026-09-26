//! HTTP servers: the producer/consumer API, health probes, and metrics.

pub mod admin;
pub mod error;
pub mod health;
pub mod requests;
pub mod results;

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post, put};

use crate::dispatch::queues::Queues;
use crate::store::Store;
use crate::store::signal::ResultSignal;
use crate::telemetry::metrics::Metrics;

#[derive(Debug, Clone, Copy)]
pub struct PayloadLimits {
    /// Bodies at or under this stay in the database; larger ones go to blobs.
    pub inline: usize,
    /// Largest accepted payload.
    pub max: u64,
    /// Largest JSON submission body. JSON bodies are buffered whole; large
    /// payloads belong in streamed multipart submissions.
    pub json_body: usize,
}

#[derive(Clone)]
pub struct AppState {
    pub store: Store,
    pub queues: Arc<Queues>,
    pub metrics: Arc<Metrics>,
    /// Signalled whenever results are written.
    pub results: Arc<ResultSignal>,
    pub limits: PayloadLimits,
    pub default_result_queue: String,
    /// How long a result delivered by request stays when its queue sets no
    /// result TTL.
    pub request_result_ttl: Duration,
    pub ready: Arc<AtomicBool>,
}

pub fn api_router(state: AppState) -> Router {
    Router::new()
        .route(
            "/v1/requests",
            post(requests::submit).layer(DefaultBodyLimit::disable()),
        )
        .route(
            "/v1/requests/batch",
            post(requests::submit_batch).layer(DefaultBodyLimit::disable()),
        )
        .route("/v1/requests/cancel", post(requests::cancel))
        .route("/v1/requests/{id}", get(requests::status))
        .route("/v1/queues", get(requests::list_queues))
        .route("/v1/results/{route}/pop", post(results::pop))
        .route("/v1/results/{route}/claims", post(results::claim))
        .route(
            "/v1/results/{route}/claims/{claim_id}/renew",
            post(results::renew),
        )
        .route(
            "/v1/results/{route}/claims/{claim_id}/ack",
            post(results::ack),
        )
        .route("/v1/results/{route}/depth", get(results::depth))
        .route("/v1/blobs/results/{name}", get(results::blob))
        .route(
            "/v1/admin/budgets/{key}",
            put(admin::put_budget)
                .get(admin::get_budget)
                .delete(admin::delete_budget),
        )
        .route(
            "/v1/admin/dispatch-rates/{key}",
            put(admin::put_dispatch_rate)
                .get(admin::get_dispatch_rate)
                .delete(admin::delete_dispatch_rate),
        )
        .with_state(state)
}

pub fn health_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(health::healthz))
        .route("/readyz", get(health::readyz))
        .with_state(state)
}

pub fn metrics_router(state: AppState) -> Router {
    Router::new()
        .route("/metrics", get(health::metrics))
        .with_state(state)
}
