//! Startup and graceful shutdown.
//!
//! Shutdown order:
//! 1. readiness goes false and consumers stop claiming;
//! 2. merge tasks stop; buffered claims return to their queues;
//! 3. in-flight requests get `--drain-timeout` to finish, then are aborted
//!    and returned to their queues;
//! 4. the outcome writer drains and the store closes; the API stays up
//!    until then so result consumers are not cut off.

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

use crate::config::ConfigError;
use crate::config::cli::Cli;
use crate::config::merge_policy::MergePolicyConfig;
use crate::config::pools::{WorkerPoolConfig, WorkerPools};
use crate::config::transport::TransportConfig;
use crate::dispatch::queues::{QueueError, Queues};
use crate::dispatch::{backlog, reload, upkeep, writer};
use crate::gate::factory::{GateConfigError, GateFactory};
use crate::merge::{self, PoolSpec};
use crate::server::{self, AppState, PayloadLimits};
use crate::store::error::StoreError;
use crate::store::{Store, StoreOptions};
use crate::telemetry::metrics::{Metrics, QueueLabels};
use crate::tls::{self, TlsError};
use crate::worker::Worker;
use crate::worker::client::InferenceClient;

#[derive(Debug, thiserror::Error)]
pub enum RunError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error("metrics: {0}")]
    Metrics(#[from] prometheus::Error),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("pool {pool:?}: {source}")]
    PoolGate {
        pool: String,
        source: GateConfigError,
    },
    #[error(transparent)]
    Gate(#[from] GateConfigError),
    #[error(transparent)]
    Queue(#[from] QueueError),
    #[error(transparent)]
    Tls(#[from] TlsError),
    #[error("invalid --prometheus-url: {0}")]
    PrometheusUrl(String),
    #[error("bind {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        source: std::io::Error,
    },
}

async fn bind(addr: SocketAddr) -> Result<TcpListener, RunError> {
    TcpListener::bind(addr)
        .await
        .map_err(|source| RunError::Bind { addr, source })
}

fn serve(
    tasks: &TaskTracker,
    listener: TcpListener,
    router: axum::Router,
    stop: CancellationToken,
    name: &'static str,
) {
    tasks.spawn(async move {
        let served = axum::serve(listener, router)
            .with_graceful_shutdown(async move { stop.cancelled().await })
            .await;
        if let Err(e) = served {
            tracing::error!(server = name, error = %e, "server failed");
        }
    });
}

/// Runs the processor until `shutdown` resolves, then drains.
pub async fn run(cli: Cli, shutdown: impl Future<Output = ()>) -> Result<(), RunError> {
    cli.validate()?;
    let pools = match &cli.pool_config_file {
        Some(path) => WorkerPools::load(path)?,
        None => WorkerPools::new(vec![WorkerPoolConfig::new("default", cli.concurrency)])?,
    };
    let transport = TransportConfig::parse(&cli.transport_config_bytes()?, &pools, false)?;
    let merge_policy = match &cli.request_merge_policy_config_file {
        Some(path) => MergePolicyConfig::load(path)?,
        None => MergePolicyConfig::default(),
    };
    let prometheus = cli
        .prometheus_url
        .as_deref()
        .filter(|u| !u.is_empty())
        .map(|u| reqwest::Url::parse(u).map_err(|e| RunError::PrometheusUrl(e.to_string())))
        .transpose()?;

    let metrics = Arc::new(Metrics::new()?);
    for pool in pools.iter() {
        metrics.pool_worker_limit(&pool.id, pool.workers);
    }
    let (store, recovery) = Store::open(
        &cli.data_dir,
        &StoreOptions {
            result_blob_retention: cli.result_blob_retention,
        },
    )?;
    tracing::info!(
        data_dir = %cli.data_dir.display(),
        recovered_claims = recovery.claims,
        orphan_blobs = recovery.orphan_blobs,
        "store opened"
    );

    let factory = Arc::new(GateFactory::new(
        store.clone(),
        Arc::clone(&metrics),
        prometheus,
        cli.prometheus_cache_ttl,
    )?);
    let inference = Arc::new(InferenceClient::new(
        tls::inference_client(&cli, pools.total_workers())?,
        store.blobs().clone(),
    ));

    let consume = CancellationToken::new();
    let drain = CancellationToken::new();
    let stop_servers = CancellationToken::new();
    let background = TaskTracker::new();
    let workers = TaskTracker::new();
    let results = Arc::new(Notify::new());

    let mut queues_per_pool: HashMap<&str, usize> = HashMap::new();
    for q in &transport.queues {
        *queues_per_pool
            .entry(q.worker_pool_id.as_str())
            .or_default() += 1;
    }
    let pool_specs: Vec<PoolSpec> = pools
        .iter()
        .map(|p| PoolSpec {
            id: p.id.clone(),
            buffer: queues_per_pool.get(p.id.as_str()).copied().unwrap_or(0),
        })
        .collect();
    let (merge, receivers) =
        merge::start(&merge_policy, &pool_specs, &metrics, &consume, &background);

    let (outcomes_tx, outcomes_rx) = mpsc::unbounded_channel();
    let writer = tokio::spawn(writer::run(
        store.clone(),
        outcomes_rx,
        Arc::clone(&results),
    ));

    let queues = Arc::new(Queues::new(
        store.clone(),
        Arc::clone(&metrics),
        Arc::clone(&factory),
        merge,
        outcomes_tx,
        Arc::clone(&results),
        Duration::from_millis(transport.poll_interval_ms),
        transport.batch_size,
        consume.clone(),
    ));

    let ready = Arc::new(AtomicBool::new(false));
    let state = AppState {
        store: store.clone(),
        queues: Arc::clone(&queues),
        metrics: Arc::clone(&metrics),
        results: Arc::clone(&results),
        limits: PayloadLimits {
            inline: cli.inline_payload_limit,
            max: cli.max_payload_bytes,
            json_body: cli.max_json_body_bytes,
        },
        default_result_queue: transport.result_queue_name.clone(),
        ready: Arc::clone(&ready),
    };
    let servers = TaskTracker::new();
    let health_addr = SocketAddr::from(([0, 0, 0, 0], cli.health_port));
    let metrics_addr = SocketAddr::from(([0, 0, 0, 0], cli.metrics_port));
    serve(
        &servers,
        bind(health_addr).await?,
        server::health_router(state.clone()),
        stop_servers.clone(),
        "health",
    );
    serve(
        &servers,
        bind(metrics_addr).await?,
        server::metrics_router(state.clone()),
        stop_servers.clone(),
        "metrics",
    );
    serve(
        &servers,
        bind(cli.api_addr).await?,
        server::api_router(state),
        stop_servers.clone(),
        "api",
    );

    let started = start(
        &cli,
        &pools,
        &transport,
        &factory,
        &queues,
        &metrics,
        &store,
        &inference,
        receivers,
        &consume,
        &drain,
        &background,
        &workers,
    )
    .await;
    if let Err(e) = started {
        consume.cancel();
        drain.cancel();
        queues.stop().await;
        stop_servers.cancel();
        return Err(e);
    }

    ready.store(true, Ordering::Relaxed);
    tracing::info!(api = %cli.api_addr, "llm-d-async started");
    shutdown.await;

    ready.store(false, Ordering::Relaxed);
    tracing::info!("signal received, stopping message consumption");
    consume.cancel();
    queues.stop().await;

    tracing::info!(timeout = ?cli.drain_timeout, "draining in-flight requests");
    workers.close();
    if tokio::time::timeout(cli.drain_timeout, workers.wait())
        .await
        .is_err()
    {
        tracing::info!("drain timeout reached, returning in-flight requests to their queues");
        drain.cancel();
        workers.wait().await;
    }
    drain.cancel();
    background.close();
    background.wait().await;
    drop(queues);
    if let Err(e) = writer.await {
        tracing::error!(error = %e, "outcome writer failed");
    }
    stop_servers.cancel();
    servers.close();
    servers.wait().await;
    tracing::info!("llm-d-async stopped");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn start(
    cli: &Cli,
    pools: &WorkerPools,
    transport: &TransportConfig,
    factory: &Arc<GateFactory>,
    queues: &Arc<Queues>,
    metrics: &Arc<Metrics>,
    store: &Store,
    inference: &Arc<InferenceClient>,
    receivers: HashMap<String, merge::DispatchReceiver>,
    consume: &CancellationToken,
    drain: &CancellationToken,
    background: &TaskTracker,
    workers: &TaskTracker,
) -> Result<(), RunError> {
    for pool in pools.iter() {
        let pool_gate = if pool.gate_type.is_empty() {
            None
        } else {
            let gate = factory
                .create(
                    &pool.gate_type,
                    &pool.gate_params,
                    &QueueLabels::pool(&pool.id),
                )
                .map_err(|source| RunError::PoolGate {
                    pool: pool.id.clone(),
                    source,
                })?;
            metrics.init_gate_decisions(&QueueLabels::pool(&pool.id));
            tracing::info!(pool = %pool.id, gate_type = %pool.gate_type, "created pool gate");
            Some(gate)
        };
        let Some(rx) = receivers.get(&pool.id) else {
            continue;
        };
        let worker = Arc::new(Worker {
            store: store.clone(),
            metrics: Arc::clone(metrics),
            client: Arc::clone(inference),
            pool: pool.id.clone(),
            pool_gate,
            request_timeout: cli.request_timeout,
            gate_wait_timeout: cli.gate_wait_timeout,
            consume: consume.clone(),
            drain: drain.clone(),
        });
        tracing::info!(pool = %pool.id, workers = pool.workers, "spawning workers");
        for _ in 0..pool.workers {
            workers.spawn(Arc::clone(&worker).run(Arc::clone(rx)));
        }
    }

    let change = queues.apply(transport.queues.clone()).await?;
    tracing::info!(queues = change.added, "queues started");

    let poll_interval = Duration::from_millis(transport.poll_interval_ms);
    background.spawn(upkeep::promote_retries(
        store.clone(),
        poll_interval,
        consume.clone(),
    ));
    background.spawn(upkeep::sweep(store.clone(), consume.clone()));
    if !cli.metrics_backlog_poll_interval.is_zero() {
        background.spawn(backlog::run(
            store.clone(),
            Arc::clone(metrics),
            Arc::clone(queues),
            cli.metrics_backlog_poll_interval,
            consume.clone(),
        ));
    }
    if let Some(path) = &cli.transport_config_file
        && !cli.transport_config_watch_interval.is_zero()
    {
        background.spawn(reload::watch(
            path.clone(),
            cli.transport_config_watch_interval,
            transport.clone(),
            pools.clone(),
            Arc::clone(queues),
            Arc::clone(metrics),
            consume.clone(),
        ));
    }
    Ok(())
}
