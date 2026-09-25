//! Prometheus metrics, named exactly as the Go processor names them so
//! dashboards and alerts carry over.

use prometheus::{
    CounterVec, Encoder, Gauge, GaugeVec, HistogramOpts, HistogramVec, IntCounterVec, Opts,
    Registry, TextEncoder,
};

use crate::telemetry::deadline_proximity::DeadlineProximity;

const SUBSYSTEM: &str = "llm_d_async";

pub const QUEUE_LABELS: [&str; 3] = ["queue_id", "queue_name", "pool_name"];
const GATE_LABELS: [&str; 4] = ["queue_id", "queue_name", "pool_name", "inference_pool"];

/// The queue triple every per-queue series carries. A pool-level series
/// leaves the queue fields empty.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QueueLabels {
    pub queue_id: String,
    pub queue_name: String,
    pub pool: String,
}

impl QueueLabels {
    pub fn new(queue_id: &str, queue_name: &str, pool: &str) -> Self {
        Self {
            queue_id: queue_id.to_owned(),
            queue_name: queue_name.to_owned(),
            pool: pool.to_owned(),
        }
    }

    pub fn pool(pool: &str) -> Self {
        Self::new("", "", pool)
    }

    pub fn values(&self) -> [&str; 3] {
        [&self.queue_id, &self.queue_name, &self.pool]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GateReason {
    GateClosed,
    QuotaExhausted,
    Dropped,
    Error,
}

impl GateReason {
    const ALL: [Self; 4] = [
        Self::GateClosed,
        Self::QuotaExhausted,
        Self::Dropped,
        Self::Error,
    ];

    fn as_str(self) -> &'static str {
        match self {
            Self::GateClosed => "gate_closed",
            Self::QuotaExhausted => "quota_exhausted",
            Self::Dropped => "dropped",
            Self::Error => "error",
        }
    }
}

pub struct Metrics {
    registry: Registry,
    retries: IntCounterVec,
    async_requests: IntCounterVec,
    dispatched: IntCounterVec,
    gate_wait_requeues: IntCounterVec,
    exceeded_deadline: IntCounterVec,
    failed: IntCounterVec,
    successful: IntCounterVec,
    shedded: IntCounterVec,
    tokens: CounterVec,
    inference_latency: HistogramVec,
    queue_residence: HistogramVec,
    queue_depth: GaugeVec,
    inflight: GaugeVec,
    broker_backlog: GaugeVec,
    broker_backlog_source_available: GaugeVec,
    dispatch_budget: GaugeVec,
    pool_worker_limit: GaugeVec,
    queue_config_reloads: IntCounterVec,
    queue_config_last_success: Gauge,
    drain_limit_rps: GaugeVec,
    drain_limit_lease_valid: GaugeVec,
    drain_limit_valid_until: GaugeVec,
    gate_decisions: IntCounterVec,
    gate_metric_value: GaugeVec,
    gate_metric_threshold: GaugeVec,
    gate_metric_source_available: GaugeVec,
    deadline_proximity: DeadlineProximity,
}

fn opts(name: &str, help: &str) -> Opts {
    Opts::new(name, help).subsystem(SUBSYSTEM)
}

fn counter(
    r: &Registry,
    name: &str,
    help: &str,
    labels: &[&str],
) -> Result<IntCounterVec, prometheus::Error> {
    let c = IntCounterVec::new(opts(name, help), labels)?;
    r.register(Box::new(c.clone()))?;
    Ok(c)
}

fn gauge(
    r: &Registry,
    name: &str,
    help: &str,
    labels: &[&str],
) -> Result<GaugeVec, prometheus::Error> {
    let g = GaugeVec::new(opts(name, help), labels)?;
    r.register(Box::new(g.clone()))?;
    Ok(g)
}

fn histogram(
    r: &Registry,
    name: &str,
    help: &str,
    buckets: &[f64],
) -> Result<HistogramVec, prometheus::Error> {
    let h = HistogramVec::new(
        HistogramOpts::new(name, help)
            .subsystem(SUBSYSTEM)
            .buckets(buckets.to_vec()),
        &QUEUE_LABELS,
    )?;
    r.register(Box::new(h.clone()))?;
    Ok(h)
}

fn bool_value(v: bool) -> f64 {
    if v { 1.0 } else { 0.0 }
}

impl Metrics {
    pub fn new() -> Result<Self, prometheus::Error> {
        let r = Registry::new();
        let q = &QUEUE_LABELS;
        let deadline_proximity = DeadlineProximity::new()?;
        r.register(Box::new(deadline_proximity.clone()))?;
        let queue_config_last_success = Gauge::with_opts(opts(
            "async_queue_config_last_success_timestamp_seconds",
            "Unix timestamp of the last successfully applied queue-config hot reload.",
        ))?;
        r.register(Box::new(queue_config_last_success.clone()))?;
        Ok(Self {
            retries: counter(
                &r,
                "async_request_retries_total",
                "Total number of async request retries.",
                q,
            )?,
            async_requests: counter(
                &r,
                "async_request_total",
                "Total number of async requests.",
                q,
            )?,
            dispatched: counter(
                &r,
                "async_dispatched_requests_total",
                "Total number of downstream inference dispatch attempts, including retries.",
                q,
            )?,
            gate_wait_requeues: counter(
                &r,
                "async_gate_wait_requeues_total",
                "Total number of requests re-enqueued after the pool gate wait timeout elapsed.",
                q,
            )?,
            exceeded_deadline: counter(
                &r,
                "async_exceeded_deadline_requests_total",
                "Total number of async requests that exceeded their deadline.",
                q,
            )?,
            failed: counter(
                &r,
                "async_failed_requests_total",
                "Total number of async requests that failed.",
                q,
            )?,
            successful: counter(
                &r,
                "async_successful_requests_total",
                "Total number of async requests that succeeded.",
                q,
            )?,
            shedded: counter(
                &r,
                "async_shedded_requests_total",
                "Total number of async requests that were shedded.",
                q,
            )?,
            tokens: {
                let c = CounterVec::new(
                    opts(
                        "async_tokens_total",
                        "Tokens processed by successful requests, parsed best-effort from the OpenAI usage object.",
                    ),
                    &["queue_id", "queue_name", "pool_name", "direction"],
                )?;
                r.register(Box::new(c.clone()))?;
                c
            },
            inference_latency: histogram(
                &r,
                "async_inference_latency_time_millis",
                "Time spent calling the inference gateway.",
                &[
                    10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0,
                    30000.0, 60000.0, 120000.0,
                ],
            )?,
            queue_residence: histogram(
                &r,
                "async_queue_residence_time_millis",
                "Time a message spent buffered in-process from ingestion until a worker pulled it.",
                &[
                    500.0, 1000.0, 2000.0, 5000.0, 10000.0, 30000.0, 60000.0, 120000.0, 300000.0,
                    600000.0, 1800000.0, 3600000.0, 7200000.0, 21600000.0, 43200000.0, 86400000.0,
                ],
            )?,
            queue_depth: gauge(
                &r,
                "async_queue_depth",
                "Number of requests buffered in-process awaiting an available worker.",
                q,
            )?,
            inflight: gauge(
                &r,
                "async_inflight_requests",
                "Number of requests currently being processed by workers.",
                q,
            )?,
            broker_backlog: gauge(
                &r,
                "async_broker_backlog",
                "Number of pending requests held by the queue.",
                q,
            )?,
            broker_backlog_source_available: gauge(
                &r,
                "async_broker_backlog_source_available",
                "1 when the most recent backlog read for the queue succeeded; 0 otherwise.",
                q,
            )?,
            dispatch_budget: gauge(
                &r,
                "async_dispatch_budget",
                "Current dispatch budget [0.0-1.0] returned by the queue's gate.",
                q,
            )?,
            pool_worker_limit: gauge(
                &r,
                "async_pool_worker_limit",
                "Configured number of concurrent workers for a pool.",
                &["pool_name"],
            )?,
            queue_config_reloads: counter(
                &r,
                "async_queue_config_reloads_total",
                "Count of queue-config hot reload attempts, by result (success or error).",
                &["result"],
            )?,
            queue_config_last_success,
            drain_limit_rps: gauge(
                &r,
                "async_drain_limit_rps",
                "Leased maximum admission rate observed by the most recent gate evaluation for a pool.",
                &["pool_name"],
            )?,
            drain_limit_lease_valid: gauge(
                &r,
                "async_drain_limit_lease_valid",
                "1 when the most recent gate evaluation observed a valid, unexpired drain-limit lease.",
                &["pool_name"],
            )?,
            drain_limit_valid_until: gauge(
                &r,
                "async_drain_limit_valid_until_seconds",
                "Unix timestamp at which the observed drain-limit lease expires; 0 when none is valid.",
                &["pool_name"],
            )?,
            gate_decisions: counter(
                &r,
                "async_gate_decisions_total",
                "Count of gate decisions that prevented dispatch, by reason.",
                &["queue_id", "queue_name", "pool_name", "reason"],
            )?,
            gate_metric_value: gauge(
                &r,
                "async_gate_metric_value",
                "Raw metric value last read by a metric-based dispatch gate.",
                &GATE_LABELS,
            )?,
            gate_metric_threshold: gauge(
                &r,
                "async_gate_metric_threshold",
                "Threshold a metric-based gate compares its value against; closes when value <= threshold.",
                &GATE_LABELS,
            )?,
            gate_metric_source_available: gauge(
                &r,
                "async_gate_metric_source_available",
                "1 when a metric-based gate's last evaluation got a usable reading, 0 when it used its fallback.",
                &GATE_LABELS,
            )?,
            deadline_proximity,
            registry: r,
        })
    }

    pub fn render(&self) -> Result<String, prometheus::Error> {
        let mut buf = Vec::new();
        TextEncoder::new().encode(&self.registry.gather(), &mut buf)?;
        Ok(String::from_utf8_lossy(&buf).into_owned())
    }

    /// The rendered value of `llm_d_async_<name>` whose labels include every
    /// pair in `labels`, or `None` when no such series is rendered.
    #[cfg(test)]
    pub fn sample(&self, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
        let text = self.render().ok()?;
        let prefix = format!("llm_d_async_{name}{{");
        text.lines()
            .filter(|line| line.starts_with(&prefix))
            .find(|line| {
                labels.iter().all(|(k, v)| {
                    let pair = format!("{k}=\"{v}\"");
                    line.contains(&format!("{{{pair}")) || line.contains(&format!(",{pair}"))
                })
            })
            .and_then(|line| line.rsplit(' ').next()?.parse().ok())
    }

    pub fn retry(&self, l: &QueueLabels) {
        self.retries.with_label_values(&l.values()).inc();
    }
    pub fn async_request(&self, l: &QueueLabels) {
        self.async_requests.with_label_values(&l.values()).inc();
    }
    pub fn dispatched(&self, l: &QueueLabels) {
        self.dispatched.with_label_values(&l.values()).inc();
    }
    pub fn gate_wait_requeue(&self, l: &QueueLabels) {
        self.gate_wait_requeues.with_label_values(&l.values()).inc();
    }
    pub fn exceeded_deadline(&self, l: &QueueLabels) {
        self.exceeded_deadline.with_label_values(&l.values()).inc();
    }
    pub fn failed(&self, l: &QueueLabels) {
        self.failed.with_label_values(&l.values()).inc();
    }
    pub fn succeeded(&self, l: &QueueLabels) {
        self.successful.with_label_values(&l.values()).inc();
    }
    pub fn shed(&self, l: &QueueLabels) {
        self.shedded.with_label_values(&l.values()).inc();
    }
    pub fn tokens(&self, l: &QueueLabels, input: u64, output: u64) {
        let [id, name, pool] = l.values();
        self.tokens
            .with_label_values(&[id, name, pool, "input"])
            .inc_by(input as f64);
        self.tokens
            .with_label_values(&[id, name, pool, "output"])
            .inc_by(output as f64);
    }
    pub fn inference_latency(&self, l: &QueueLabels, millis: f64) {
        self.inference_latency
            .with_label_values(&l.values())
            .observe(millis);
    }
    pub fn queue_residence(&self, l: &QueueLabels, millis: f64) {
        self.queue_residence
            .with_label_values(&l.values())
            .observe(millis);
    }
    pub fn queue_depth_inc(&self, l: &QueueLabels) {
        self.queue_depth.with_label_values(&l.values()).inc();
    }
    pub fn queue_depth_dec(&self, l: &QueueLabels) {
        self.queue_depth.with_label_values(&l.values()).dec();
    }
    pub fn inflight_inc(&self, l: &QueueLabels) {
        self.inflight.with_label_values(&l.values()).inc();
    }
    pub fn inflight_dec(&self, l: &QueueLabels) {
        self.inflight.with_label_values(&l.values()).dec();
    }
    pub fn broker_backlog(&self, l: &QueueLabels, depth: u64, available: bool) {
        self.broker_backlog
            .with_label_values(&l.values())
            .set(depth as f64);
        self.broker_backlog_source_available
            .with_label_values(&l.values())
            .set(bool_value(available));
    }
    pub fn deadline_proximity(&self, l: &QueueLabels, cumulative: &[u64]) {
        self.deadline_proximity.set(l, cumulative);
    }
    pub fn dispatch_budget(&self, l: &QueueLabels, budget: f64) {
        self.dispatch_budget
            .with_label_values(&l.values())
            .set(budget);
    }
    pub fn pool_worker_limit(&self, pool: &str, workers: usize) {
        self.pool_worker_limit
            .with_label_values(&[pool])
            .set(workers as f64);
    }
    pub fn queue_config_reload(&self, success: bool) {
        let result = if success { "success" } else { "error" };
        self.queue_config_reloads.with_label_values(&[result]).inc();
        if success {
            self.queue_config_last_success
                .set(crate::clock::now_millis() as f64 / 1000.0);
        }
    }
    /// Records the lease observed by the latest leased-rate evaluation.
    pub fn drain_limit(&self, pool: &str, lease: Option<(f64, i64)>) {
        let (rps, valid_until_ms) = lease.unwrap_or((0.0, 0));
        self.drain_limit_rps.with_label_values(&[pool]).set(rps);
        self.drain_limit_lease_valid
            .with_label_values(&[pool])
            .set(bool_value(lease.is_some()));
        self.drain_limit_valid_until
            .with_label_values(&[pool])
            .set(valid_until_ms as f64 / 1000.0);
    }
    pub fn gate_decision(&self, l: &QueueLabels, reason: GateReason) {
        let [id, name, pool] = l.values();
        self.gate_decisions
            .with_label_values(&[id, name, pool, reason.as_str()])
            .inc();
    }
    /// Pre-creates every reason at 0 so an unfired reason reads 0, not absent.
    pub fn init_gate_decisions(&self, l: &QueueLabels) {
        let [id, name, pool] = l.values();
        for reason in GateReason::ALL {
            self.gate_decisions
                .with_label_values(&[id, name, pool, reason.as_str()]);
        }
    }
    pub fn gate_metric_value(
        &self,
        l: &QueueLabels,
        inference_pool: &str,
        value: f64,
        threshold: f64,
    ) {
        let [id, name, pool] = l.values();
        let labels = [id, name, pool, inference_pool];
        self.gate_metric_value.with_label_values(&labels).set(value);
        self.gate_metric_threshold
            .with_label_values(&labels)
            .set(threshold);
    }
    pub fn gate_metric_source_available(
        &self,
        l: &QueueLabels,
        inference_pool: &str,
        available: bool,
    ) {
        let [id, name, pool] = l.values();
        self.gate_metric_source_available
            .with_label_values(&[id, name, pool, inference_pool])
            .set(bool_value(available));
    }
    /// Drops point-in-time series of a queue that is no longer configured.
    /// Counters and histograms are kept.
    pub fn remove_queue_snapshots(&self, l: &QueueLabels) {
        let values = l.values();
        let _ = self.broker_backlog.remove_label_values(&values);
        let _ = self
            .broker_backlog_source_available
            .remove_label_values(&values);
        let _ = self.dispatch_budget.remove_label_values(&values);
        self.deadline_proximity.remove(l);
    }
}

#[cfg(test)]
mod tests {
    use crate::telemetry::metrics::{GateReason, Metrics, QueueLabels};

    #[test]
    fn renders_go_metric_names() {
        let m = Metrics::new().unwrap();
        let l = QueueLabels::new("q", "queue", "pool");
        m.retry(&l);
        m.init_gate_decisions(&l);
        m.gate_decision(&l, GateReason::QuotaExhausted);
        m.tokens(&l, 3, 4);
        m.deadline_proximity(&l, &[1; 14]);
        m.drain_limit("pool", Some((2.5, 5_000)));
        let text = m.render().unwrap();
        for want in [
            r#"llm_d_async_async_request_retries_total{pool_name="pool",queue_id="q",queue_name="queue"} 1"#,
            r#"llm_d_async_async_gate_decisions_total{pool_name="pool",queue_id="q",queue_name="queue",reason="gate_closed"} 0"#,
            r#"llm_d_async_async_gate_decisions_total{pool_name="pool",queue_id="q",queue_name="queue",reason="quota_exhausted"} 1"#,
            r#"llm_d_async_async_tokens_total{direction="output",pool_name="pool",queue_id="q",queue_name="queue"} 4"#,
            r#"llm_d_async_async_deadline_proximity_millis_bucket{queue_id="q",queue_name="queue",pool_name="pool",le="0"} 1"#,
            r#"llm_d_async_async_drain_limit_valid_until_seconds{pool_name="pool"} 5"#,
        ] {
            assert!(text.contains(want), "missing {want} in\n{text}");
        }
        m.remove_queue_snapshots(&l);
        assert!(
            !m.render()
                .unwrap()
                .contains("deadline_proximity_millis_bucket")
        );
    }
}
