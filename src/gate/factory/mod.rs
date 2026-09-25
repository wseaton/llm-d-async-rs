//! Builds gates from `gate_type` and `gate_params`.
//!
//! | gate_type                 | kind      | notes                                        |
//! |---------------------------|-----------|----------------------------------------------|
//! | `constant`                | budget    | always open                                  |
//! | `composite`               | combinator| every inner gate must admit                  |
//! | `wait-on-refuse`          | combinator| inner Refuse becomes Wait                    |
//! | `tier-priority-admission` | combinator| sheds by tier once a saturation gate refuses |
//! | `local-max-concurrency`   | admission | in-process in-flight cap                     |
//! | `quota`                   | admission | per-tenant rate or concurrency quota         |
//! | `budget-key`              | budget    | budget set through the admin API             |
//! | `leased-rate`             | admission | controller-leased pool dispatch rate         |
//! | `prometheus-saturation`   | budget    | 1 − EPP pool saturation                      |
//! | `prometheus-budget`       | budget    | cascade of queue-depth sources               |
//! | `prometheus-query`        | budget    | arbitrary PromQL                             |
//! | `endpoint-scrape`         | budget    | scrape a /metrics endpoint                   |
//!
//! `redis-quota`, `redis` and `redis-leased-rate` are accepted as aliases of
//! `quota`, `budget-key` and `leased-rate`; their Redis `address` is ignored.

pub mod params;

use std::sync::Arc;
use std::time::Duration;

use reqwest::Url;

use crate::config::transport::GateParams;
use crate::gate::SharedGate;
use crate::gate::admission::GatingMode;
use crate::gate::admission::budget_key::BudgetKeyGate;
use crate::gate::admission::counters::Counters;
use crate::gate::admission::leased_rate::LeasedRateGate;
use crate::gate::admission::local_concurrency::LocalConcurrencyGate;
use crate::gate::admission::quota::{QuotaGate, QuotaMode};
use crate::gate::combinator::composite::CompositeGate;
use crate::gate::combinator::tier_admission::TierAdmissionGate;
use crate::gate::combinator::wait_on_refuse::WaitOnRefuseGate;
use crate::gate::constant::OpenGate;
use crate::gate::factory::params::Params;
use crate::gate::metric::MetricGate;
use crate::gate::metric::source::cached::CachedSource;
use crate::gate::metric::source::cascade::CascadeSource;
use crate::gate::metric::source::promql::PromQlSource;
use crate::gate::metric::source::scrape::{ScrapeConfig, ScrapeSource};
use crate::gate::metric::source::{MetricSource, queries};
use crate::store::Store;
use crate::telemetry::metrics::{Metrics, QueueLabels};

const PROMETHEUS_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, thiserror::Error)]
pub enum GateConfigError {
    #[error("unknown gate_type {0:?}")]
    Unknown(String),
    #[error("{gate} gate: {message}")]
    Param { gate: String, message: String },
    #[error("{0} gate requires --prometheus-url")]
    NoPrometheus(String),
    #[error("build HTTP client: {0}")]
    Http(#[from] reqwest::Error),
}

fn param_err(gate: &str) -> impl Fn(String) -> GateConfigError + '_ {
    move |message| GateConfigError::Param {
        gate: gate.to_owned(),
        message,
    }
}

pub struct GateFactory {
    store: Store,
    counters: Arc<dyn Counters>,
    metrics: Arc<Metrics>,
    http: reqwest::Client,
    prometheus: Option<Url>,
    cache_ttl: Duration,
}

impl GateFactory {
    pub fn new(
        store: Store,
        counters: Arc<dyn Counters>,
        metrics: Arc<Metrics>,
        prometheus: Option<Url>,
        cache_ttl: Duration,
    ) -> Result<Self, GateConfigError> {
        Ok(Self {
            store,
            counters,
            metrics,
            http: reqwest::Client::builder()
                .timeout(PROMETHEUS_TIMEOUT)
                .build()?,
            prometheus,
            cache_ttl,
        })
    }

    /// Builds the gate for `gate_type`; an empty type is an open gate.
    pub fn create(
        &self,
        gate_type: &str,
        gate_params: &GateParams,
        owner: &QueueLabels,
    ) -> Result<SharedGate, GateConfigError> {
        let p = Params(gate_params);
        let err = param_err(gate_type);
        let gate: SharedGate = match gate_type {
            "" | "constant" => Arc::new(OpenGate),
            "composite" => {
                let inner = p
                    .gates("gates")
                    .map_err(&err)?
                    .iter()
                    .map(|g| self.create(&g.gate_type, &g.gate_params, owner))
                    .collect::<Result<Vec<_>, _>>()?;
                Arc::new(CompositeGate::new(inner))
            }
            "wait-on-refuse" => {
                let inner = p.gate("gate").map_err(&err)?;
                Arc::new(WaitOnRefuseGate::new(self.create(
                    &inner.gate_type,
                    &inner.gate_params,
                    owner,
                )?))
            }
            "tier-priority-admission" => {
                let saturation_type = p.string("saturation_gate", "");
                if saturation_type.is_empty() {
                    return Err(err("requires a 'saturation_gate' parameter".into()));
                }
                let saturation_params = p.object("saturation_gate_params").map_err(&err)?;
                let saturation = self.create(&saturation_type, &saturation_params, owner)?;
                Arc::new(TierAdmissionGate::new(
                    saturation,
                    p.string("tier_label", "tier"),
                ))
            }
            "local-max-concurrency" => {
                let limit = p.int("limit", 0).map_err(&err)?;
                let limit = usize::try_from(limit)
                    .ok()
                    .filter(|l| *l > 0)
                    .ok_or_else(|| err(format!("limit must be greater than 0, got {limit}")))?;
                let mode = gating_mode(&p, GatingMode::Classifying).map_err(&err)?;
                Arc::new(LocalConcurrencyGate::new(limit, mode))
            }
            "quota" | "redis-quota" => self.quota(&p).map_err(&err)?,
            "budget-key" | "redis" => Arc::new(BudgetKeyGate::new(
                self.store.clone(),
                p.string("budget_key", "dispatch-gate-budget"),
            )),
            "leased-rate" | "redis-leased-rate" => self.leased_rate(&p, owner).map_err(&err)?,
            "prometheus-saturation" => {
                let prometheus = self.prometheus(gate_type)?;
                let pool = required(&p, "pool").map_err(&err)?;
                let threshold = p.float("threshold", 0.8).map_err(&err)?;
                let fallback = p.float("fallback", 0.0).map_err(&err)?;
                let expr = queries::saturation(&pool, &p.string("namespace", ""));
                tracing::info!(%pool, query = %expr, "prometheus-saturation metric source");
                let source = self.promql(prometheus, expr).map_err(&err)?;
                Arc::new(MetricGate::saturation(
                    self.cached(source),
                    threshold,
                    fallback,
                    owner.clone(),
                    pool,
                    Arc::clone(&self.metrics),
                ))
            }
            "prometheus-budget" => self.prometheus_budget(&p, owner, gate_type)?,
            "prometheus-query" => {
                let prometheus = self.prometheus(gate_type)?;
                let expr = required(&p, "query").map_err(&err)?;
                let fallback = p.float("fallback", 0.0).map_err(&err)?;
                let source = self.promql(prometheus, expr).map_err(&err)?;
                Arc::new(MetricGate::new(
                    self.cached(source),
                    0.0,
                    fallback,
                    owner.clone(),
                    p.string("pool", ""),
                    Arc::clone(&self.metrics),
                ))
            }
            "endpoint-scrape" => self.endpoint_scrape(&p, owner).map_err(&err)?,
            other => return Err(GateConfigError::Unknown(other.to_owned())),
        };
        if gate_type.starts_with("redis") {
            tracing::warn!(
                gate_type,
                "Redis gate type is an alias for the store-backed gate; 'address' is ignored"
            );
        }
        Ok(gate)
    }

    fn prometheus(&self, gate_type: &str) -> Result<&Url, GateConfigError> {
        self.prometheus
            .as_ref()
            .ok_or_else(|| GateConfigError::NoPrometheus(gate_type.to_owned()))
    }

    fn promql(&self, prometheus: &Url, expr: String) -> Result<Arc<dyn MetricSource>, String> {
        PromQlSource::new(self.http.clone(), prometheus, expr)
            .map(|s| Arc::new(s) as Arc<dyn MetricSource>)
            .map_err(|e| e.to_string())
    }

    fn cached(&self, source: Arc<dyn MetricSource>) -> Arc<dyn MetricSource> {
        if self.cache_ttl.is_zero() {
            source
        } else {
            Arc::new(CachedSource::new(source, self.cache_ttl))
        }
    }

    fn quota(&self, p: &Params<'_>) -> Result<SharedGate, String> {
        let mode_name = p.string("mode", "rate-limit");
        let mode = QuotaMode::parse(&mode_name).ok_or_else(|| {
            format!("mode must be 'rate-limit' or 'concurrency', got {mode_name:?}")
        })?;
        let limit = p.int("limit", 0)?;
        let limit = u32::try_from(limit)
            .ok()
            .filter(|l| *l > 0)
            .ok_or_else(|| format!("requires a positive 'limit', got {limit}"))?;
        let window = p.duration("window", Duration::from_secs(60))?;
        if mode == QuotaMode::RateLimit && window.is_zero() {
            return Err("rate-limit mode requires a positive 'window'".into());
        }
        let gating = gating_mode(p, GatingMode::Blocking)?;
        Ok(Arc::new(QuotaGate::new(
            p.string("attribute", "userid"),
            p.string("prefix", "quota:"),
            mode,
            gating,
            limit,
            window,
            Arc::clone(&self.counters),
        )))
    }

    fn leased_rate(&self, p: &Params<'_>, owner: &QueueLabels) -> Result<SharedGate, String> {
        let pool_id = p.string("pool_id", &owner.pool);
        if pool_id.is_empty() {
            return Err("requires a 'pool_id' or a worker-pool owner".into());
        }
        let control_key = required(p, "control_key")?;
        let burst_seconds = p.float("burst_seconds", 1.0)?;
        if !burst_seconds.is_finite() || burst_seconds <= 0.0 {
            return Err(format!(
                "burst_seconds must be finite and positive, got {burst_seconds}"
            ));
        }
        let state_key = p.string("state_key", &format!("{control_key}:state"));
        Ok(Arc::new(LeasedRateGate::new(
            self.store.clone(),
            control_key,
            state_key,
            pool_id,
            burst_seconds,
            Arc::clone(&self.counters),
            Arc::clone(&self.metrics),
        )))
    }

    fn prometheus_budget(
        &self,
        p: &Params<'_>,
        owner: &QueueLabels,
        gate_type: &str,
    ) -> Result<SharedGate, GateConfigError> {
        let err = param_err(gate_type);
        let prometheus = self.prometheus(gate_type)?;
        let pool = required(p, "pool").map_err(&err)?;
        let max_concurrency = p.float("max_concurrency", 100.0).map_err(&err)?;
        if max_concurrency <= 0.0 || !max_concurrency.is_finite() {
            return Err(err(format!(
                "max_concurrency must be positive, got {max_concurrency}"
            )));
        }
        let baseline = p.float("baseline", 0.05).map_err(&err)?;
        if !(0.0..1.0).contains(&baseline) {
            return Err(err(format!("baseline must be in [0, 1), got {baseline}")));
        }
        let fallback = p.float("fallback", 0.0).map_err(&err)?;
        let namespace = p.string("namespace", "");
        let exprs = [
            queries::flow_control_queue(&pool, max_concurrency, &namespace),
            queries::pool_queue(&pool, max_concurrency, &namespace),
            queries::vllm_running(&pool, max_concurrency, &namespace),
        ];
        let mut sources = Vec::with_capacity(exprs.len());
        for (i, expr) in exprs.into_iter().enumerate() {
            tracing::info!(%pool, source_index = i, query = %expr, "prometheus-budget metric source");
            sources.push(self.cached(self.promql(prometheus, expr).map_err(&err)?));
        }
        tracing::info!(
            %pool,
            max_concurrency,
            baseline,
            closes_at_load_per_ready_pod = max_concurrency * (1.0 - baseline),
            "prometheus-budget gate configured"
        );
        Ok(Arc::new(MetricGate::new(
            Arc::new(CascadeSource::new(sources)),
            baseline,
            fallback,
            owner.clone(),
            pool,
            Arc::clone(&self.metrics),
        )))
    }

    fn endpoint_scrape(&self, p: &Params<'_>, owner: &QueueLabels) -> Result<SharedGate, String> {
        let value_type = p.string("value_type", "saturation");
        let direct_budget = match value_type.as_str() {
            "saturation" => false,
            "budget" => true,
            other => {
                return Err(format!(
                    "value_type must be either 'saturation' or 'budget', got {other:?}"
                ));
            }
        };
        let cfg = ScrapeConfig {
            url: required(p, "url")?,
            metric: required(p, "metric")?,
            labels: p.string_map("labels")?,
            max_count_per_pod: p.float("max_count_per_pod", 0.0)?,
            direct_budget,
            pods_url: p.string("pods_url", ""),
            pods_metric: p.string("pods_metric", ""),
            pods_labels: p.string_map("pods_labels")?,
        };
        let baseline = p.float("baseline", 0.0)?;
        let fallback = p.float("fallback", 0.0)?;
        let source = Arc::new(ScrapeSource::new(self.http.clone(), cfg));
        Ok(Arc::new(MetricGate::new(
            self.cached(source),
            baseline,
            fallback,
            owner.clone(),
            String::new(),
            Arc::clone(&self.metrics),
        )))
    }
}

fn required(p: &Params<'_>, key: &str) -> Result<String, String> {
    let value = p.string(key, "");
    if value.is_empty() {
        Err(format!("requires a {key:?} parameter"))
    } else {
        Ok(value)
    }
}

fn gating_mode(p: &Params<'_>, default: GatingMode) -> Result<GatingMode, String> {
    let name = p.string("gating_mode", "");
    if name.is_empty() {
        return Ok(default);
    }
    GatingMode::parse(&name)
        .ok_or_else(|| format!("gating_mode must be 'blocking' or 'classifying', got {name:?}"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use serde_json::json;

    use crate::config::transport::GateParams;
    use crate::gate::Verdict;
    use crate::gate::admission::counters::local::LocalCounters;
    use crate::gate::factory::{GateConfigError, GateFactory};
    use crate::gate::release::Releases;
    use crate::gate::test_support::request;
    use crate::store::embedded::test_support::{Fixture, open};
    use crate::telemetry::metrics::{Metrics, QueueLabels};

    fn params(v: serde_json::Value) -> GateParams {
        v.as_object().unwrap().clone()
    }

    async fn factory(prometheus: Option<&str>) -> (Fixture, GateFactory) {
        let fixture = open().await;
        let f = GateFactory::new(
            fixture.store.clone(),
            Arc::new(LocalCounters::default()),
            Arc::new(Metrics::new().unwrap()),
            prometheus.map(|u| u.parse().unwrap()),
            Duration::from_secs(5),
        )
        .unwrap();
        (fixture, f)
    }

    #[tokio::test]
    async fn builds_every_gate_type() {
        let (_dir, f) = factory(Some("http://prom:9090")).await;
        let owner = QueueLabels::new("q", "queue", "pool");
        let cases = [
            ("", json!({})),
            ("constant", json!({})),
            (
                "composite",
                json!({"gates": [{"gate_type": "constant"}, {"gate_type": "local-max-concurrency", "gate_params": {"limit": 2}}]}),
            ),
            (
                "composite",
                json!({"gates": "[{\"gate_type\":\"constant\"}]"}),
            ),
            (
                "wait-on-refuse",
                json!({"gate": {"gate_type": "leased-rate", "gate_params": {"control_key": "c"}}}),
            ),
            (
                "tier-priority-admission",
                json!({"saturation_gate": "local-max-concurrency", "saturation_gate_params": {"limit": 1}}),
            ),
            (
                "local-max-concurrency",
                json!({"limit": "3", "gating_mode": "blocking"}),
            ),
            ("quota", json!({"limit": 5, "mode": "concurrency"})),
            (
                "redis-quota",
                json!({"limit": 5, "window": "30s", "address": "redis:6379"}),
            ),
            ("budget-key", json!({})),
            ("redis", json!({"address": "redis:6379", "budget_key": "b"})),
            (
                "leased-rate",
                json!({"control_key": "c", "burst_seconds": 0.5}),
            ),
            ("prometheus-saturation", json!({"pool": "p"})),
            (
                "prometheus-budget",
                json!({"pool": "p", "max_concurrency": 32, "namespace": "ns"}),
            ),
            ("prometheus-query", json!({"query": "vector(1)"})),
            (
                "endpoint-scrape",
                json!({"url": "http://epp/metrics", "metric": "m", "labels": {"a": "b"}, "value_type": "budget"}),
            ),
        ];
        for (gate_type, p) in cases {
            if let Err(e) = f.create(gate_type, &params(p.clone()), &owner) {
                panic!("{gate_type} {p}: {e}");
            }
        }
    }

    #[tokio::test]
    async fn rejects_bad_configs() {
        let (_dir, f) = factory(None).await;
        let owner = QueueLabels::pool("pool");
        let cases: &[(&str, serde_json::Value, &str)] = &[
            ("nope", json!({}), "unknown gate_type"),
            ("composite", json!({}), "requires"),
            (
                "composite",
                json!({"gates": [{"gate_type": "nope"}]}),
                "unknown gate_type",
            ),
            ("wait-on-refuse", json!({}), "requires"),
            ("tier-priority-admission", json!({}), "saturation_gate"),
            (
                "local-max-concurrency",
                json!({"limit": 0}),
                "greater than 0",
            ),
            (
                "local-max-concurrency",
                json!({"limit": 1, "gating_mode": "maybe"}),
                "gating_mode",
            ),
            ("quota", json!({}), "positive 'limit'"),
            (
                "quota",
                json!({"limit": 1, "mode": "tokens"}),
                "mode must be",
            ),
            (
                "quota",
                json!({"limit": 1, "window": "0s"}),
                "positive 'window'",
            ),
            ("quota", json!({"limit": 1.5}), "not an integer"),
            ("leased-rate", json!({}), "control_key"),
            (
                "leased-rate",
                json!({"control_key": "c", "burst_seconds": 0}),
                "burst_seconds",
            ),
            (
                "prometheus-saturation",
                json!({"pool": "p"}),
                "--prometheus-url",
            ),
            ("endpoint-scrape", json!({"url": "u"}), "metric"),
            (
                "endpoint-scrape",
                json!({"url": "u", "metric": "m", "value_type": "x"}),
                "value_type",
            ),
        ];
        for (gate_type, p, want) in cases {
            let e = f
                .create(gate_type, &params(p.clone()), &owner)
                .err()
                .unwrap();
            assert!(e.to_string().contains(want), "{gate_type} {p}: {e}");
        }
        let (_dir, f) = factory(Some("http://prom")).await;
        let e = f
            .create(
                "prometheus-budget",
                &params(json!({"pool": "p", "baseline": 1.0})),
                &owner,
            )
            .err()
            .unwrap();
        assert!(e.to_string().contains("baseline"), "{e}");
        assert!(matches!(
            f.create(
                "leased-rate",
                &params(json!({"control_key": "c"})),
                &QueueLabels::default()
            ),
            Err(GateConfigError::Param { .. })
        ));
    }

    #[tokio::test]
    async fn quota_gates_with_one_prefix_share_counters() {
        let (_dir, f) = factory(None).await;
        let p = params(json!({"limit": 1, "mode": "concurrency"}));
        let a = f.create("quota", &p, &QueueLabels::default()).unwrap();
        let b = f.create("quota", &p, &QueueLabels::default()).unwrap();
        let other = f
            .create(
                "quota",
                &params(json!({"limit": 1, "mode": "concurrency", "prefix": "x:"})),
                &QueueLabels::default(),
            )
            .unwrap();
        let mut held = Releases::default();
        assert_eq!(
            a.apply(&mut request(&[("userid", "t")]), &mut held).await,
            Verdict::Continue
        );
        assert_eq!(
            b.apply(&mut request(&[("userid", "t")]), &mut Releases::default())
                .await,
            Verdict::Refuse
        );
        assert_eq!(
            other
                .apply(&mut request(&[("userid", "t")]), &mut Releases::default())
                .await,
            Verdict::Continue
        );
    }
}
