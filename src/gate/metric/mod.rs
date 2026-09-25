//! Budget gates driven by a metric.

pub mod source;

use std::sync::Arc;

use crate::api::request::InternalRequest;
use crate::boxed::BoxFuture;
use crate::gate::metric::source::MetricSource;
use crate::gate::release::Releases;
use crate::gate::{Gate, GateError, Verdict, budget_verdict};
use crate::telemetry::metrics::{Metrics, QueueLabels};

/// Reads a budget D from its source and returns D − threshold clamped to
/// [0, 1], closing when D ≤ threshold. With the threshold set to a reserved
/// baseline B, callers dispatch N = max_SYS × (D − B). An unusable reading
/// returns `fallback`.
pub struct MetricGate {
    source: Arc<dyn MetricSource>,
    threshold: f64,
    fallback: f64,
    owner: QueueLabels,
    inference_pool: String,
    metrics: Arc<Metrics>,
}

impl MetricGate {
    pub fn new(
        source: Arc<dyn MetricSource>,
        threshold: f64,
        fallback: f64,
        owner: QueueLabels,
        inference_pool: String,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            source,
            threshold,
            fallback: fallback.clamp(0.0, 1.0),
            owner,
            inference_pool,
            metrics,
        }
    }

    /// For a source returning 1 − saturation, with threshold and fallback
    /// given in saturation terms.
    pub fn saturation(
        source: Arc<dyn MetricSource>,
        threshold: f64,
        fallback: f64,
        owner: QueueLabels,
        inference_pool: String,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self::new(
            source,
            1.0 - threshold,
            1.0 - fallback,
            owner,
            inference_pool,
            metrics,
        )
    }

    fn use_fallback(&self, reason: &str) -> f64 {
        self.metrics
            .gate_metric_source_available(&self.owner, &self.inference_pool, false);
        tracing::error!(
            reason,
            fallback = self.fallback,
            pool = %self.owner.pool,
            queue = %self.owner.queue_name,
            "metric gate using fallback budget"
        );
        self.fallback
    }
}

impl Gate for MetricGate {
    fn budget(&self) -> BoxFuture<'_, f64> {
        Box::pin(async move {
            let samples = match self.source.query().await {
                Ok(samples) => samples,
                Err(e) => return self.use_fallback(&e.to_string()),
            };
            let Some(value) = samples.first().map(|s| s.value) else {
                return self.use_fallback("no metric samples found");
            };
            if !value.is_finite() {
                return self.use_fallback(&format!("invalid metric value {value}"));
            }
            self.metrics.gate_metric_value(
                &self.owner,
                &self.inference_pool,
                value,
                self.threshold,
            );
            self.metrics
                .gate_metric_source_available(&self.owner, &self.inference_pool, true);
            if value <= self.threshold {
                0.0
            } else {
                (value - self.threshold).clamp(0.0, 1.0)
            }
        })
    }

    fn apply<'a>(
        &'a self,
        _msg: &'a mut InternalRequest,
        _releases: &'a mut Releases,
    ) -> BoxFuture<'a, Result<Verdict, GateError>> {
        Box::pin(async move { Ok(budget_verdict(self.budget().await)) })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::boxed::BoxFuture;
    use crate::gate::Gate;
    use crate::gate::metric::MetricGate;
    use crate::gate::metric::source::{MetricSource, Sample, SourceError};
    use crate::telemetry::metrics::{Metrics, QueueLabels};

    struct Fixed(Result<Vec<f64>, SourceError>);

    impl MetricSource for Fixed {
        fn query(&self) -> BoxFuture<'_, Result<Vec<Sample>, SourceError>> {
            let reading = self.0.clone().map(|values| {
                values
                    .into_iter()
                    .map(|value| Sample {
                        labels: Default::default(),
                        value,
                    })
                    .collect()
            });
            Box::pin(async move { reading })
        }
    }

    fn gate(
        reading: Result<Vec<f64>, SourceError>,
        threshold: f64,
        fallback: f64,
        m: &Arc<Metrics>,
    ) -> MetricGate {
        MetricGate::new(
            Arc::new(Fixed(reading)),
            threshold,
            fallback,
            QueueLabels::new("q", "queue", "pool"),
            "ip".into(),
            Arc::clone(m),
        )
    }

    #[tokio::test]
    async fn budget_is_value_minus_threshold() {
        let m = Arc::new(Metrics::new().unwrap());
        assert!((gate(Ok(vec![0.8]), 0.05, 0.0, &m).budget().await - 0.75).abs() < 1e-12);
        assert_eq!(gate(Ok(vec![0.05]), 0.05, 0.0, &m).budget().await, 0.0);
        assert_eq!(gate(Ok(vec![2.0]), 0.0, 0.0, &m).budget().await, 1.0);
        let text = m.render().unwrap();
        assert!(text.contains(r#"llm_d_async_async_gate_metric_source_available{inference_pool="ip",pool_name="pool",queue_id="q",queue_name="queue"} 1"#));
    }

    #[tokio::test]
    async fn unusable_readings_use_the_clamped_fallback() {
        let m = Arc::new(Metrics::new().unwrap());
        assert_eq!(
            gate(Err(SourceError::Exhausted), 0.0, 0.3, &m)
                .budget()
                .await,
            0.3
        );
        assert_eq!(gate(Ok(vec![]), 0.0, 7.0, &m).budget().await, 1.0);
        assert_eq!(gate(Ok(vec![f64::NAN]), 0.0, -1.0, &m).budget().await, 0.0);
        assert!(m.render().unwrap().contains(r#"llm_d_async_async_gate_metric_source_available{inference_pool="ip",pool_name="pool",queue_id="q",queue_name="queue"} 0"#));
    }

    #[tokio::test]
    async fn saturation_gate_works_in_saturation_terms() {
        let m = Arc::new(Metrics::new().unwrap());
        let g = MetricGate::saturation(
            Arc::new(Fixed(Ok(vec![0.3]))),
            0.8,
            0.0,
            QueueLabels::default(),
            String::new(),
            m,
        );
        // Saturation 0.7 is under the 0.8 threshold: budget 0.3 - 0.2.
        assert!((g.budget().await - 0.1).abs() < 1e-12);
    }
}
