use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, Ordering};

use crate::boxed::BoxFuture;
use crate::gate::metric::source::{MetricSource, Sample, SourceError};

/// Tries sources in order and returns the first non-empty reading. Logs only
/// when the serving source changes.
pub struct CascadeSource {
    sources: Vec<Arc<dyn MetricSource>>,
    /// -1 until a source first resolves.
    active: AtomicIsize,
}

impl CascadeSource {
    pub fn new(sources: Vec<Arc<dyn MetricSource>>) -> Self {
        Self {
            sources,
            active: AtomicIsize::new(-1),
        }
    }
}

impl MetricSource for CascadeSource {
    fn query(&self) -> BoxFuture<'_, Result<Vec<Sample>, SourceError>> {
        Box::pin(async move {
            for (i, source) in self.sources.iter().enumerate() {
                let Ok(samples) = source.query().await else {
                    continue;
                };
                if samples.is_empty() {
                    continue;
                }
                let index = isize::try_from(i).unwrap_or(isize::MAX);
                let previous = self.active.swap(index, Ordering::Relaxed);
                if previous != index {
                    match (previous, i) {
                        (p, _) if p < 0 => {
                            tracing::info!(source_index = i, "metric source resolved")
                        }
                        (_, 0) => tracing::info!("primary metric source recovered"),
                        (p, _) => tracing::info!(
                            fallback_index = i,
                            previous_index = p,
                            "using fallback metric source"
                        ),
                    }
                }
                return Ok(samples);
            }
            Err(SourceError::Exhausted)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::boxed::BoxFuture;
    use crate::gate::metric::source::cascade::CascadeSource;
    use crate::gate::metric::source::{MetricSource, Sample, SourceError};

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

    #[tokio::test]
    async fn skips_errors_and_empty_readings() {
        let c = CascadeSource::new(vec![
            Arc::new(Fixed(Err(SourceError::Exhausted))),
            Arc::new(Fixed(Ok(vec![]))),
            Arc::new(Fixed(Ok(vec![0.4]))),
        ]);
        assert_eq!(c.query().await.unwrap()[0].value, 0.4);
        let dead = CascadeSource::new(vec![Arc::new(Fixed(Ok(vec![])))]);
        assert_eq!(dead.query().await, Err(SourceError::Exhausted));
    }
}
