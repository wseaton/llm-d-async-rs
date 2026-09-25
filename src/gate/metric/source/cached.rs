use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

use crate::boxed::BoxFuture;
use crate::gate::metric::source::{MetricSource, Sample, SourceError};

type Reading = Result<Vec<Sample>, SourceError>;

/// Serves a source's last reading (success or error) for `ttl`. Concurrent
/// callers during a refresh wait for it instead of querying again.
pub struct CachedSource {
    inner: Arc<dyn MetricSource>,
    ttl: Duration,
    last: Mutex<Option<(Instant, Reading)>>,
}

impl CachedSource {
    pub fn new(inner: Arc<dyn MetricSource>, ttl: Duration) -> Self {
        Self {
            inner,
            ttl,
            last: Mutex::new(None),
        }
    }
}

impl MetricSource for CachedSource {
    fn query(&self) -> BoxFuture<'_, Reading> {
        Box::pin(async move {
            let mut last = self.last.lock().await;
            if let Some((at, reading)) = last.as_ref()
                && at.elapsed() < self.ttl
            {
                return reading.clone();
            }
            let reading = self.inner.query().await;
            *last = Some((Instant::now(), reading.clone()));
            reading
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use crate::boxed::BoxFuture;
    use crate::gate::metric::source::cached::CachedSource;
    use crate::gate::metric::source::{MetricSource, Sample, SourceError};

    #[derive(Default)]
    struct Counting(AtomicUsize);

    impl MetricSource for Counting {
        fn query(&self) -> BoxFuture<'_, Result<Vec<Sample>, SourceError>> {
            Box::pin(async move {
                let n = self.0.fetch_add(1, Ordering::SeqCst);
                Ok(vec![Sample {
                    labels: Default::default(),
                    value: n as f64,
                }])
            })
        }
    }

    #[tokio::test]
    async fn serves_the_cached_reading_until_ttl() {
        let inner = Arc::new(Counting::default());
        let cached = CachedSource::new(inner.clone(), Duration::from_millis(50));
        assert_eq!(cached.query().await.unwrap()[0].value, 0.0);
        assert_eq!(cached.query().await.unwrap()[0].value, 0.0);
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(cached.query().await.unwrap()[0].value, 1.0);
        assert_eq!(inner.0.load(Ordering::SeqCst), 2);
    }
}
