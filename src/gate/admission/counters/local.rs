use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::boxed::BoxFuture;
use crate::clock::now_millis;
use crate::gate::admission::counters::{BucketSpec, CounterError, Counters, Slot};

/// Counters for one process.
#[derive(Default)]
pub struct LocalCounters {
    slots: Arc<Mutex<HashMap<String, u32>>>,
    windows: Mutex<RateWindows>,
    buckets: Mutex<HashMap<String, Bucket>>,
}

#[derive(Default)]
struct RateWindows {
    by_key: HashMap<(String, Duration), VecDeque<Instant>>,
    last_sweep: Option<Instant>,
}

const SWEEP_EVERY: Duration = Duration::from_secs(60);

impl RateWindows {
    fn admit(&mut self, key: &str, limit: u32, window: Duration, now: Instant) -> bool {
        let prune = |q: &mut VecDeque<Instant>, window: Duration| {
            let cutoff = now.checked_sub(window);
            while q.front().is_some_and(|t| Some(*t) <= cutoff) {
                q.pop_front();
            }
        };
        if self
            .last_sweep
            .is_none_or(|at| now.duration_since(at) >= SWEEP_EVERY)
        {
            self.by_key.retain(|(_, window), q| {
                prune(q, *window);
                !q.is_empty()
            });
            self.last_sweep = Some(now);
        }
        let q = self.by_key.entry((key.to_owned(), window)).or_default();
        prune(q, window);
        if q.len() >= limit as usize {
            return false;
        }
        q.push_back(now);
        true
    }
}

#[derive(Debug, Clone, Copy)]
struct Bucket {
    tokens: f64,
    last_ms: i64,
    expires_ms: i64,
}

impl Bucket {
    /// Takes one token at `now_ms` if available.
    fn take(slot: &mut Option<Self>, spec: BucketSpec, now_ms: i64) -> bool {
        let capacity = spec.capacity.max(1.0);
        if !capacity.is_finite() || !spec.rate.is_finite() {
            return false;
        }
        let bucket = slot.get_or_insert(Self {
            tokens: capacity,
            last_ms: now_ms,
            expires_ms: 0,
        });
        if now_ms >= bucket.expires_ms {
            bucket.tokens = capacity;
            bucket.last_ms = now_ms;
        }
        let now_ms = now_ms.max(bucket.last_ms);
        let elapsed_s = (now_ms - bucket.last_ms) as f64 / 1000.0;
        bucket.tokens = (bucket.tokens + elapsed_s * spec.rate).min(capacity);
        bucket.last_ms = now_ms;
        bucket.expires_ms = spec.expires_ms;
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

impl LocalCounters {
    fn slot(&self, key: String, limit: u32) -> Option<Slot> {
        let mut slots = self.slots.lock().ok()?;
        let used = slots.entry(key.clone()).or_default();
        if *used >= limit {
            return None;
        }
        *used += 1;
        let all = Arc::clone(&self.slots);
        Some(Slot::new(move || {
            if let Ok(mut slots) = all.lock()
                && let Some(used) = slots.get_mut(&key)
            {
                *used = used.saturating_sub(1);
                if *used == 0 {
                    slots.remove(&key);
                }
            }
        }))
    }

    fn admit_at(&self, key: &str, limit: u32, window: Duration, now: Instant) -> bool {
        self.windows
            .lock()
            .is_ok_and(|mut w| w.admit(key, limit, window, now))
    }

    fn take_at(&self, key: String, spec: BucketSpec, now_ms: i64) -> bool {
        let Ok(mut buckets) = self.buckets.lock() else {
            return false;
        };
        let mut slot = buckets.remove(&key);
        let taken = Bucket::take(&mut slot, spec, now_ms);
        if let Some(bucket) = slot {
            buckets.insert(key, bucket);
        }
        taken
    }
}

impl Counters for LocalCounters {
    fn acquire_slot(
        &self,
        key: String,
        limit: u32,
    ) -> BoxFuture<'_, Result<Option<Slot>, CounterError>> {
        let slot = self.slot(key, limit);
        Box::pin(async move { Ok(slot) })
    }

    fn admit(
        &self,
        key: String,
        limit: u32,
        window: Duration,
    ) -> BoxFuture<'_, Result<bool, CounterError>> {
        let admitted = self.admit_at(&key, limit, window, Instant::now());
        Box::pin(async move { Ok(admitted) })
    }

    fn take_token(
        &self,
        key: String,
        bucket: BucketSpec,
    ) -> BoxFuture<'_, Result<bool, CounterError>> {
        let taken = self.take_at(key, bucket, now_millis());
        Box::pin(async move { Ok(taken) })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use crate::gate::admission::counters::BucketSpec;
    use crate::gate::admission::counters::conformance::counters_conformance;
    use crate::gate::admission::counters::local::LocalCounters;

    async fn fixture() -> Option<Arc<LocalCounters>> {
        Some(Arc::new(LocalCounters::default()))
    }

    counters_conformance!(fixture);

    fn spec(rate: f64, capacity: f64, until: i64) -> BucketSpec {
        BucketSpec {
            rate,
            capacity,
            expires_ms: until + 1000,
        }
    }

    #[test]
    fn rate_limit_slides() {
        let c = LocalCounters::default();
        let t0 = Instant::now();
        let w = Duration::from_secs(10);
        let admit = |at: Instant| c.admit_at("a", 2, w, at);
        assert!(admit(t0));
        assert!(admit(t0 + Duration::from_secs(5)));
        assert!(!admit(t0 + Duration::from_secs(9)));
        assert!(admit(t0 + Duration::from_secs(10)));
        assert!(!admit(t0 + Duration::from_secs(11)));
        assert!(admit(t0 + Duration::from_secs(15)));
    }

    #[test]
    fn idle_rate_windows_are_swept() {
        let c = LocalCounters::default();
        let t0 = Instant::now();
        c.admit_at("gone", 1, Duration::from_secs(10), t0);
        c.admit_at(
            "x",
            1,
            Duration::from_secs(10),
            t0 + Duration::from_secs(61),
        );
        let windows = c.windows.lock().unwrap();
        assert!(
            !windows
                .by_key
                .contains_key(&("gone".to_owned(), Duration::from_secs(10)))
        );
    }

    #[test]
    fn bucket_refills_at_the_rate_and_never_runs_backwards() {
        let c = LocalCounters::default();
        let until = 1_000_000;
        let take = |now| c.take_at("k".into(), spec(2.0, 2.0, until), now);
        assert!(take(0));
        assert!(take(0));
        assert!(!take(0));
        assert!(!take(499));
        assert!(take(510));
        assert!(!take(100));
        assert!(!take(510));
        assert!(take(1010));
        assert!(take(until + 1000));
        assert!(take(until + 1000));
    }

    #[test]
    fn small_capacities_still_allow_one_request() {
        let c = LocalCounters::default();
        let take = |now| c.take_at("k".into(), spec(0.5, 0.05, 1_000_000), now);
        assert!(take(0));
        assert!(!take(1_000));
        assert!(take(2_000));
    }

    #[test]
    fn unusable_rates_take_nothing() {
        let c = LocalCounters::default();
        assert!(!c.take_at("k".into(), spec(f64::INFINITY, 1.0, 10), 0));
        assert!(!c.take_at("k".into(), spec(1.0, f64::INFINITY, 10), 0));
    }
}
