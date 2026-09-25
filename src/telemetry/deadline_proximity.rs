use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use prometheus::core::{Collector, Desc};
use prometheus::proto::{Bucket, Histogram, LabelPair, Metric, MetricFamily, MetricType};

use crate::telemetry::metrics::{QUEUE_LABELS, QueueLabels};

pub const NAME: &str = "llm_d_async_async_deadline_proximity_millis";

/// Bucket upper bounds in seconds, most urgent first. The 0 bound holds
/// requests already past their deadline but still queued.
pub const BOUNDS_SECS: [i64; 14] = [
    0, 1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600, 7200, 21_600, 86_400,
];

/// Snapshot histogram of time-to-deadline for queued requests.
///
/// Bucket counts are exact store counts, not observations, so this cannot be
/// a regular `HistogramVec`: every poll replaces the snapshot. `rate()` on it
/// is meaningless; use `histogram_quantile` per scrape. `_sum` is estimated
/// from bucket midpoints.
#[derive(Clone)]
pub struct DeadlineProximity {
    desc: Desc,
    series: Arc<Mutex<BTreeMap<QueueLabels, [u64; BOUNDS_SECS.len()]>>>,
}

impl DeadlineProximity {
    pub fn new() -> Result<Self, prometheus::Error> {
        let desc = Desc::new(
            NAME.to_owned(),
            "Time remaining until deadline (ms) for items still queued, as a per-poll snapshot \
             histogram of exact bucket counts (le=\"0\" holds items past their deadline but still \
             queued). Not monotonic; use histogram_quantile per scrape."
                .to_owned(),
            QUEUE_LABELS.iter().map(|l| (*l).to_owned()).collect(),
            HashMap::new(),
        )?;
        Ok(Self {
            desc,
            series: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    /// Replaces a queue's snapshot. `cumulative` is aligned with [`BOUNDS_SECS`].
    pub fn set(&self, labels: &QueueLabels, cumulative: &[u64]) {
        let Ok(counts) = <[u64; BOUNDS_SECS.len()]>::try_from(cumulative) else {
            return;
        };
        if let Ok(mut series) = self.series.lock() {
            series.insert(labels.clone(), counts);
        }
    }

    pub fn remove(&self, labels: &QueueLabels) {
        if let Ok(mut series) = self.series.lock() {
            series.remove(labels);
        }
    }
}

fn bound_millis(i: usize) -> f64 {
    (BOUNDS_SECS[i] * 1000) as f64
}

fn estimated_sum(cumulative: &[u64]) -> f64 {
    let mut sum = 0.0;
    let (mut prev_bound, mut prev_count) = (0.0, 0u64);
    for (i, count) in cumulative.iter().enumerate() {
        let bound = bound_millis(i);
        let in_bucket = count.saturating_sub(prev_count);
        sum += in_bucket as f64 * (prev_bound + bound) / 2.0;
        prev_bound = bound;
        prev_count = *count;
    }
    sum
}

impl Collector for DeadlineProximity {
    fn desc(&self) -> Vec<&Desc> {
        vec![&self.desc]
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let Ok(series) = self.series.lock() else {
            return Vec::new();
        };
        let metrics = series
            .iter()
            .map(|(labels, cumulative)| {
                let mut histogram = Histogram::default();
                histogram.set_sample_count(cumulative.last().copied().unwrap_or(0));
                histogram.set_sample_sum(estimated_sum(cumulative));
                histogram.set_bucket(
                    cumulative
                        .iter()
                        .enumerate()
                        .map(|(i, c)| {
                            let mut b = Bucket::default();
                            b.set_upper_bound(bound_millis(i));
                            b.set_cumulative_count(*c);
                            b
                        })
                        .collect(),
                );
                let mut metric = Metric::from_label(
                    QUEUE_LABELS
                        .iter()
                        .zip(labels.values())
                        .map(|(name, value)| {
                            let mut pair = LabelPair::default();
                            pair.set_name((*name).to_owned());
                            pair.set_value(value.to_owned());
                            pair
                        })
                        .collect(),
                );
                metric.set_histogram(histogram);
                metric
            })
            .collect();
        let mut family = MetricFamily::default();
        family.set_name(NAME.to_owned());
        family.set_help(self.desc.help.clone());
        family.set_field_type(MetricType::HISTOGRAM);
        family.set_metric(metrics);
        vec![family]
    }
}

#[cfg(test)]
mod tests {
    use crate::telemetry::deadline_proximity::estimated_sum;

    #[test]
    fn sum_uses_bucket_midpoints() {
        // 2 expired (count as 0), 1 in (0, 1s], 1 in (1s, 5s].
        let mut cumulative = [4u64; 14];
        cumulative[0] = 2;
        cumulative[1] = 3;
        assert_eq!(estimated_sum(&cumulative), 500.0 + 3000.0);
    }
}
