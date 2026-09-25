//! Where metric gates read their numbers from.

pub mod cached;
pub mod cascade;
pub mod promql;
pub mod queries;
pub mod scrape;

use std::collections::BTreeMap;

use crate::boxed::BoxFuture;

#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    pub labels: BTreeMap<String, String>,
    pub value: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SourceError {
    #[error("request {url}: {message}")]
    Http { url: String, message: String },
    #[error("{url} returned {status}")]
    Status { url: String, status: u16 },
    #[error("prometheus query failed ({error_type}): {message}")]
    Query { error_type: String, message: String },
    #[error("parse response: {0}")]
    Parse(String),
    #[error("{0}")]
    Invalid(String),
    #[error("all metric sources unavailable")]
    Exhausted,
}

pub trait MetricSource: Send + Sync {
    fn query(&self) -> BoxFuture<'_, Result<Vec<Sample>, SourceError>>;
}

/// Parses a Prometheus sample value, including `NaN`, `+Inf` and `-Inf`.
pub fn parse_value(s: &str) -> Option<f64> {
    match s {
        "NaN" => Some(f64::NAN),
        "+Inf" | "Inf" => Some(f64::INFINITY),
        "-Inf" => Some(f64::NEG_INFINITY),
        _ => s.parse().ok(),
    }
}
