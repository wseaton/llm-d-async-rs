use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub const LABEL_CLASSIFICATION: &str = "classification";

/// Result routes starting with this are the processor's own: producers and
/// queue configs cannot name them.
pub const RESERVED_ROUTE_PREFIX: char = '@';

const REQUEST_ROUTE_PREFIX: &str = "@request-";

/// The result route of request `id`, delivered by request. Such requests
/// need IDs no other submission uses, or their results share a route.
pub fn request_route(id: &str) -> String {
    format!("{REQUEST_ROUTE_PREFIX}{id}")
}

pub fn is_request_route(route: &str) -> bool {
    route.starts_with(REQUEST_ROUTE_PREFIX)
}

/// Quota classification a gate stamps on a message in classifying mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Classification {
    Reserved,
    Overflow,
}

impl Classification {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Reserved => "reserved",
            Self::Overflow => "overflow",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "reserved" => Some(Self::Reserved),
            "overflow" => Some(Self::Overflow),
            _ => None,
        }
    }
}

/// SLA tier a queue declares through its labels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    Interactive,
    Async,
    Batch,
}

impl Tier {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Interactive => "interactive",
            Self::Async => "async",
            Self::Batch => "batch",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "interactive" => Some(Self::Interactive),
            "async" => Some(Self::Async),
            "batch" => Some(Self::Batch),
            _ => None,
        }
    }
}

/// Resolved routing owned by the processor, never by callers.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InternalRouting {
    #[serde(default)]
    pub retry_count: u32,
    #[serde(default)]
    pub queue_id: String,
    #[serde(default)]
    pub request_token: String,
    #[serde(default)]
    pub request_queue_name: String,
    #[serde(default)]
    pub result_queue_name: String,
    #[serde(default)]
    pub result_ttl_seconds: u64,
    #[serde(default)]
    pub result_routing_resolved: bool,
    /// Per-message labels seeded from the queue config; gates may add to it.
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

impl InternalRouting {
    pub fn set_classification(&mut self, classification: Option<Classification>) {
        match classification {
            Some(c) => {
                self.labels
                    .insert(LABEL_CLASSIFICATION.to_owned(), c.as_str().to_owned());
            }
            None => {
                self.labels.remove(LABEL_CLASSIFICATION);
            }
        }
    }

    pub fn classification(&self) -> Option<Classification> {
        self.labels
            .get(LABEL_CLASSIFICATION)
            .and_then(|v| Classification::parse(v))
    }

    /// The tier under `tier_label`, falling back to batch when missing or unknown.
    pub fn tier(&self, tier_label: &str) -> Tier {
        self.labels
            .get(tier_label)
            .and_then(|v| Tier::parse(v))
            .unwrap_or(Tier::Batch)
    }
}

#[cfg(test)]
mod tests {
    use crate::api::routing::{Classification, InternalRouting, Tier};

    #[test]
    fn classification_round_trips_through_labels() {
        let mut r = InternalRouting::default();
        assert_eq!(r.classification(), None);
        r.set_classification(Some(Classification::Overflow));
        assert_eq!(r.labels["classification"], "overflow");
        assert_eq!(r.classification(), Some(Classification::Overflow));
        r.set_classification(None);
        assert!(r.labels.is_empty());
    }

    #[test]
    fn unknown_classification_reads_as_none() {
        let mut r = InternalRouting::default();
        r.labels.insert("classification".into(), "vip".into());
        assert_eq!(r.classification(), None);
    }

    #[test]
    fn tier_defaults_to_batch() {
        let mut r = InternalRouting::default();
        assert_eq!(r.tier("tier"), Tier::Batch);
        r.labels.insert("tier".into(), "gold".into());
        assert_eq!(r.tier("tier"), Tier::Batch);
        r.labels.insert("sla".into(), "interactive".into());
        assert_eq!(r.tier("sla"), Tier::Interactive);
    }
}
