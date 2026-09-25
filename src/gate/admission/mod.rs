//! Per-request admission gates.

pub mod budget_key;
pub mod counters;
pub mod leased_rate;
pub mod local_concurrency;
pub mod quota;

/// What an over-limit admission gate does with a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatingMode {
    /// Refuse it.
    Blocking,
    /// Admit it, classified as overflow, for downstream lanes to rank.
    Classifying,
}

impl GatingMode {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "blocking" => Some(Self::Blocking),
            "classifying" => Some(Self::Classifying),
            _ => None,
        }
    }
}
