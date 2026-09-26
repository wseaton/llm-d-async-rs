use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;

use crate::api::request::InternalRequest;
use crate::config::transport::EppProfile;
use crate::dispatch::claim::ClaimGuard;
use crate::gate::release::Releases;
use crate::merge::headers::Headers;
use crate::telemetry::metrics::QueueLabels;

/// A request a consumer claimed, on its way to the merge policy.
pub struct Claimed {
    pub envelope: InternalRequest,
    pub guard: ClaimGuard,
    /// Queue-gate reservations, held until the request finishes or requeues.
    pub releases: Releases,
    pub ingested: Instant,
    /// The request body, when it is stored inline and came with the claim.
    pub payload: Option<Bytes>,
}

/// Where a queue's requests go.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceMeta {
    pub labels: QueueLabels,
    pub igw_base_url: String,
    pub request_path: String,
    pub inference_objective: String,
    pub resume: Option<ResumeTarget>,
}

/// Where a resumable queue renders, and how it sends generate requests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumeTarget {
    /// A vLLM serving the queue's model with `--enable-scale-out`.
    pub render_url: String,
    pub generate_epp_profile: Option<EppProfile>,
}

impl SourceMeta {
    /// The queue's base URL joined with the request's endpoint, or the
    /// queue's default path.
    pub fn url_for(&self, envelope: &InternalRequest) -> String {
        let path = if envelope.request.endpoint.is_empty() {
            &self.request_path
        } else {
            &envelope.request.endpoint
        };
        format!(
            "{}/{}",
            self.igw_base_url.trim_end_matches('/'),
            path.trim_start_matches('/')
        )
    }
}

/// A request ready for a worker.
pub struct Dispatch {
    pub claimed: Claimed,
    pub source: Arc<SourceMeta>,
    pub url: String,
    pub headers: Headers,
}

#[cfg(test)]
mod tests {
    use crate::dispatch::message::SourceMeta;
    use crate::store::test_support::envelope;
    use crate::telemetry::metrics::QueueLabels;

    #[test]
    fn url_joins_base_and_path() {
        let meta = SourceMeta {
            labels: QueueLabels::default(),
            igw_base_url: "http://gw:8000/".into(),
            request_path: "/v1/completions".into(),
            inference_objective: String::new(),
            resume: None,
        };
        let mut env = envelope("a", "t", "q", 1);
        assert_eq!(meta.url_for(&env), "http://gw:8000/v1/completions");
        env.request.endpoint = "v1/chat/completions".into();
        assert_eq!(meta.url_for(&env), "http://gw:8000/v1/chat/completions");
    }
}
