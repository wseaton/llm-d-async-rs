use reqwest::header::HeaderValue;

use crate::api::headers;
use crate::api::request::InternalRequest;
use crate::config::merge_policy::Fairness;
use crate::dispatch::message::SourceMeta;
use crate::merge::headers::Headers;

/// Longest tenant identity stamped; a longer one is skipped rather than
/// turned into a 431 at the gateway.
const MAX_IDENTITY_LEN: usize = 256;

/// Headers every policy sends: the payload's Content-Type, the queue's
/// objective, the caller's headers, then the fairness identity.
pub fn base_headers(
    source: &SourceMeta,
    envelope: &InternalRequest,
    fairness: &Fairness,
) -> Headers {
    let mut h = Headers::default();
    h.set("Content-Type", &envelope.payload.content_type);
    if !source.inference_objective.is_empty() {
        h.set(headers::GATEWAY_OBJECTIVE, &source.inference_objective);
    }
    for (k, v) in &envelope.request.headers {
        h.set(k, v);
    }
    stamp_fairness(&mut h, envelope, fairness);
    h
}

/// Replaces the fairness header with the tenant the quota gate keys on, so
/// the gateway arbitrates a tenant under the identity it is accounted under.
/// An identity that cannot be stamped leaves the caller's header alone.
fn stamp_fairness(h: &mut Headers, envelope: &InternalRequest, fairness: &Fairness) {
    let Some(header) = &fairness.header else {
        return;
    };
    let Some(id) = envelope.request.metadata.get(&fairness.attribute) else {
        return;
    };
    if id.is_empty() || id.len() > MAX_IDENTITY_LEN || HeaderValue::from_str(id).is_err() {
        return;
    }
    h.set(header.as_str(), id);
}

#[cfg(test)]
mod tests {
    use reqwest::header::HeaderName;

    use crate::config::merge_policy::Fairness;
    use crate::dispatch::message::SourceMeta;
    use crate::merge::stamp::base_headers;
    use crate::store::test_support::envelope;
    use crate::telemetry::metrics::QueueLabels;

    fn source(objective: &str) -> SourceMeta {
        SourceMeta {
            labels: QueueLabels::default(),
            igw_base_url: "http://gw".into(),
            request_path: "/v1/completions".into(),
            inference_objective: objective.into(),
        }
    }

    fn fairness(header: Option<&'static str>) -> Fairness {
        Fairness {
            header: header.map(HeaderName::from_static),
            attribute: "userid".into(),
        }
    }

    #[test]
    fn caller_headers_override_defaults_and_fairness_overrides_callers() {
        let mut env = envelope("a", "t", "q", 1);
        env.payload.content_type = "audio/wav".into();
        env.request.headers.insert("x-custom".into(), "1".into());
        env.request
            .headers
            .insert("X-LLM-D-Inference-Fairness-Id".into(), "spoofed".into());
        env.request
            .metadata
            .insert("userid".into(), "tenant-a".into());
        let h = base_headers(
            &source("obj"),
            &env,
            &fairness(Some("x-llm-d-inference-fairness-id")),
        );
        assert_eq!(h.get("content-type"), Some("audio/wav"));
        assert_eq!(h.get("x-gateway-inference-objective"), Some("obj"));
        assert_eq!(h.get("x-custom"), Some("1"));
        assert_eq!(h.get("x-llm-d-inference-fairness-id"), Some("tenant-a"));
        assert_eq!(h.to_header_map().unwrap().len(), 4);
    }

    #[test]
    fn unusable_identities_leave_the_caller_header() {
        for id in ["", "bad\nvalue", &"x".repeat(257)] {
            let mut env = envelope("a", "t", "q", 1);
            env.request
                .headers
                .insert("x-llm-d-inference-fairness-id".into(), "caller".into());
            env.request.metadata.insert("userid".into(), id.to_owned());
            let h = base_headers(
                &source(""),
                &env,
                &fairness(Some("x-llm-d-inference-fairness-id")),
            );
            assert_eq!(h.get("x-llm-d-inference-fairness-id"), Some("caller"));
            assert_eq!(h.get("x-gateway-inference-objective"), None);
        }
    }

    #[test]
    fn disabled_fairness_stamps_nothing() {
        let mut env = envelope("a", "t", "q", 1);
        env.request
            .metadata
            .insert("userid".into(), "tenant".into());
        let h = base_headers(&source(""), &env, &fairness(None));
        assert_eq!(h.get("x-llm-d-inference-fairness-id"), None);
    }
}
