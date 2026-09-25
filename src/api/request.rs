use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::api::payload::PayloadInfo;
use crate::api::routing::InternalRouting;

/// Caller-visible request fields, stored without the payload.
///
/// Metadata is opaque pass-through data. It is read only where a feature is
/// configured to key on a named attribute (quota gates and fairness stamping).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestMessage {
    pub id: String,
    /// Unix seconds.
    #[serde(default)]
    pub created: i64,
    /// Unix seconds.
    pub deadline: i64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,
    /// Per-request path overriding the queue's `request_path_url`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub endpoint: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub model: String,
}

impl RequestMessage {
    /// Whether the deadline has passed. The deadline is the start of its
    /// second, the same instant the request's HTTP timeout uses.
    pub fn expired_at(&self, now_ms: i64) -> bool {
        now_ms >= self.deadline.saturating_mul(1000)
    }

    /// Whole seconds left before the deadline, at most 0 once it passed.
    pub fn secs_left(&self, now_ms: i64) -> i64 {
        self.deadline
            .saturating_mul(1000)
            .saturating_sub(now_ms)
            .div_euclid(1000)
    }
}

/// The persisted envelope: routing plus the caller's request, minus payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InternalRequest {
    pub routing: InternalRouting,
    pub request: RequestMessage,
    pub payload: PayloadInfo,
}

impl InternalRequest {
    /// Store key for one generation of a request. IDs may be reused across
    /// submissions; the token makes each submission distinct.
    pub fn generation_key(&self) -> String {
        generation_key(&self.request.id, &self.routing.request_token)
    }
}

pub fn generation_key(id: &str, token: &str) -> String {
    format!("{id}\u{0}{token}")
}

/// A submission: the JSON body of `POST /v1/requests`, or the `request` part
/// of a multipart submission (whose `payload` part carries the body, so this
/// field must then be absent). Unknown fields are ignored so producers written
/// against the Redis wire format keep working.
#[derive(Debug, Clone, Deserialize)]
pub struct SubmitRequest {
    pub id: String,
    #[serde(default)]
    pub created: i64,
    pub deadline: i64,
    /// Inline JSON payload. Its bytes are kept verbatim.
    #[serde(default)]
    pub payload: Option<Box<RawValue>>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub model: String,
    #[serde(default)]
    pub request_queue_name: String,
    #[serde(default)]
    pub result_queue_name: String,
}

impl SubmitRequest {
    pub fn message(&self) -> RequestMessage {
        RequestMessage {
            id: self.id.clone(),
            created: self.created,
            deadline: self.deadline,
            metadata: self.metadata.clone(),
            headers: self.headers.clone(),
            endpoint: self.endpoint.clone(),
            model: self.model.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::api::request::{SubmitRequest, generation_key};

    #[test]
    fn submit_request_keeps_payload_bytes_verbatim() {
        let body = r#"{"id":"a","deadline":10,"payload":{"b": [1, 2]},"pubsub_id":"ignored"}"#;
        let req: SubmitRequest = serde_json::from_str(body).unwrap();
        assert_eq!(req.payload.unwrap().get(), r#"{"b": [1, 2]}"#);
        assert_eq!(req.created, 0);
        assert!(req.request_queue_name.is_empty());
    }

    #[test]
    fn null_payload_is_absent() {
        let req: SubmitRequest =
            serde_json::from_str(r#"{"id":"a","deadline":10,"payload":null}"#).unwrap();
        assert!(req.payload.is_none());
    }

    #[test]
    fn missing_deadline_is_rejected() {
        assert!(serde_json::from_str::<SubmitRequest>(r#"{"id":"a"}"#).is_err());
    }

    #[test]
    fn deadline_is_the_start_of_its_second() {
        let req: SubmitRequest = serde_json::from_str(r#"{"id":"a","deadline":10}"#).unwrap();
        let m = req.message();
        assert!(!m.expired_at(9_999));
        assert!(m.expired_at(10_000));
        assert!(m.expired_at(10_500));
        assert_eq!(m.secs_left(8_500), 1);
        assert_eq!(m.secs_left(9_001), 0);
        assert!(m.secs_left(10_001) < 0);
    }

    #[test]
    fn generation_keys_differ_by_token() {
        assert_ne!(generation_key("a", "1"), generation_key("a", "2"));
        assert_ne!(generation_key("ab", "1"), generation_key("a", "b1"));
    }
}
