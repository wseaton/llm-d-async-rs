//! The request envelope as upstream llm-d-async stores it in a sorted set:
//!
//! ```text
//! {"internal": {routing}, "request_kind": "redis", "data": {id, created, deadline, payload, ...}}
//! ```
//!
//! A JSON payload travels inline in `data.payload`, byte for byte. A payload
//! elsewhere is named by `internal.payload_ref`: `request-payload:<id>:<token>`
//! (llm-d-async#458) or one of this processor's blobs, `blob://requests/<token>`,
//! whose content type and size ride in `internal` too. Go dispatchers ignore
//! those two fields and drop them if they re-serialize the envelope.

use std::collections::BTreeMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::api::payload::{JSON_CONTENT_TYPE, PayloadInfo, PayloadStorage};
use crate::api::request::{InternalRequest, RequestMessage};
use crate::api::routing::InternalRouting;
use crate::store::blob::key::BlobKey;
use crate::store::redis::keys;

const REQUEST_KIND: &str = "redis";
const BLOB_CONTENT_TYPE: &str = "application/octet-stream";

#[derive(Serialize, Deserialize)]
struct Member {
    internal: Internal,
    request_kind: String,
    data: Data,
}

#[derive(Default, Serialize, Deserialize)]
struct Internal {
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    retry_count: u32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    queue_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    request_token: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    request_queue_name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    result_queue_name: String,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    result_ttl_seconds: u64,
    #[serde(default, skip_serializing_if = "is_false")]
    result_routing_resolved: bool,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    labels: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    payload_ref: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    payload_content_type: String,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    payload_size: u64,
}

#[derive(Serialize, Deserialize)]
struct Data {
    id: String,
    #[serde(default)]
    created: i64,
    #[serde(default)]
    deadline: i64,
    #[serde(default)]
    payload: Option<Box<RawValue>>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    metadata: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    headers: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    endpoint: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    model: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    request_queue_name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    result_queue_name: String,
}

fn is_zero_u32(v: &u32) -> bool {
    *v == 0
}

fn is_zero_u64(v: &u64) -> bool {
    *v == 0
}

fn is_false(v: &bool) -> bool {
    !*v
}

/// Where a request's body is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Body {
    /// JSON inside the envelope, as it arrived.
    Inline(Bytes),
    /// A `request-payload:` key.
    Key(String),
    /// One of this processor's blobs.
    Blob,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decoded {
    pub envelope: InternalRequest,
    pub body: Body,
}

#[derive(Debug, thiserror::Error)]
pub enum WireError {
    #[error("undecodable envelope: {0}")]
    Json(#[from] serde_json::Error),
    #[error("envelope has no request ID")]
    NoId,
    #[error("envelope has no deadline")]
    NoDeadline,
    #[error("payload reference {0:?} names no payload this store can read")]
    UnknownRef(String),
}

/// Decodes a sorted-set member. The envelope's `progress` is left empty; it
/// lives in its own key.
pub fn decode(member: &str) -> Result<Decoded, WireError> {
    let m: Member = serde_json::from_str(member)?;
    if m.data.id.is_empty() {
        return Err(WireError::NoId);
    }
    if m.data.deadline <= 0 {
        return Err(WireError::NoDeadline);
    }
    let i = m.internal;
    let content_type = |fallback: &str| {
        if i.payload_content_type.is_empty() {
            fallback.to_owned()
        } else {
            i.payload_content_type.clone()
        }
    };
    let (body, payload) = if i.payload_ref.is_empty() {
        let raw = m.data.payload.map_or_else(
            || Bytes::from_static(b"null"),
            |p| Bytes::from(p.get().to_owned()),
        );
        let info = PayloadInfo {
            content_type: content_type(JSON_CONTENT_TYPE),
            size: raw.len() as u64,
            storage: PayloadStorage::Inline,
        };
        (Body::Inline(raw), info)
    } else if keys::is_payload_key(&i.payload_ref) {
        let info = PayloadInfo {
            content_type: content_type(JSON_CONTENT_TYPE),
            size: i.payload_size,
            storage: PayloadStorage::Inline,
        };
        (Body::Key(i.payload_ref.clone()), info)
    } else if BlobKey::from_ref(&i.payload_ref)
        .is_some_and(|k| Some(k) == BlobKey::request(&i.request_token))
    {
        let info = PayloadInfo {
            content_type: content_type(BLOB_CONTENT_TYPE),
            size: i.payload_size,
            storage: PayloadStorage::Blob,
        };
        (Body::Blob, info)
    } else {
        return Err(WireError::UnknownRef(i.payload_ref));
    };
    let pick = |internal: String, data: String| if internal.is_empty() { data } else { internal };
    let envelope = InternalRequest {
        routing: InternalRouting {
            retry_count: i.retry_count,
            queue_id: i.queue_id,
            request_token: i.request_token,
            request_queue_name: pick(i.request_queue_name, m.data.request_queue_name),
            result_queue_name: pick(i.result_queue_name, m.data.result_queue_name),
            result_ttl_seconds: i.result_ttl_seconds,
            result_routing_resolved: i.result_routing_resolved,
            labels: i.labels,
        },
        request: RequestMessage {
            id: m.data.id,
            created: m.data.created,
            deadline: m.data.deadline,
            metadata: m.data.metadata,
            headers: m.data.headers,
            endpoint: m.data.endpoint,
            model: m.data.model,
        },
        payload,
        progress: None,
    };
    Ok(Decoded { envelope, body })
}

/// Encodes `envelope` with its body as a sorted-set member. An inline body
/// must be JSON.
pub fn encode(envelope: &InternalRequest, body: &Body) -> Result<String, WireError> {
    let r = &envelope.routing;
    let mut internal = Internal {
        retry_count: r.retry_count,
        queue_id: r.queue_id.clone(),
        request_token: r.request_token.clone(),
        request_queue_name: r.request_queue_name.clone(),
        result_queue_name: r.result_queue_name.clone(),
        result_ttl_seconds: r.result_ttl_seconds,
        result_routing_resolved: r.result_routing_resolved,
        labels: r.labels.clone(),
        ..Internal::default()
    };
    if envelope.payload.content_type != JSON_CONTENT_TYPE {
        internal.payload_content_type = envelope.payload.content_type.clone();
    }
    let payload = match body {
        Body::Inline(raw) => Some(RawValue::from_string(
            String::from_utf8(raw.to_vec()).map_err(|e| {
                WireError::Json(serde::de::Error::custom(format!(
                    "payload is not UTF-8: {e}"
                )))
            })?,
        )?),
        Body::Key(key) => {
            internal.payload_ref = key.clone();
            internal.payload_size = envelope.payload.size;
            None
        }
        Body::Blob => {
            internal.payload_ref = BlobKey::request(&r.request_token)
                .ok_or_else(|| WireError::UnknownRef(r.request_token.clone()))?
                .to_ref();
            internal.payload_size = envelope.payload.size;
            None
        }
    };
    let q = &envelope.request;
    let member = Member {
        internal,
        request_kind: REQUEST_KIND.to_owned(),
        data: Data {
            id: q.id.clone(),
            created: q.created,
            deadline: q.deadline,
            payload,
            metadata: q.metadata.clone(),
            headers: q.headers.clone(),
            endpoint: q.endpoint.clone(),
            model: q.model.clone(),
            request_queue_name: r.request_queue_name.clone(),
            result_queue_name: r.result_queue_name.clone(),
        },
    };
    Ok(serde_json::to_string(&member)?)
}

/// Whether `bytes` can travel inline: a JSON document.
pub fn is_json(bytes: &[u8]) -> bool {
    serde_json::from_slice::<serde::de::IgnoredAny>(bytes).is_ok()
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::api::payload::PayloadStorage;
    use crate::store::redis::wire::{Body, WireError, decode, encode, is_json};
    use crate::store::test_support::envelope;

    /// What upstream's Go producer writes, field for field.
    const GO_MEMBER: &str = r#"{"internal":{"request_token":"9f86d081884c7d659a2feaa0c55ad015","request_queue_name":"request-sortedset","result_queue_name":"results:req:acme:job-1"},"request_kind":"redis","data":{"id":"acme:job-1","created":1790000000,"deadline":1790003600,"payload":{"model":"m","prompt":"hi","max_tokens":8},"metadata":{"userid":"acme"},"endpoint":"/v1/completions","request_queue_name":"request-sortedset","result_queue_name":"results:req:acme:job-1"}}"#;

    #[test]
    fn decodes_what_the_go_producer_writes() {
        let d = decode(GO_MEMBER).unwrap();
        let e = &d.envelope;
        assert_eq!(e.request.id, "acme:job-1");
        assert_eq!(e.request.deadline, 1_790_003_600);
        assert_eq!(e.request.metadata["userid"], "acme");
        assert_eq!(e.routing.request_token, "9f86d081884c7d659a2feaa0c55ad015");
        assert_eq!(e.routing.request_queue_name, "request-sortedset");
        assert_eq!(e.routing.result_queue_name, "results:req:acme:job-1");
        assert_eq!(e.payload.storage, PayloadStorage::Inline);
        assert_eq!(e.payload.content_type, "application/json");
        assert_eq!(
            d.body,
            Body::Inline(Bytes::from_static(
                br#"{"model":"m","prompt":"hi","max_tokens":8}"#
            ))
        );
    }

    #[test]
    fn round_trips_and_keeps_the_payload_verbatim() {
        let mut env = envelope("a", "0a", "q", 100);
        env.routing.retry_count = 2;
        env.routing.labels.insert("tier".into(), "batch".into());
        let raw = Bytes::from_static(br#"{"z": 1,  "a": [1.50, 2]}"#);
        env.payload.size = raw.len() as u64;
        let member = encode(&env, &Body::Inline(raw.clone())).unwrap();
        assert!(member.starts_with(r#"{"internal":{"retry_count":2,"request_token":"0a""#));
        let d = decode(&member).unwrap();
        assert_eq!(d.body, Body::Inline(raw));
        assert_eq!(d.envelope, env);
    }

    #[test]
    fn blob_and_key_payloads_are_referenced() {
        let mut env = envelope("a", "0a", "q", 100);
        env.payload.storage = PayloadStorage::Blob;
        env.payload.content_type = "audio/wav".into();
        env.payload.size = 20;
        let member = encode(&env, &Body::Blob).unwrap();
        assert!(member.contains(r#""payload_ref":"blob://requests/0a""#));
        assert!(member.contains(r#""payload":null"#));
        let d = decode(&member).unwrap();
        assert_eq!((d.body, d.envelope), (Body::Blob, env));

        let pr458 = GO_MEMBER.replace(
            r#""result_queue_name":"results:req:acme:job-1"},"request_kind""#,
            r#""result_queue_name":"results:req:acme:job-1","payload_ref":"request-payload:acme:job-1:9f86"},"request_kind""#,
        );
        let d = decode(&pr458).unwrap();
        assert_eq!(d.body, Body::Key("request-payload:acme:job-1:9f86".into()));
        assert_eq!(d.envelope.payload.storage, PayloadStorage::Inline);
    }

    #[test]
    fn rejects_what_upstream_drops() {
        assert!(matches!(decode("garbage"), Err(WireError::Json(_))));
        assert!(matches!(
            decode(r#"{"internal":{},"request_kind":"redis","data":{"id":"","deadline":5}}"#),
            Err(WireError::NoId)
        ));
        assert!(matches!(
            decode(r#"{"internal":{},"request_kind":"redis","data":{"id":"a"}}"#),
            Err(WireError::NoDeadline)
        ));
        assert!(matches!(
            decode(
                r#"{"internal":{"payload_ref":"s3://x"},"request_kind":"redis","data":{"id":"a","deadline":5}}"#
            ),
            Err(WireError::UnknownRef(_))
        ));
    }

    #[test]
    fn only_json_travels_inline() {
        assert!(is_json(br#"{"a":1}"#));
        assert!(is_json(b"[1, 2]"));
        assert!(!is_json(b"plain text"));
        let env = envelope("a", "0a", "q", 100);
        assert!(encode(&env, &Body::Inline(Bytes::from_static(b"\xff"))).is_err());
    }
}
