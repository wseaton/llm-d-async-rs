use serde::{Deserialize, Serialize};

use crate::api::request::InternalRequest;

/// Why a request finished without an HTTP response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    DeadlineExceeded,
    Cancelled,
    GateDropped,
    GateError,
    InferenceError,
    InvalidRequest,
    PayloadUnavailable,
}

/// A result as delivered to consumers.
///
/// `status_code > 0` means an HTTP response was received. `payload` is its
/// body, or, when `payload_ref` is set, `payload` is empty and the body is the
/// blob `payload_ref` names. `status_code == 0` means no response;
/// `error_code` says why.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultMessage {
    pub id: String,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub status_code: u16,
    pub payload: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_code: Option<ErrorCode>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error_message: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub payload_ref: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub content_type: String,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub payload_size: u64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub payload_sha256: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub request_token: String,
}

/// A response body stored by reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredBody {
    pub payload_ref: String,
    pub content_type: String,
    pub size: u64,
    pub sha256: String,
}

fn is_zero(v: &u16) -> bool {
    *v == 0
}

fn is_zero_u64(v: &u64) -> bool {
    *v == 0
}

impl ResultMessage {
    pub fn error(req: &InternalRequest, code: ErrorCode, message: impl Into<String>) -> Self {
        let message = message.into();
        Self {
            id: req.request.id.clone(),
            status_code: 0,
            payload: serde_json::json!({ "error": message }).to_string(),
            error_code: Some(code),
            error_message: message,
            payload_ref: String::new(),
            content_type: String::new(),
            payload_size: 0,
            payload_sha256: String::new(),
            request_token: req.routing.request_token.clone(),
        }
    }

    pub fn http(req: &InternalRequest, status_code: u16, body: &[u8]) -> Self {
        Self {
            id: req.request.id.clone(),
            status_code,
            payload: String::from_utf8_lossy(body).into_owned(),
            error_code: None,
            error_message: String::new(),
            payload_ref: String::new(),
            content_type: String::new(),
            payload_size: 0,
            payload_sha256: String::new(),
            request_token: req.routing.request_token.clone(),
        }
    }

    pub fn http_by_reference(req: &InternalRequest, status_code: u16, body: StoredBody) -> Self {
        Self {
            payload_ref: body.payload_ref,
            content_type: body.content_type,
            payload_size: body.size,
            payload_sha256: body.sha256,
            ..Self::http(req, status_code, b"")
        }
    }

    pub fn deadline_exceeded(req: &InternalRequest) -> Self {
        Self::error(req, ErrorCode::DeadlineExceeded, "deadline exceeded")
    }

    pub fn cancelled(req: &InternalRequest) -> Self {
        Self::error(req, ErrorCode::Cancelled, "cancelled")
    }

    pub fn gate_dropped(req: &InternalRequest) -> Self {
        Self::error(req, ErrorCode::GateDropped, "Pool gating dropped request")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::api::payload::{PayloadInfo, PayloadStorage};
    use crate::api::request::{InternalRequest, RequestMessage};
    use crate::api::result::{ErrorCode, ResultMessage, StoredBody};
    use crate::api::routing::InternalRouting;

    fn req() -> InternalRequest {
        InternalRequest {
            routing: InternalRouting {
                request_token: "tok".into(),
                ..Default::default()
            },
            request: RequestMessage {
                id: "r1".into(),
                created: 0,
                deadline: 1,
                metadata: BTreeMap::new(),
                headers: BTreeMap::new(),
                endpoint: String::new(),
                model: String::new(),
            },
            payload: PayloadInfo {
                content_type: "application/json".into(),
                size: 0,
                storage: PayloadStorage::Inline,
            },
        }
    }

    #[test]
    fn error_result_matches_the_go_wire_format() {
        let r = ResultMessage::error(&req(), ErrorCode::GateError, "boom \"x\"");
        let v: serde_json::Value = serde_json::to_value(&r).unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "id": "r1",
                "payload": "{\"error\":\"boom \\\"x\\\"\"}",
                "error_code": "GATE_ERROR",
                "error_message": "boom \"x\"",
                "request_token": "tok",
            })
        );
    }

    #[test]
    fn http_result_omits_error_fields() {
        let r = ResultMessage::http(&req(), 503, b"busy");
        let v: serde_json::Value = serde_json::to_value(&r).unwrap();
        assert_eq!(
            v,
            serde_json::json!({"id":"r1","status_code":503,"payload":"busy","request_token":"tok"})
        );
    }

    #[test]
    fn stored_body_result_matches_the_go_wire_format() {
        let r = ResultMessage::http_by_reference(
            &req(),
            200,
            StoredBody {
                payload_ref: "blob://results/tok".into(),
                content_type: "audio/wav".into(),
                size: 42,
                sha256: "ab".into(),
            },
        );
        let v: serde_json::Value = serde_json::to_value(&r).unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "id": "r1",
                "status_code": 200,
                "payload": "",
                "payload_ref": "blob://results/tok",
                "content_type": "audio/wav",
                "payload_size": 42,
                "payload_sha256": "ab",
                "request_token": "tok",
            })
        );
    }

    #[test]
    fn invalid_utf8_body_is_replaced_not_dropped() {
        let r = ResultMessage::http(&req(), 200, &[0x66, 0xff]);
        assert_eq!(r.payload, "f\u{fffd}");
    }
}
