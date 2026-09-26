//! vLLM's error body (`vllm/entrypoints/serve/engine/protocol.py`), which a
//! failed request returns and an error event in a stream carries.

use serde::{Deserialize, Serialize};

/// `ErrorResponse`: the body of a failed request, and the payload of an
/// error event in a stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: ErrorInfo,
}

/// `ErrorInfo`. `code` is the HTTP status the request fails with.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorInfo {
    pub message: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub param: Option<String>,
    pub code: u16,
}
