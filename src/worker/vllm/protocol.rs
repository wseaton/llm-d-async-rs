//! Types shared by vLLM's completions and chat endpoints, as Python vLLM
//! serializes them (`vllm/entrypoints/serve/engine/protocol.py`,
//! `vllm/entrypoints/generate/base/protocol.py`). Stream chunks omit null
//! fields; non-streamed responses write them as `null`, so every optional
//! field deserializes from absent and serializes as `null`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The fields every stream chunk and response starts with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    pub id: String,
    pub created: u64,
    pub model: String,
}

/// `UsageInfo`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Usage {
    #[serde(default)]
    pub prompt_tokens: u64,
    #[serde(default)]
    pub total_tokens: u64,
    pub completion_tokens: Option<u64>,
    pub prompt_tokens_details: Option<PromptTokenUsage>,
    pub completion_tokens_details: Option<CompletionTokenUsage>,
}

impl Usage {
    /// The usage of a continuation whose prompt carried `saved` output
    /// tokens, as the original request would have reported it.
    pub fn count_saved_as_output(mut self, saved: usize) -> Self {
        if saved == 0 {
            return self;
        }
        let saved = saved as u64;
        self.prompt_tokens = self.prompt_tokens.saturating_sub(saved);
        self.completion_tokens = Some(self.completion_tokens.unwrap_or(0) + saved);
        if let Some(cached) = self
            .prompt_tokens_details
            .as_mut()
            .and_then(|d| d.cached_tokens.as_mut())
        {
            *cached = (*cached).min(self.prompt_tokens);
        }
        self
    }
}

/// `PromptTokenUsageInfo`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromptTokenUsage {
    pub cached_tokens: Option<u64>,
    pub created_cache_tokens: Option<u64>,
    pub multimodal_tokens: Option<BTreeMap<String, u64>>,
}

/// `CompletionTokenUsageInfo`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompletionTokenUsage {
    #[serde(default)]
    pub reasoning_tokens: u64,
}

/// `PerRequestMetrics`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RequestMetrics {
    pub time_to_first_token_ms: Option<f64>,
    pub generation_time_ms: Option<f64>,
    pub queue_time_ms: Option<f64>,
    pub mean_itl_ms: Option<f64>,
    pub tokens_per_second: Option<f64>,
    pub speculative_decoding: Option<SpeculativeDecodingMetrics>,
}

/// `SpeculativeDecodingMetrics`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpeculativeDecodingMetrics {
    pub mean_acceptance_length: f64,
    pub draft_acceptance_rate: f64,
    pub acceptance_histogram: Vec<u64>,
    pub num_spec_steps: u64,
    pub num_accepted_draft_tokens: u64,
}

/// The stop string or stop token that ended a generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StopReason {
    Token(u64),
    Text(String),
}

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
