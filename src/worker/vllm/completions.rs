//! `/v1/completions`, from `vllm/entrypoints/openai/completion/protocol.py`
//! and `serving.py`.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::api::progress::Progress;
use crate::worker::vllm::protocol::{Envelope, RequestMetrics, StopReason, Usage};
use crate::worker::vllm::{ABORT, StreamError};

/// `CompletionStreamResponse`.
#[derive(Debug, Deserialize)]
pub struct Chunk {
    pub id: String,
    pub created: u64,
    pub model: String,
    pub choices: Vec<StreamChoice>,
    pub usage: Option<Usage>,
    pub system_fingerprint: Option<String>,
    pub metrics: Option<RequestMetrics>,
}

/// `CompletionResponseStreamChoice`.
#[derive(Debug, Deserialize)]
pub struct StreamChoice {
    pub index: u32,
    pub text: String,
    pub finish_reason: Option<String>,
    pub stop_reason: Option<StopReason>,
    pub prompt_token_ids: Option<Vec<u32>>,
    pub token_ids: Option<Vec<u32>>,
}

/// `CompletionResponse`.
#[derive(Debug, Serialize)]
pub struct Response {
    pub id: String,
    pub object: &'static str,
    pub created: u64,
    pub model: String,
    pub choices: Vec<Choice>,
    pub service_tier: Option<String>,
    pub system_fingerprint: Option<String>,
    pub usage: Usage,
    pub kv_transfer_params: Option<Value>,
    pub ec_transfer_params: Option<Value>,
    pub metrics: Option<RequestMetrics>,
}

/// `CompletionResponseChoice`.
#[derive(Debug, Serialize)]
pub struct Choice {
    pub index: u32,
    pub text: String,
    pub logprobs: Option<Value>,
    pub finish_reason: Option<String>,
    pub stop_reason: Option<StopReason>,
    pub token_ids: Option<Vec<u32>>,
    pub prompt_logprobs: Option<Value>,
    pub prompt_token_ids: Option<Vec<u32>>,
    pub routed_experts: Option<String>,
}

/// Rebuilds the non-streamed response of a single-choice completion from
/// its stream, and from the output saved by earlier attempts.
#[derive(Debug, Default)]
pub struct Accumulator {
    /// Whether the caller asked for token IDs.
    caller_token_ids: bool,
    envelope: Option<Envelope>,
    text: String,
    token_ids: Vec<u32>,
    /// The original prompt. Set from progress when resuming, since the
    /// stream then reports the continuation's prompt.
    prompt_token_ids: Option<Vec<u32>>,
    /// Tokens carried in from earlier attempts, which the engine counted as
    /// prompt this time.
    saved: usize,
    /// Text and token lengths after the last single-token chunk that
    /// emitted text. The detokenizer holds back an unfinished character, so
    /// only these points split text and tokens cleanly.
    clean: (usize, usize),
    finish: Option<(String, Option<StopReason>)>,
    usage: Option<Usage>,
    system_fingerprint: Option<String>,
    metrics: Option<RequestMetrics>,
}

impl Accumulator {
    pub fn new(caller_token_ids: bool) -> Self {
        Self {
            caller_token_ids,
            ..Self::default()
        }
    }

    pub fn resume(caller_token_ids: bool, progress: &Progress) -> Self {
        let clean = (progress.text.len(), progress.token_ids.len());
        Self {
            caller_token_ids,
            text: progress.text.clone(),
            token_ids: progress.token_ids.clone(),
            prompt_token_ids: Some(progress.prompt_token_ids.clone()),
            saved: progress.token_ids.len(),
            clean,
            ..Self::default()
        }
    }

    /// The prompt, and the output up to the last clean point, once there is
    /// output and no finish reason.
    pub fn progress(&self) -> Option<(&[u32], &[u32], &str)> {
        let (text_len, token_len) = self.clean;
        if self.finish.is_some() || token_len == 0 {
            return None;
        }
        Some((
            self.prompt_token_ids.as_deref()?,
            &self.token_ids[..token_len],
            &self.text[..text_len],
        ))
    }

    pub fn push(&mut self, chunk: Chunk) -> Result<(), StreamError> {
        if self.envelope.is_none() {
            self.envelope = Some(Envelope {
                id: chunk.id,
                created: chunk.created,
                model: chunk.model,
            });
        }
        for choice in chunk.choices {
            if choice.index != 0 {
                return Err(StreamError::UnexpectedChoice(choice.index));
            }
            if self.finish.is_some() {
                return Err(StreamError::AfterFinish);
            }
            let ids = choice.token_ids.ok_or(StreamError::MissingTokenIds)?;
            if ids.len() == 1 && !choice.text.is_empty() {
                self.clean = (
                    self.text.len() + choice.text.len(),
                    self.token_ids.len() + 1,
                );
            }
            self.text.push_str(&choice.text);
            self.token_ids.extend(ids);
            if self.saved == 0
                && let Some(ids) = choice.prompt_token_ids
            {
                self.prompt_token_ids = Some(ids);
            }
            if let Some(reason) = choice.finish_reason {
                if reason == ABORT {
                    return Err(StreamError::Aborted);
                }
                self.finish = Some((reason, choice.stop_reason));
            }
        }
        if let Some(usage) = chunk.usage {
            self.usage = Some(usage);
            self.system_fingerprint = chunk.system_fingerprint;
            self.metrics = chunk.metrics;
        }
        Ok(())
    }

    pub fn finish(self) -> Result<Response, StreamError> {
        let Envelope { id, created, model } = self.envelope.ok_or(StreamError::Empty)?;
        let (finish_reason, stop_reason) = self.finish.ok_or(StreamError::Unfinished)?;
        let usage = self
            .usage
            .ok_or(StreamError::MissingUsage)?
            .count_saved_as_output(self.saved);
        let keep = self.caller_token_ids;
        Ok(Response {
            id,
            object: "text_completion",
            created,
            model,
            choices: vec![Choice {
                index: 0,
                text: self.text,
                logprobs: None,
                finish_reason: Some(finish_reason),
                stop_reason,
                token_ids: keep.then_some(self.token_ids),
                prompt_logprobs: None,
                prompt_token_ids: self.prompt_token_ids.filter(|_| keep),
                routed_experts: None,
            }],
            service_tier: None,
            system_fingerprint: self.system_fingerprint,
            usage,
            kv_transfer_params: None,
            ec_transfer_params: None,
            metrics: self.metrics,
        })
    }
}
