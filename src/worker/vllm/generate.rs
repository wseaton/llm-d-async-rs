//! `/inference/v1/generate`, vLLM's token-in, token-out endpoint, from
//! `vllm/entrypoints/scale_out/token_in_token_out/protocol.py`.

use serde::Deserialize;
use serde_json::{Map, Value};

use crate::worker::vllm::{ABORT, StreamError};

/// `GenerateStreamResponse`.
#[derive(Debug, Deserialize)]
pub struct Chunk {
    pub choices: Vec<StreamChoice>,
    pub usage: Option<Usage>,
}

/// `UsageInfo`, of which only the prompt token details are kept: vLLM
/// reports them with `--enable-prompt-tokens-details`, and derender cannot.
#[derive(Debug, Deserialize)]
pub struct Usage {
    #[serde(default)]
    pub prompt_tokens_details: Option<Map<String, Value>>,
}

/// `GenerateResponseStreamChoice`.
#[derive(Debug, Deserialize)]
pub struct StreamChoice {
    pub index: u32,
    pub finish_reason: Option<String>,
    pub token_ids: Option<Vec<u32>>,
}

/// A finished generation: the rendered prompt and every output token, saved
/// ones included.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Generated {
    pub prompt_token_ids: Vec<u32>,
    pub token_ids: Vec<u32>,
    pub finish_reason: String,
    /// The last attempt's prompt token details, when vLLM reported them.
    pub prompt_tokens_details: Option<Map<String, Value>>,
}

/// Collects a generate stream onto the output saved before it.
#[derive(Debug)]
pub struct Accumulator {
    prompt_token_ids: Vec<u32>,
    token_ids: Vec<u32>,
    finish_reason: Option<String>,
    usage: bool,
    prompt_tokens_details: Option<Map<String, Value>>,
}

impl Accumulator {
    pub fn new(prompt_token_ids: Vec<u32>, saved: Vec<u32>) -> Self {
        Self {
            prompt_token_ids,
            token_ids: saved,
            finish_reason: None,
            usage: false,
            prompt_tokens_details: None,
        }
    }

    pub fn finished(generated: Generated) -> Self {
        Self {
            prompt_token_ids: generated.prompt_token_ids,
            token_ids: generated.token_ids,
            finish_reason: Some(generated.finish_reason),
            usage: true,
            prompt_tokens_details: generated.prompt_tokens_details,
        }
    }

    /// The prompt, the output so far, and the finish reason once there is one.
    pub fn progress(&self) -> (&[u32], &[u32], Option<&str>) {
        (
            &self.prompt_token_ids,
            &self.token_ids,
            self.finish_reason.as_deref(),
        )
    }

    pub fn push(&mut self, chunk: Chunk) -> Result<(), StreamError> {
        for choice in chunk.choices {
            if choice.index != 0 {
                return Err(StreamError::UnexpectedChoice(choice.index));
            }
            if self.finish_reason.is_some() {
                return Err(StreamError::AfterFinish);
            }
            self.token_ids
                .extend(choice.token_ids.ok_or(StreamError::MissingTokenIds)?);
            if let Some(reason) = choice.finish_reason {
                if reason == ABORT {
                    return Err(StreamError::Aborted);
                }
                self.finish_reason = Some(reason);
            }
        }
        if let Some(usage) = chunk.usage {
            self.usage = true;
            self.prompt_tokens_details = usage.prompt_tokens_details;
        }
        Ok(())
    }

    pub fn finish(&self) -> Result<Generated, StreamError> {
        let finish_reason = self.finish_reason.clone().ok_or(StreamError::Unfinished)?;
        if !self.usage {
            return Err(StreamError::MissingUsage);
        }
        Ok(Generated {
            prompt_token_ids: self.prompt_token_ids.clone(),
            token_ids: self.token_ids.clone(),
            finish_reason,
            prompt_tokens_details: self.prompt_tokens_details.clone(),
        })
    }
}
