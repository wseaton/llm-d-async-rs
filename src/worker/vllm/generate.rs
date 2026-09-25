//! `/inference/v1/generate`, vLLM's token-in, token-out endpoint, from
//! `vllm/entrypoints/scale_out/token_in_token_out/protocol.py`. A resumed
//! chat request continues here, and `/v1/chat/completions/derender` turns
//! the whole output back into a chat message.

use serde::Deserialize;

use crate::api::progress::Progress;
use crate::worker::vllm::protocol::Usage;
use crate::worker::vllm::{ABORT, StreamError};

/// `GenerateStreamResponse`.
#[derive(Debug, Deserialize)]
pub struct Chunk {
    pub choices: Vec<StreamChoice>,
    pub usage: Option<Usage>,
}

/// `GenerateResponseStreamChoice`.
#[derive(Debug, Deserialize)]
pub struct StreamChoice {
    pub index: u32,
    pub finish_reason: Option<String>,
    pub token_ids: Option<Vec<u32>>,
}

/// A finished generation: the original prompt and every output token,
/// saved ones included.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Generated {
    pub prompt_token_ids: Vec<u32>,
    pub token_ids: Vec<u32>,
    pub finish_reason: String,
}

/// Collects a generate stream that continues saved output.
#[derive(Debug)]
pub struct Accumulator {
    prompt_token_ids: Vec<u32>,
    token_ids: Vec<u32>,
    finish_reason: Option<String>,
    usage: bool,
}

impl Accumulator {
    pub fn resume(progress: &Progress) -> Self {
        Self {
            prompt_token_ids: progress.prompt_token_ids.clone(),
            token_ids: progress.token_ids.clone(),
            finish_reason: None,
            usage: false,
        }
    }

    /// The prompt and all output so far, while unfinished.
    pub fn progress(&self) -> Option<(&[u32], &[u32])> {
        self.finish_reason
            .is_none()
            .then_some((&self.prompt_token_ids[..], &self.token_ids[..]))
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
        self.usage |= chunk.usage.is_some();
        Ok(())
    }

    pub fn finish(self) -> Result<Generated, StreamError> {
        let finish_reason = self.finish_reason.ok_or(StreamError::Unfinished)?;
        if !self.usage {
            return Err(StreamError::MissingUsage);
        }
        Ok(Generated {
            prompt_token_ids: self.prompt_token_ids,
            token_ids: self.token_ids,
            finish_reason,
        })
    }
}
