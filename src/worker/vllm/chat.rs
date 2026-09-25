//! `/v1/chat/completions`, from
//! `vllm/entrypoints/openai/chat_completion/protocol.py` and `serving.py`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use serde_json::value::RawValue;

use crate::worker::vllm::generate::Generated;
use crate::worker::vllm::protocol::{Envelope, RequestMetrics, StopReason, Usage};
use crate::worker::vllm::{ABORT, StreamError};

/// `ChatCompletionStreamResponse`.
#[derive(Debug, Deserialize)]
pub struct Chunk {
    pub id: String,
    pub created: u64,
    pub model: String,
    pub choices: Vec<StreamChoice>,
    pub usage: Option<Usage>,
    pub system_fingerprint: Option<String>,
    pub prompt_token_ids: Option<Vec<u32>>,
    pub metrics: Option<RequestMetrics>,
}

/// `ChatCompletionResponseStreamChoice`.
#[derive(Debug, Deserialize)]
pub struct StreamChoice {
    pub index: u32,
    pub delta: Delta,
    pub finish_reason: Option<String>,
    pub stop_reason: Option<StopReason>,
    pub token_ids: Option<Vec<u32>>,
}

/// `DeltaMessage`.
#[derive(Debug, Deserialize)]
pub struct Delta {
    pub role: Option<String>,
    pub content: Option<String>,
    pub reasoning: Option<String>,
    #[serde(default)]
    pub tool_calls: Vec<ToolCallDelta>,
}

/// `DeltaToolCall`.
#[derive(Debug, Deserialize)]
pub struct ToolCallDelta {
    pub id: Option<String>,
    pub index: u32,
    pub function: Option<FunctionDelta>,
}

/// `DeltaFunctionCall`.
#[derive(Debug, Deserialize)]
pub struct FunctionDelta {
    pub name: Option<String>,
    pub arguments: Option<String>,
}

/// `ChatCompletionResponse`.
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
    pub prompt_logprobs: Option<Value>,
    pub prompt_token_ids: Option<Vec<u32>>,
    pub prompt_text: Option<String>,
    pub kv_transfer_params: Option<Value>,
    pub ec_transfer_params: Option<Value>,
    pub metrics: Option<RequestMetrics>,
}

/// `ChatCompletionResponseChoice`.
#[derive(Debug, Serialize)]
pub struct Choice {
    pub index: u32,
    pub message: Message,
    pub logprobs: Option<Value>,
    pub finish_reason: Option<String>,
    pub stop_reason: Option<StopReason>,
    pub token_ids: Option<Vec<u32>>,
    pub routed_experts: Option<String>,
}

/// `ChatMessage`.
#[derive(Debug, Serialize)]
pub struct Message {
    pub role: String,
    pub content: Option<String>,
    pub refusal: Option<String>,
    pub annotations: Option<Value>,
    pub audio: Option<Value>,
    pub function_call: Option<Value>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ToolCall>,
    pub reasoning: Option<String>,
}

/// `ToolCall`.
#[derive(Debug, Serialize)]
pub struct ToolCall {
    pub id: String,
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub function: FunctionCall,
}

/// `FunctionCall`.
#[derive(Debug, Serialize)]
pub struct FunctionCall {
    pub name: String,
    pub arguments: String,
}

#[derive(Debug, Default)]
struct PartialToolCall {
    id: Option<String>,
    name: Option<String>,
    arguments: String,
}

/// Rebuilds the non-streamed response of a single-choice chat completion
/// from its stream.
#[derive(Debug, Default)]
pub struct Accumulator {
    /// Whether the caller asked for token IDs.
    caller_token_ids: bool,
    /// vLLM's `strip_content_whitespace_with_tools` for the queue's parser.
    strip_content_with_tools: bool,
    envelope: Option<Envelope>,
    role: Option<String>,
    /// `None` until a chunk after the role chunk carries content.
    content: Option<String>,
    reasoning: Option<String>,
    tool_calls: BTreeMap<u32, PartialToolCall>,
    token_ids: Vec<u32>,
    prompt_token_ids: Option<Vec<u32>>,
    finish: Option<(String, Option<StopReason>)>,
    usage: Option<Usage>,
    system_fingerprint: Option<String>,
    metrics: Option<RequestMetrics>,
}

fn append(to: &mut Option<String>, text: String) {
    match to {
        Some(existing) => existing.push_str(&text),
        None => *to = Some(text),
    }
}

impl Accumulator {
    pub fn new(caller_token_ids: bool, strip_content_with_tools: bool) -> Self {
        Self {
            caller_token_ids,
            strip_content_with_tools,
            ..Self::default()
        }
    }

    /// The prompt and all output so far, while unfinished. vLLM derenders
    /// from token IDs, so any token is a clean place to stop.
    pub fn progress(&self) -> Option<(&[u32], &[u32])> {
        if self.finish.is_some() || self.token_ids.is_empty() {
            return None;
        }
        Some((self.prompt_token_ids.as_deref()?, &self.token_ids))
    }

    pub fn push(&mut self, chunk: Chunk) -> Result<(), StreamError> {
        if self.envelope.is_none() {
            self.envelope = Some(Envelope {
                id: chunk.id,
                created: chunk.created,
                model: chunk.model,
            });
        }
        if let Some(ids) = chunk.prompt_token_ids {
            self.prompt_token_ids = Some(ids);
        }
        for choice in chunk.choices {
            if choice.index != 0 {
                return Err(StreamError::UnexpectedChoice(choice.index));
            }
            if self.finish.is_some() {
                return Err(StreamError::AfterFinish);
            }
            let delta = choice.delta;
            if let Some(role) = delta.role {
                self.role = Some(role);
            } else {
                self.token_ids
                    .extend(choice.token_ids.ok_or(StreamError::MissingTokenIds)?);
                if let Some(content) = delta.content {
                    append(&mut self.content, content);
                }
            }
            if let Some(reasoning) = delta.reasoning {
                append(&mut self.reasoning, reasoning);
            }
            for call in delta.tool_calls {
                let partial = self.tool_calls.entry(call.index).or_default();
                if partial.id.is_none() {
                    partial.id = call.id;
                }
                if let Some(function) = call.function {
                    if partial.name.is_none() {
                        partial.name = function.name;
                    }
                    if let Some(arguments) = function.arguments {
                        partial.arguments.push_str(&arguments);
                    }
                }
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
        let role = self.role.ok_or(StreamError::MissingRole)?;
        let (finish_reason, stop_reason) = self.finish.ok_or(StreamError::Unfinished)?;
        let usage = self.usage.ok_or(StreamError::MissingUsage)?;
        let tool_calls = self
            .tool_calls
            .into_iter()
            .map(|(index, call)| {
                Ok(ToolCall {
                    id: call.id.unwrap_or_else(tool_call_id),
                    kind: "function",
                    function: FunctionCall {
                        name: call.name.ok_or(StreamError::UnnamedToolCall(index))?,
                        arguments: call.arguments,
                    },
                })
            })
            .collect::<Result<Vec<_>, StreamError>>()?;
        let content = if self.strip_content_with_tools && !tool_calls.is_empty() {
            self.content
                .map(|c| c.trim_matches(is_python_space).to_owned())
                .filter(|c| !c.is_empty())
        } else {
            self.content
        };
        let keep = self.caller_token_ids;
        Ok(Response {
            id,
            object: "chat.completion",
            created,
            model,
            choices: vec![Choice {
                index: 0,
                message: Message {
                    role,
                    content,
                    refusal: None,
                    annotations: None,
                    audio: None,
                    function_call: None,
                    tool_calls,
                    reasoning: self.reasoning,
                },
                logprobs: None,
                finish_reason: Some(finish_reason),
                stop_reason,
                token_ids: keep.then_some(self.token_ids),
                routed_experts: None,
            }],
            service_tier: None,
            system_fingerprint: self.system_fingerprint,
            usage,
            prompt_logprobs: None,
            prompt_token_ids: self.prompt_token_ids.filter(|_| keep),
            prompt_text: None,
            kv_transfer_params: None,
            ec_transfer_params: None,
            metrics: self.metrics,
        })
    }
}

/// `DerenderChatRequest`, for one continued generation.
#[derive(Serialize)]
struct DerenderRequest<'a> {
    generate_response: GenerateResponse<'a>,
    prompt_tokens: usize,
    chat_request: &'a RawValue,
}

/// `GenerateResponse`.
#[derive(Serialize)]
struct GenerateResponse<'a> {
    request_id: &'a str,
    choices: [GenerateChoice<'a>; 1],
    prompt_token_ids: &'a [u32],
}

/// `GenerateResponseChoice`.
#[derive(Serialize)]
struct GenerateChoice<'a> {
    index: u32,
    token_ids: &'a [u32],
    finish_reason: &'a str,
}

/// The body that asks vLLM to derender `generated`, the output of the chat
/// request `chat_request`, under `id`.
pub fn derender_request(
    generated: &Generated,
    id: &str,
    chat_request: &[u8],
) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&DerenderRequest {
        generate_response: GenerateResponse {
            request_id: id,
            choices: [GenerateChoice {
                index: 0,
                token_ids: &generated.token_ids,
                finish_reason: &generated.finish_reason,
            }],
            prompt_token_ids: &generated.prompt_token_ids,
        },
        prompt_tokens: generated.prompt_token_ids.len(),
        chat_request: serde_json::from_slice(chat_request)?,
    })
}

/// A derendered `ChatCompletionResponse`, of which only the message is
/// kept: the rest does not match the non-streamed endpoint.
#[derive(Debug, Deserialize)]
struct Derendered {
    model: String,
    choices: Vec<DerenderedChoice>,
}

#[derive(Debug, Deserialize)]
struct DerenderedChoice {
    message: DerenderedMessage,
}

#[derive(Debug, Deserialize)]
struct DerenderedMessage {
    role: String,
    content: Option<String>,
    reasoning: Option<String>,
    #[serde(default)]
    tool_calls: Vec<DerenderedToolCall>,
}

#[derive(Debug, Deserialize)]
struct DerenderedToolCall {
    function: DerenderedFunction,
}

#[derive(Debug, Deserialize)]
struct DerenderedFunction {
    name: String,
    arguments: String,
}

/// The non-streamed response of a resumed chat request: the message vLLM
/// derendered from the whole output, with the finish reason, tool call IDs
/// and usage the non-streamed endpoint gives. The reasoning token count is
/// left null: derender does not report it.
pub fn resumed_response(
    derendered: &[u8],
    id: String,
    created: u64,
    generated: Generated,
    caller_token_ids: bool,
) -> Result<Response, StreamError> {
    let Derendered { model, choices } = serde_json::from_slice(derendered)?;
    let [DerenderedChoice { message }] = <[DerenderedChoice; 1]>::try_from(choices)
        .map_err(|choices| StreamError::UnexpectedChoice(choices.len() as u32))?;
    let tool_calls: Vec<ToolCall> = message
        .tool_calls
        .into_iter()
        .map(|call| ToolCall {
            id: tool_call_id(),
            kind: "function",
            function: FunctionCall {
                name: call.function.name,
                arguments: call.function.arguments,
            },
        })
        .collect();
    let finish_reason = if tool_calls.is_empty() {
        generated.finish_reason
    } else {
        "tool_calls".to_owned()
    };
    let prompt_tokens = generated.prompt_token_ids.len() as u64;
    let completion_tokens = generated.token_ids.len() as u64;
    Ok(Response {
        id,
        object: "chat.completion",
        created,
        model,
        choices: vec![Choice {
            index: 0,
            message: Message {
                role: message.role,
                content: message.content,
                refusal: None,
                annotations: None,
                audio: None,
                function_call: None,
                tool_calls,
                reasoning: message.reasoning,
            },
            logprobs: None,
            finish_reason: Some(finish_reason),
            stop_reason: None,
            token_ids: caller_token_ids.then_some(generated.token_ids),
            routed_experts: None,
        }],
        service_tier: None,
        system_fingerprint: None,
        usage: Usage {
            prompt_tokens,
            total_tokens: prompt_tokens + completion_tokens,
            completion_tokens: Some(completion_tokens),
            prompt_tokens_details: None,
            completion_tokens_details: None,
        },
        prompt_logprobs: None,
        prompt_token_ids: caller_token_ids.then_some(generated.prompt_token_ids),
        prompt_text: None,
        kv_transfer_params: None,
        ec_transfer_params: None,
        metrics: None,
    })
}

/// Python's `str.isspace`: Unicode white space plus the information
/// separators U+001C to U+001F.
fn is_python_space(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
}

/// `make_tool_call_id`'s default form, for a call the stream gave no ID.
fn tool_call_id() -> String {
    format!("chatcmpl-tool-{:032x}", rand::random::<u128>())
}
