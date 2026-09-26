//! `/v1/completions/derender` and `/v1/chat/completions/derender`, from
//! `vllm/entrypoints/scale_out/derender/serving.py`: turn a finished
//! generation back into the response of the caller's request.

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::{Map, Value};

use crate::worker::vllm::generate::Generated;
use crate::worker::vllm::{Api, Caller};

/// `GenerateResponse`, for one generation.
#[derive(Serialize)]
struct GenerateResponse<'a> {
    request_id: &'a str,
    choices: [GenerateChoice<'a>; 1],
}

/// `GenerateResponseChoice`.
#[derive(Serialize)]
struct GenerateChoice<'a> {
    index: u32,
    token_ids: &'a [u32],
    finish_reason: &'a str,
}

/// `DerenderChatRequest` or `DerenderCompletionRequest`.
#[derive(Serialize)]
#[serde(untagged)]
enum Request<'a> {
    Chat {
        generate_response: GenerateResponse<'a>,
        prompt_tokens: usize,
        chat_request: &'a RawValue,
    },
    Completions {
        generate_responses: [GenerateResponse<'a>; 1],
        prompt_tokens: [usize; 1],
        completion_request: &'a RawValue,
    },
}

/// A new response ID in the form vLLM gives the caller's API.
pub fn response_id(api: Api) -> String {
    let prefix = match api {
        Api::Completions => "cmpl",
        Api::Chat => "chatcmpl",
    };
    format!("{prefix}-{:016x}", rand::random::<u64>())
}

/// The body that asks vLLM to derender `generated`, the output of the
/// caller's request `payload`, as the response `id`.
pub fn request(
    api: Api,
    generated: &Generated,
    id: &str,
    payload: &[u8],
) -> Result<Vec<u8>, serde_json::Error> {
    let caller: &RawValue = serde_json::from_slice(payload)?;
    let response = GenerateResponse {
        request_id: id,
        choices: [GenerateChoice {
            index: 0,
            token_ids: &generated.token_ids,
            finish_reason: &generated.finish_reason,
        }],
    };
    let prompt_tokens = generated.prompt_token_ids.len();
    serde_json::to_vec(&match api {
        Api::Chat => Request::Chat {
            generate_response: response,
            prompt_tokens,
            chat_request: caller,
        },
        Api::Completions => Request::Completions {
            generate_responses: [response],
            prompt_tokens: [prompt_tokens],
            completion_request: caller,
        },
    })
}

/// A derendered `ChatCompletionResponse` or `CompletionResponse`, typed
/// where it differs from the non-streamed endpoint's.
#[derive(Deserialize, Serialize)]
struct Response {
    choices: Vec<Choice>,
    #[serde(flatten)]
    rest: Map<String, Value>,
}

#[derive(Deserialize, Serialize)]
struct Choice {
    finish_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    message: Option<Message>,
    #[serde(flatten)]
    rest: Map<String, Value>,
}

#[derive(Deserialize, Serialize)]
struct Message {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    tool_calls: Vec<ToolCall>,
    #[serde(flatten)]
    rest: Map<String, Value>,
}

#[derive(Deserialize, Serialize)]
struct ToolCall {
    id: String,
    #[serde(flatten)]
    rest: Map<String, Value>,
}

#[derive(Debug, thiserror::Error)]
pub enum Underendered {
    #[error("unparsable derender response: {0}")]
    Json(#[from] serde_json::Error),
    #[error("derender returned {0} choices")]
    Choices(usize),
}

/// The response the caller's non-streamed request would have got, from
/// vLLM's derender of `generated`: tool call IDs in the endpoint's form,
/// `tool_calls` as the finish reason when the output called tools, the
/// token IDs when the caller asked for them, and the prompt token details the
/// generate stream reported, cached tokens capped at the caller's prompt (a
/// continuation's prompt includes saved output). The reasoning token count
/// and the stop reason stay null: derender reports neither.
pub fn response(
    caller: Caller,
    derendered: &[u8],
    generated: Generated,
) -> Result<String, Underendered> {
    let mut response: Response = serde_json::from_slice(derendered)?;
    let [choice] = response.choices.as_mut_slice() else {
        return Err(Underendered::Choices(response.choices.len()));
    };
    if let Some(message) = &mut choice.message
        && !message.tool_calls.is_empty()
    {
        for call in &mut message.tool_calls {
            call.id = format!("chatcmpl-tool-{:016x}", rand::random::<u64>());
        }
        choice.finish_reason = Some("tool_calls".to_owned());
    }
    if let Some(mut details) = generated.prompt_tokens_details.clone() {
        let prompt = generated.prompt_token_ids.len() as u64;
        if let Some(cached) = details.get("cached_tokens").and_then(Value::as_u64) {
            details.insert("cached_tokens".into(), cached.min(prompt).into());
        }
        if let Some(usage) = response
            .rest
            .get_mut("usage")
            .and_then(Value::as_object_mut)
        {
            usage.insert("prompt_tokens_details".into(), Value::Object(details));
        }
    }
    if caller.token_ids {
        choice
            .rest
            .insert("token_ids".into(), generated.token_ids.into());
        let prompt = Value::from(generated.prompt_token_ids);
        match caller.api {
            Api::Chat => response.rest.insert("prompt_token_ids".into(), prompt),
            Api::Completions => choice.rest.insert("prompt_token_ids".into(), prompt),
        };
    }
    Ok(serde_json::to_string(&response)?)
}
