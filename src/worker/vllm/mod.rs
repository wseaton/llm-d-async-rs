//! Sends a resumable request streamed and rebuilds, from the stream, the
//! response vLLM would have returned to the caller's non-streamed request.
//! An interrupted completion continues on `/v1/completions` from its saved
//! tokens. An interrupted chat request continues on `/inference/v1/generate`
//! from the prompt vLLM renders for it, and vLLM derenders the whole output
//! into the chat message.

pub mod chat;
pub mod completions;
pub mod generate;
pub mod protocol;
pub mod sse;

use std::collections::BTreeMap;

use bytes::Bytes;
use serde::de::{DeserializeOwned, IgnoredAny};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::api::progress::Progress;
use crate::worker::vllm::protocol::ErrorResponse;
use crate::worker::vllm::sse::{NotUtf8, SseDecoder};

/// The finish reason of a generation the engine aborted.
pub const ABORT: &str = "abort";

const DONE: &str = "[DONE]";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Api {
    Completions,
    Chat,
}

impl Api {
    /// The API a request URL calls. Matches llm-d-router's suffix rule, so
    /// gateways behind a base path and per-request endpoints still match.
    pub fn from_url(url: &str) -> Option<Self> {
        let url = reqwest::Url::parse(url).ok()?;
        let path = url.path().trim().trim_end_matches('/');
        if path.ends_with("v1/chat/completions") {
            Some(Self::Chat)
        } else if path.ends_with("v1/completions") {
            Some(Self::Completions)
        } else {
            None
        }
    }
}

/// A vLLM tool call parser (`--tool-call-parser`) whose whole-output rules
/// are modeled here, so a response rebuilt from its stream matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ToolCallParser {
    Glm45,
    Glm47,
}

impl ToolCallParser {
    /// vLLM's `strip_content_whitespace_with_tools` for this parser.
    fn strips_content_with_tools(self) -> bool {
        match self {
            Self::Glm45 | Self::Glm47 => true,
        }
    }
}

/// Why a request is sent as submitted instead of streamed for resumption.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Ineligible {
    #[error("payload is not a JSON object with valid field types")]
    Unparsable,
    #[error("the caller asked for a stream")]
    CallerStreams,
    #[error("stream_options without stream")]
    StreamOptions,
    #[error("more than one choice")]
    Choices,
    #[error("beam search")]
    BeamSearch,
    #[error("seeded sampling")]
    Seed,
    #[error("structured output")]
    StructuredOutput,
    #[error("forced tool choice")]
    ForcedToolChoice,
    #[error("presence or frequency penalty")]
    Penalty,
    #[error("logprobs")]
    Logprobs,
    #[error("echo")]
    Echo,
    #[error("prompt is not one text or one list of token IDs")]
    PromptShape,
    #[error("reasoning hidden from the response")]
    HiddenReasoning,
    #[error("rendered prompt text requested")]
    PromptText,
    #[error("kv_transfer_params")]
    KvTransfer,
    #[error("tools offered, and the queue names no tool_call_parser")]
    UnknownToolParser,
}

/// Where a plan's body goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    /// The request's own URL.
    Request,
    /// vLLM's `/inference/v1/generate`, on the queue's gateway.
    Generate,
}

/// A request to send streamed: the rewritten body, where it goes, and the
/// reassembly its stream feeds.
pub struct Plan {
    pub body: Bytes,
    pub target: Target,
    pub reassembly: Reassembly,
    /// The body continues saved progress instead of starting over.
    pub resumed: bool,
}

/// Why saved chat progress cannot be continued, so the request restarts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Restart {
    #[error("the rendered prompt differs from the saved one")]
    PromptChanged,
    #[error("the saved output used the whole token budget")]
    Exhausted,
    #[error("unparsable render response")]
    Unparsable,
}

type Fields = BTreeMap<String, Box<RawValue>>;

fn field<T: DeserializeOwned>(fields: &Fields, name: &str) -> Result<Option<T>, Ineligible> {
    fields
        .get(name)
        .map_or(Ok(None), |raw| serde_json::from_str(raw.get()))
        .map_err(|_| Ineligible::Unparsable)
}

fn present(fields: &Fields, name: &str) -> Result<bool, Ineligible> {
    Ok(field::<IgnoredAny>(fields, name)?.is_some())
}

/// A field of the rewritten body.
#[derive(Serialize)]
#[serde(untagged)]
enum Sent<'a> {
    Caller(&'a RawValue),
    Flag(bool),
    Count(u64),
    Tokens(Vec<u32>),
    Value(serde_json::Value),
    StreamOptions { include_usage: bool },
}

/// vLLM's `max_tokens` for a completion that leaves it unset or null.
const DEFAULT_MAX_TOKENS: u64 = 16;

/// Checks that `payload` for `api` can be streamed, and rewrites it to
/// stream with usage and token IDs. With `progress` from an interrupted
/// attempt that can be continued, the body continues it: the prompt becomes
/// the original prompt's tokens plus the saved ones, and the token limits
/// shrink by the saved count. Every other field is sent unchanged. `parser`
/// is the queue's vLLM tool call parser, if it names one; `renders` says
/// whether the queue has a vLLM that renders and derenders chat, without
/// which chat output cannot be continued. Chat progress is continued by
/// `continue_chat`, not here.
pub fn plan(
    api: Api,
    parser: Option<ToolCallParser>,
    renders: bool,
    payload: &[u8],
    progress: Option<&Progress>,
) -> Result<Plan, Ineligible> {
    let fields: Fields = serde_json::from_slice(payload).map_err(|_| Ineligible::Unparsable)?;
    check(api, parser, &fields)?;
    let caller_token_ids = field::<bool>(&fields, "return_token_ids")?.unwrap_or(false);
    let continuable = continuable(api, renders, &fields)?;
    let continuation = match progress.filter(|_| continuable && api == Api::Completions) {
        Some(progress) => continuation(&fields, progress)?,
        None => None,
    };
    let mut sent: BTreeMap<&str, Sent<'_>> = fields
        .iter()
        .map(|(name, value)| (name.as_str(), Sent::Caller(value)))
        .collect();
    sent.insert("stream", Sent::Flag(true));
    sent.insert(
        "stream_options",
        Sent::StreamOptions {
            include_usage: true,
        },
    );
    sent.insert("return_token_ids", Sent::Flag(true));
    let resumed = continuation.is_some();
    if let Some(c) = continuation {
        sent.insert("prompt", Sent::Tokens(c.prompt));
        sent.insert("max_tokens", Sent::Count(c.max_tokens));
        if let Some(min) = c.min_tokens {
            sent.insert("min_tokens", Sent::Count(min));
        }
    }
    let body = serde_json::to_vec(&sent).map_err(|_| Ineligible::Unparsable)?;
    let reassembly = match progress.filter(|_| resumed) {
        Some(progress) => Reassembly::resume(caller_token_ids, progress),
        None => Reassembly::new(api, caller_token_ids, parser, continuable),
    };
    Ok(Plan {
        body: Bytes::from(body),
        target: Target::Request,
        reassembly,
        resumed,
    })
}

/// Whether an interrupted stream of this request can be continued rather
/// than restarted. Stop strings and prompt truncation would act on the saved
/// output as prompt. Chat continues only where vLLM derenders it, and not
/// with stop token IDs, whose stop reason a generate stream does not carry.
fn continuable(api: Api, renders: bool, fields: &Fields) -> Result<bool, Ineligible> {
    use serde_json::Value;

    let stops = match field::<Value>(fields, "stop")? {
        None => false,
        Some(Value::String(s)) => !s.is_empty(),
        Some(Value::Array(a)) => !a.is_empty(),
        Some(_) => return Err(Ineligible::Unparsable),
    };
    let common = !stops && !present(fields, "truncate_prompt_tokens")?;
    Ok(match api {
        Api::Completions => common,
        Api::Chat => {
            let stop_tokens =
                field::<Vec<IgnoredAny>>(fields, "stop_token_ids")?.is_some_and(|t| !t.is_empty());
            common && renders && !stop_tokens
        }
    })
}

/// The `/inference/v1/generate` body that continues saved chat progress,
/// from vLLM's render of the caller's chat request `payload`: the rendered
/// prompt must be the saved one, and the token limits shrink by the saved
/// count.
pub fn continue_chat(
    rendered: &[u8],
    progress: &Progress,
    payload: &[u8],
) -> Result<Plan, Restart> {
    use serde_json::Value;

    let caller: Fields = serde_json::from_slice(payload).map_err(|_| Restart::Unparsable)?;
    let caller_token_ids = field::<bool>(&caller, "return_token_ids")
        .map_err(|_| Restart::Unparsable)?
        .unwrap_or(false);
    let fields: Fields = serde_json::from_slice(rendered).map_err(|_| Restart::Unparsable)?;
    let prompt: Vec<u32> = field(&fields, "token_ids")
        .ok()
        .flatten()
        .ok_or(Restart::Unparsable)?;
    if prompt != progress.prompt_token_ids {
        return Err(Restart::PromptChanged);
    }
    let mut sampling: serde_json::Map<String, Value> = field(&fields, "sampling_params")
        .ok()
        .flatten()
        .ok_or(Restart::Unparsable)?;
    let saved = progress.token_ids.len() as u64;
    let max_tokens = sampling
        .get("max_tokens")
        .and_then(Value::as_u64)
        .ok_or(Restart::Unparsable)?;
    let left = max_tokens
        .checked_sub(saved)
        .filter(|left| *left > 0)
        .ok_or(Restart::Exhausted)?;
    sampling.insert("max_tokens".into(), left.into());
    if let Some(min) = sampling.get("min_tokens").and_then(Value::as_u64) {
        sampling.insert("min_tokens".into(), min.saturating_sub(saved).into());
    }
    let mut continued = prompt;
    continued.extend_from_slice(&progress.token_ids);

    let mut sent: BTreeMap<&str, Sent<'_>> = fields
        .iter()
        .map(|(name, value)| (name.as_str(), Sent::Caller(value)))
        .collect();
    sent.insert("token_ids", Sent::Tokens(continued));
    sent.insert("sampling_params", Sent::Value(Value::Object(sampling)));
    sent.insert("stream", Sent::Flag(true));
    sent.insert(
        "stream_options",
        Sent::StreamOptions {
            include_usage: true,
        },
    );
    sent.insert("return_token_ids", Sent::Flag(true));
    let body = serde_json::to_vec(&sent).map_err(|_| Restart::Unparsable)?;
    Ok(Plan {
        body: Bytes::from(body),
        target: Target::Generate,
        reassembly: Reassembly::generate(caller_token_ids, progress),
        resumed: true,
    })
}

struct Continuation {
    prompt: Vec<u32>,
    max_tokens: u64,
    min_tokens: Option<u64>,
}

/// The prompt and limits that continue `progress`, or `None` when the saved
/// output already used the whole token budget.
fn continuation(fields: &Fields, progress: &Progress) -> Result<Option<Continuation>, Ineligible> {
    let saved = progress.token_ids.len() as u64;
    let max_tokens = field::<u64>(fields, "max_tokens")?.unwrap_or(DEFAULT_MAX_TOKENS);
    let Some(max_tokens) = max_tokens.checked_sub(saved).filter(|left| *left > 0) else {
        return Ok(None);
    };
    let min_tokens = field::<u64>(fields, "min_tokens")?.map(|m| m.saturating_sub(saved));
    let mut prompt = progress.prompt_token_ids.clone();
    prompt.extend_from_slice(&progress.token_ids);
    Ok(Some(Continuation {
        prompt,
        max_tokens,
        min_tokens,
    }))
}

fn check(api: Api, parser: Option<ToolCallParser>, fields: &Fields) -> Result<(), Ineligible> {
    use serde_json::Value;

    let flag = |name| Ok::<_, Ineligible>(field::<bool>(fields, name)?.unwrap_or(false));
    let penalty = |name| Ok::<_, Ineligible>(field::<f64>(fields, name)?.unwrap_or(0.0) != 0.0);
    if flag("stream")? {
        return Err(Ineligible::CallerStreams);
    }
    if present(fields, "stream_options")? {
        return Err(Ineligible::StreamOptions);
    }
    if field::<u64>(fields, "n")?.unwrap_or(1) > 1
        || field::<u64>(fields, "best_of")?.unwrap_or(1) > 1
    {
        return Err(Ineligible::Choices);
    }
    if flag("use_beam_search")? {
        return Err(Ineligible::BeamSearch);
    }
    if present(fields, "seed")? {
        return Err(Ineligible::Seed);
    }
    let text_format = field::<Value>(fields, "response_format")?
        .is_none_or(|f| f.get("type").and_then(Value::as_str) == Some("text"));
    if !text_format || present(fields, "structured_outputs")? {
        return Err(Ineligible::StructuredOutput);
    }
    let tools_suppressed = match field::<Value>(fields, "tool_choice")? {
        None => false,
        Some(Value::String(choice)) if choice == "auto" => false,
        Some(Value::String(choice)) if choice == "none" => true,
        Some(_) => return Err(Ineligible::ForcedToolChoice),
    };
    let tools_offered = field::<Vec<IgnoredAny>>(fields, "tools")?.is_some_and(|t| !t.is_empty());
    if tools_offered && !tools_suppressed && parser.is_none() {
        return Err(Ineligible::UnknownToolParser);
    }
    if penalty("presence_penalty")? || penalty("frequency_penalty")? {
        return Err(Ineligible::Penalty);
    }
    let logprobs = !matches!(
        field::<Value>(fields, "logprobs")?,
        None | Some(Value::Bool(false))
    );
    if logprobs || present(fields, "prompt_logprobs")? {
        return Err(Ineligible::Logprobs);
    }
    if flag("echo")? {
        return Err(Ineligible::Echo);
    }
    if present(fields, "kv_transfer_params")? {
        return Err(Ineligible::KvTransfer);
    }
    match api {
        Api::Completions => check_prompt(fields),
        Api::Chat => {
            if field::<bool>(fields, "include_reasoning")? == Some(false) {
                return Err(Ineligible::HiddenReasoning);
            }
            if flag("return_prompt_text")? {
                return Err(Ineligible::PromptText);
            }
            Ok(())
        }
    }
}

fn check_prompt(fields: &Fields) -> Result<(), Ineligible> {
    let prompt = fields.get("prompt").ok_or(Ineligible::PromptShape)?.get();
    if prompt.starts_with('"') || serde_json::from_str::<Vec<u32>>(prompt).is_ok() {
        Ok(())
    } else {
        Err(Ineligible::PromptShape)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    #[error("vLLM reported an error: {}", .0.error.message)]
    Upstream(ErrorResponse),
    #[error("stream ended before [DONE]")]
    NoDone,
    #[error("the engine aborted the generation")]
    Aborted,
    #[error("unparsable stream event: {0}")]
    Malformed(#[from] serde_json::Error),
    #[error(transparent)]
    NotUtf8(#[from] NotUtf8),
    #[error("event after [DONE]")]
    AfterDone,
    #[error("choice {0} in a single-choice stream")]
    UnexpectedChoice(u32),
    #[error("output after the finish reason")]
    AfterFinish,
    #[error("output chunk without token_ids")]
    MissingTokenIds,
    #[error("stream carried no chunks")]
    Empty,
    #[error("stream carried no role")]
    MissingRole,
    #[error("stream ended without a finish reason")]
    Unfinished,
    #[error("stream ended without usage")]
    MissingUsage,
    #[error("tool call {0} has no name")]
    UnnamedToolCall(u32),
}

impl StreamError {
    /// The generation was cut short and can be sent again.
    pub fn interrupted(&self) -> bool {
        matches!(self, Self::NoDone | Self::Aborted)
    }
}

enum Accumulator {
    Completions(completions::Accumulator),
    Chat(chat::Accumulator),
    Generate(generate::Accumulator),
    Finished,
}

/// What a finished stream yields.
#[derive(Debug)]
pub enum Finished {
    /// The response JSON.
    Response(String),
    /// A continued chat generation, which vLLM still has to derender.
    Generated {
        generated: generate::Generated,
        caller_token_ids: bool,
    },
}

/// Rebuilds a non-streamed response from the bytes of its event stream.
pub struct Reassembly {
    decoder: SseDecoder,
    events: Vec<String>,
    state: State,
    /// An interruption keeps the output so far.
    continuable: bool,
    /// Whether the caller asked for token IDs.
    caller_token_ids: bool,
}

struct State {
    done: bool,
    accumulator: Accumulator,
}

impl Reassembly {
    pub fn new(
        api: Api,
        caller_token_ids: bool,
        parser: Option<ToolCallParser>,
        continuable: bool,
    ) -> Self {
        let accumulator = match api {
            Api::Completions => {
                Accumulator::Completions(completions::Accumulator::new(caller_token_ids))
            }
            Api::Chat => Accumulator::Chat(chat::Accumulator::new(
                caller_token_ids,
                parser.is_some_and(ToolCallParser::strips_content_with_tools),
            )),
        };
        Self {
            decoder: SseDecoder::default(),
            events: Vec::new(),
            state: State {
                done: false,
                accumulator,
            },
            continuable,
            caller_token_ids,
        }
    }

    fn continuing(caller_token_ids: bool, accumulator: Accumulator) -> Self {
        Self {
            decoder: SseDecoder::default(),
            events: Vec::new(),
            state: State {
                done: false,
                accumulator,
            },
            continuable: true,
            caller_token_ids,
        }
    }

    /// A completion continuing `progress`.
    pub fn resume(caller_token_ids: bool, progress: &Progress) -> Self {
        Self::continuing(
            caller_token_ids,
            Accumulator::Completions(completions::Accumulator::resume(caller_token_ids, progress)),
        )
    }

    /// A chat generation continuing `progress` on `/inference/v1/generate`.
    pub fn generate(caller_token_ids: bool, progress: &Progress) -> Self {
        Self::continuing(
            caller_token_ids,
            Accumulator::Generate(generate::Accumulator::resume(progress)),
        )
    }

    /// The output to save when the stream is interrupted: everything up to
    /// the last point where text and tokens split cleanly, with `resumes`
    /// continuations so far.
    pub fn progress(&self, resumes: u32) -> Option<Progress> {
        if !self.continuable {
            return None;
        }
        let (prompt, tokens, text) = match &self.state.accumulator {
            Accumulator::Completions(a) => a.progress()?,
            Accumulator::Chat(a) => {
                let (prompt, tokens) = a.progress()?;
                (prompt, tokens, "")
            }
            Accumulator::Generate(a) => {
                let (prompt, tokens) = a.progress()?;
                (prompt, tokens, "")
            }
            Accumulator::Finished => return None,
        };
        Some(Progress {
            prompt_token_ids: prompt.to_vec(),
            token_ids: tokens.to_vec(),
            text: text.to_owned(),
            resumes,
        })
    }

    pub fn feed(&mut self, bytes: &[u8]) -> Result<(), StreamError> {
        self.decoder.feed(bytes, &mut self.events)?;
        for data in self.events.drain(..) {
            self.state.event(&data)?;
        }
        Ok(())
    }

    /// What the stream yields, once it has ended.
    pub fn finish(&mut self) -> Result<Finished, StreamError> {
        self.state.finish(self.caller_token_ids)
    }
}

impl State {
    fn event(&mut self, data: &str) -> Result<(), StreamError> {
        if self.done {
            return Err(StreamError::AfterDone);
        }
        if data == DONE {
            self.done = true;
            return Ok(());
        }
        let pushed = match &mut self.accumulator {
            Accumulator::Completions(a) => serde_json::from_str(data).map(|c| a.push(c)),
            Accumulator::Chat(a) => serde_json::from_str(data).map(|c| a.push(c)),
            Accumulator::Generate(a) => serde_json::from_str(data).map(|c| a.push(c)),
            Accumulator::Finished => return Err(StreamError::AfterDone),
        };
        match pushed {
            Ok(result) => result,
            Err(e) => Err(serde_json::from_str::<ErrorResponse>(data)
                .map_or(StreamError::Malformed(e), StreamError::Upstream)),
        }
    }

    fn finish(&mut self, caller_token_ids: bool) -> Result<Finished, StreamError> {
        if !self.done {
            return Err(StreamError::NoDone);
        }
        let json = match std::mem::replace(&mut self.accumulator, Accumulator::Finished) {
            Accumulator::Completions(a) => serde_json::to_string(&a.finish()?),
            Accumulator::Chat(a) => serde_json::to_string(&a.finish()?),
            Accumulator::Generate(a) => {
                return Ok(Finished::Generated {
                    generated: a.finish()?,
                    caller_token_ids,
                });
            }
            Accumulator::Finished => return Err(StreamError::AfterDone),
        };
        Ok(Finished::Response(json?))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use crate::api::progress::Progress;
    use crate::worker::vllm::{
        Api, Finished, Ineligible, Plan, Reassembly, Restart, StreamError, Target, ToolCallParser,
        chat, continue_chat, plan as plan_resuming,
    };

    fn plan(api: Api, parser: Option<ToolCallParser>, payload: &[u8]) -> Result<Plan, Ineligible> {
        plan_resuming(api, parser, false, payload, None)
    }

    fn response(finished: Finished) -> Value {
        match finished {
            Finished::Response(json) => serde_json::from_str(&json).unwrap(),
            Finished::Generated { .. } => panic!("a first attempt yields a response"),
        }
    }

    #[test]
    fn api_from_url() {
        let cases = [
            ("http://gw/v1/completions", Some(Api::Completions)),
            ("http://gw/base/v1/completions/", Some(Api::Completions)),
            ("http://gw/v1/chat/completions", Some(Api::Chat)),
            ("http://gw/base/v1/chat/completions/", Some(Api::Chat)),
            ("http://gw/v1/embeddings", None),
            ("http://gw/v1/messages", None),
            ("not a url", None),
        ];
        for (url, want) in cases {
            assert_eq!(Api::from_url(url), want, "{url}");
        }
    }

    fn ineligible(api: Api, body: Value) -> Ineligible {
        match plan(api, None, body.to_string().as_bytes()) {
            Ok(_) => panic!("{body} was eligible"),
            Err(reason) => reason,
        }
    }

    #[test]
    fn restart_rules() {
        use Ineligible::*;
        let completion = |extra: Value| {
            let mut body = json!({"model": "m", "prompt": "hi"});
            body.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            body
        };
        let cases = [
            (json!({"stream": true}), CallerStreams),
            (
                json!({"stream_options": {"include_usage": true}}),
                StreamOptions,
            ),
            (json!({"n": 2}), Choices),
            (json!({"best_of": 3}), Choices),
            (json!({"use_beam_search": true}), BeamSearch),
            (json!({"seed": 0}), Seed),
            (
                json!({"response_format": {"type": "json_object"}}),
                StructuredOutput,
            ),
            (
                json!({"structured_outputs": {"regex": "a+"}}),
                StructuredOutput,
            ),
            (json!({"tool_choice": "required"}), ForcedToolChoice),
            (
                json!({"tool_choice": {"type": "function", "function": {"name": "f"}}}),
                ForcedToolChoice,
            ),
            (json!({"presence_penalty": 0.5}), Penalty),
            (json!({"frequency_penalty": -0.1}), Penalty),
            (json!({"logprobs": 0}), Logprobs),
            (json!({"logprobs": true}), Logprobs),
            (json!({"prompt_logprobs": 1}), Logprobs),
            (json!({"echo": true}), Echo),
            (
                json!({"kv_transfer_params": {"do_remote_decode": true}}),
                KvTransfer,
            ),
            (json!({"prompt": ["a", "b"]}), PromptShape),
            (json!({"prompt": [[1, 2], [3]]}), PromptShape),
            (json!({"prompt": null}), PromptShape),
            (json!({"n": "two"}), Unparsable),
            (json!({"stream": "yes"}), Unparsable),
        ];
        for (extra, want) in cases {
            assert_eq!(
                ineligible(Api::Completions, completion(extra.clone())),
                want,
                "{extra}"
            );
        }
        assert_eq!(
            ineligible(Api::Completions, json!({"model": "m"})),
            PromptShape
        );
        let chat = |extra: Value| {
            let mut body = json!({"model": "m", "messages": [{"role": "user", "content": "hi"}]});
            body.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            body
        };
        assert_eq!(
            ineligible(Api::Chat, chat(json!({"include_reasoning": false}))),
            HiddenReasoning
        );
        assert_eq!(
            ineligible(Api::Chat, chat(json!({"return_prompt_text": true}))),
            PromptText
        );
        assert_eq!(
            ineligible(Api::Chat, chat(json!({"logprobs": true}))),
            Logprobs
        );
        assert_eq!(ineligible(Api::Chat, json!([1, 2])), Unparsable);
        assert!(matches!(
            plan(Api::Chat, None, b"not json"),
            Err(Ineligible::Unparsable)
        ));

        let eligible = [
            json!({}),
            json!({"prompt": [1, 2, 3]}),
            json!({"stream": false, "stream_options": null, "n": 1, "best_of": 1}),
            json!({"seed": null, "logprobs": null, "echo": false, "use_beam_search": false}),
            json!({"response_format": {"type": "text"}, "tool_choice": "auto"}),
            json!({"tool_choice": "none", "presence_penalty": 0, "frequency_penalty": 0.0}),
            json!({"min_tokens": 4, "max_tokens": 64, "stop": ["END"], "repetition_penalty": 1.1}),
        ];
        for extra in eligible {
            assert!(
                plan(
                    Api::Completions,
                    None,
                    completion(extra.clone()).to_string().as_bytes()
                )
                .is_ok(),
                "{extra}"
            );
        }
        let tools =
            json!({"tools": [{"type": "function", "function": {"name": "f", "parameters": {}}}]});
        assert_eq!(
            ineligible(Api::Chat, chat(tools.clone())),
            UnknownToolParser
        );
        let offered = chat(tools.clone()).to_string();
        assert!(plan(Api::Chat, Some(ToolCallParser::Glm47), offered.as_bytes()).is_ok());
        let mut suppressed = chat(tools);
        suppressed["tool_choice"] = json!("none");
        assert!(plan(Api::Chat, None, suppressed.to_string().as_bytes()).is_ok());
        assert!(
            plan(
                Api::Chat,
                None,
                chat(json!({"tools": []})).to_string().as_bytes()
            )
            .is_ok()
        );
        assert_eq!(ineligible(Api::Chat, chat(json!({"tools": 3}))), Unparsable);

        let chat_eligible = chat(
            json!({"include_reasoning": true, "logprobs": false, "tools": [], "chat_template_kwargs": {"enable_thinking": true}}),
        );
        assert!(plan(Api::Chat, None, chat_eligible.to_string().as_bytes()).is_ok());
    }

    #[test]
    fn rewrite_streams_and_keeps_every_other_field_verbatim() {
        let body = br#"{"model":"m","prompt":"hi","temperature":0.70,"big":123456789012345678901234567890,"nested":{"b":1,"a":[1.0e0]},"stream":false,"return_token_ids":false}"#;
        let p = plan(Api::Completions, None, body).unwrap();
        let sent = std::str::from_utf8(&p.body).unwrap();
        for kept in [
            r#""temperature":0.70"#,
            r#""big":123456789012345678901234567890"#,
            r#""nested":{"b":1,"a":[1.0e0]}"#,
            r#""prompt":"hi""#,
        ] {
            assert!(sent.contains(kept), "{kept} missing from {sent}");
        }
        let sent: Value = serde_json::from_str(sent).unwrap();
        assert_eq!(sent["stream"], true);
        assert_eq!(sent["stream_options"], json!({"include_usage": true}));
        assert_eq!(sent["return_token_ids"], true);
        assert_eq!(sent.as_object().unwrap().len(), 8);
    }

    fn sse(events: &[Value]) -> String {
        let mut out = String::new();
        for event in events {
            out.push_str(&format!("data: {event}\n\n"));
        }
        out.push_str("data: [DONE]\n\n");
        out
    }

    fn reassemble(api: Api, caller_token_ids: bool, stream: &str) -> Result<Value, StreamError> {
        let mut r = Reassembly::new(api, caller_token_ids, None, false);
        for piece in stream.as_bytes().chunks(7) {
            r.feed(piece)?;
        }
        Ok(response(r.finish()?))
    }

    fn completion_chunk(text: &str, ids: &[u32], finish: Option<&str>) -> Value {
        let mut choice = json!({"index": 0, "text": text, "logprobs": null, "token_ids": ids});
        if let Some(f) = finish {
            choice["finish_reason"] = json!(f);
            choice["stop_reason"] = json!(null);
        }
        json!({"id": "cmpl-1", "object": "text_completion", "created": 1700000000, "model": "m", "choices": [choice]})
    }

    fn usage_chunk(object: &str) -> Value {
        json!({"id": "cmpl-1", "object": object, "created": 1700000000, "model": "m", "choices": [],
               "usage": {"prompt_tokens": 3, "total_tokens": 5, "completion_tokens": 2}})
    }

    #[test]
    fn completion_is_rebuilt_with_nulls_and_without_our_token_ids() {
        let mut first = completion_chunk("Hel", &[10], None);
        first["choices"][0]["prompt_token_ids"] = json!([1, 2, 3]);
        let stream = sse(&[
            first,
            completion_chunk("lo", &[11], Some("stop")),
            usage_chunk("text_completion"),
        ]);
        let want = json!({
            "id": "cmpl-1", "object": "text_completion", "created": 1700000000, "model": "m",
            "choices": [{"index": 0, "text": "Hello", "logprobs": null, "finish_reason": "stop",
                         "stop_reason": null, "token_ids": null, "prompt_logprobs": null,
                         "prompt_token_ids": null, "routed_experts": null}],
            "service_tier": null, "system_fingerprint": null,
            "usage": {"prompt_tokens": 3, "total_tokens": 5, "completion_tokens": 2,
                      "prompt_tokens_details": null, "completion_tokens_details": null},
            "kv_transfer_params": null, "ec_transfer_params": null, "metrics": null,
        });
        assert_eq!(reassemble(Api::Completions, false, &stream).unwrap(), want);

        let kept = reassemble(Api::Completions, true, &stream).unwrap();
        assert_eq!(kept["choices"][0]["token_ids"], json!([10, 11]));
        assert_eq!(kept["choices"][0]["prompt_token_ids"], json!([1, 2, 3]));
    }

    #[test]
    fn stop_reason_and_usage_chunk_extras() {
        let mut last = completion_chunk("x", &[5], Some("stop"));
        last["choices"][0]["stop_reason"] = json!("END");
        let mut usage = usage_chunk("text_completion");
        usage["system_fingerprint"] = json!("fp");
        usage["usage"]["prompt_tokens_details"] = json!({"cached_tokens": 2});
        usage["metrics"] = json!({"time_to_first_token_ms": 1.5});
        let got = reassemble(Api::Completions, false, &sse(&[last, usage])).unwrap();
        assert_eq!(got["choices"][0]["stop_reason"], "END");
        assert_eq!(got["system_fingerprint"], "fp");
        assert_eq!(
            got["usage"]["prompt_tokens_details"],
            json!({"cached_tokens": 2, "created_cache_tokens": null, "multimodal_tokens": null})
        );
        assert_eq!(
            got["metrics"],
            json!({"time_to_first_token_ms": 1.5, "generation_time_ms": null, "queue_time_ms": null,
                   "mean_itl_ms": null, "tokens_per_second": null, "speculative_decoding": null})
        );

        let mut token_stop = completion_chunk("x", &[5], Some("stop"));
        token_stop["choices"][0]["stop_reason"] = json!(151336);
        let got = reassemble(
            Api::Completions,
            false,
            &sse(&[token_stop, usage_chunk("text_completion")]),
        )
        .unwrap();
        assert_eq!(got["choices"][0]["stop_reason"], 151336);
    }

    fn chat_chunk(delta: Value, ids: Option<&[u32]>, finish: Option<&str>) -> Value {
        let mut choice = json!({"index": 0, "delta": delta, "logprobs": null});
        if let Some(ids) = ids {
            choice["token_ids"] = json!(ids);
        }
        if let Some(f) = finish {
            choice["finish_reason"] = json!(f);
            choice["stop_reason"] = json!(null);
        }
        json!({"id": "chatcmpl-1", "object": "chat.completion.chunk", "created": 1700000000,
               "model": "m", "choices": [choice]})
    }

    fn role_chunk() -> Value {
        let mut c = chat_chunk(json!({"role": "assistant", "content": ""}), None, None);
        c["prompt_token_ids"] = json!([1, 2]);
        c
    }

    #[test]
    fn chat_reasoning_content_and_tool_calls() {
        let stream = sse(&[
            role_chunk(),
            chat_chunk(json!({"reasoning": "think"}), Some(&[3]), None),
            chat_chunk(json!({"reasoning": "ing"}), Some(&[4]), None),
            chat_chunk(json!({"content": "Let me look."}), Some(&[5]), None),
            chat_chunk(
                json!({"tool_calls": [{"id": "call-a", "type": "function", "index": 0,
                                       "function": {"name": "get", "arguments": "{\"k\""}}]}),
                Some(&[6]),
                None,
            ),
            chat_chunk(
                json!({"tool_calls": [{"index": 0, "function": {"arguments": ": 1}"}}]}),
                Some(&[7]),
                None,
            ),
            chat_chunk(
                json!({"tool_calls": [{"id": "call-b", "type": "function", "index": 1,
                                       "function": {"name": "put", "arguments": "{}"}}]}),
                Some(&[8]),
                Some("tool_calls"),
            ),
            usage_chunk("chat.completion.chunk"),
        ]);
        let got = reassemble(Api::Chat, false, &stream).unwrap();
        assert_eq!(
            got["choices"][0],
            json!({
                "index": 0,
                "message": {
                    "role": "assistant", "content": "Let me look.", "refusal": null,
                    "annotations": null, "audio": null, "function_call": null,
                    "tool_calls": [
                        {"id": "call-a", "type": "function", "function": {"name": "get", "arguments": "{\"k\": 1}"}},
                        {"id": "call-b", "type": "function", "function": {"name": "put", "arguments": "{}"}},
                    ],
                    "reasoning": "thinking",
                },
                "logprobs": null, "finish_reason": "tool_calls", "stop_reason": null,
                "token_ids": null, "routed_experts": null,
            })
        );
        assert_eq!(got["object"], "chat.completion");
        assert_eq!(got["prompt_token_ids"], Value::Null);
        for key in [
            "prompt_logprobs",
            "prompt_text",
            "kv_transfer_params",
            "ec_transfer_params",
        ] {
            assert_eq!(got[key], Value::Null, "{key}");
        }

        let kept = reassemble(Api::Chat, true, &stream).unwrap();
        assert_eq!(kept["prompt_token_ids"], json!([1, 2]));
        assert_eq!(kept["choices"][0]["token_ids"], json!([3, 4, 5, 6, 7, 8]));
    }

    #[test]
    fn chat_content_is_null_unless_output_carried_it() {
        let reasoning_only = sse(&[
            role_chunk(),
            chat_chunk(json!({"reasoning": "hmm"}), Some(&[3]), None),
            chat_chunk(json!({}), Some(&[4]), Some("length")),
            usage_chunk("chat.completion.chunk"),
        ]);
        let got = reassemble(Api::Chat, false, &reasoning_only).unwrap();
        assert_eq!(got["choices"][0]["message"]["content"], Value::Null);
        assert_eq!(got["choices"][0]["message"]["reasoning"], "hmm");
        assert!(got["choices"][0]["message"].get("tool_calls").is_none());

        let empty_answer = sse(&[
            role_chunk(),
            chat_chunk(json!({"content": ""}), Some(&[4]), Some("stop")),
            usage_chunk("chat.completion.chunk"),
        ]);
        let got = reassemble(Api::Chat, false, &empty_answer).unwrap();
        assert_eq!(got["choices"][0]["message"]["content"], "");
        assert_eq!(got["choices"][0]["message"]["reasoning"], Value::Null);
    }

    fn content_then_tool_call(content: &str) -> String {
        sse(&[
            role_chunk(),
            chat_chunk(json!({"content": content}), Some(&[3]), None),
            chat_chunk(
                json!({"tool_calls": [{"id": "c", "type": "function", "index": 0,
                                       "function": {"name": "f", "arguments": "{}"}}]}),
                Some(&[4]),
                Some("tool_calls"),
            ),
            usage_chunk("chat.completion.chunk"),
        ])
    }

    fn rebuilt_content(parser: Option<ToolCallParser>, stream: &str) -> Value {
        let mut r = Reassembly::new(Api::Chat, false, parser, false);
        r.feed(stream.as_bytes()).unwrap();
        let v = response(r.finish().unwrap());
        v["choices"][0]["message"]["content"].clone()
    }

    #[test]
    fn content_with_tool_calls_follows_the_parser() {
        let glm = Some(ToolCallParser::Glm45);
        let padded = content_then_tool_call("\u{1c} Let me look.\n\u{3000}");
        assert_eq!(rebuilt_content(glm, &padded), "Let me look.");
        assert_eq!(
            rebuilt_content(None, &padded),
            "\u{1c} Let me look.\n\u{3000}"
        );
        assert_eq!(
            rebuilt_content(glm, &content_then_tool_call(" \n\t")),
            Value::Null
        );
        let no_tools = sse(&[
            role_chunk(),
            chat_chunk(json!({"content": " hi\n"}), Some(&[3]), Some("stop")),
            usage_chunk("chat.completion.chunk"),
        ]);
        assert_eq!(rebuilt_content(glm, &no_tools), " hi\n");
    }

    #[test]
    fn a_tool_call_without_an_id_gets_one() {
        let stream = sse(&[
            role_chunk(),
            chat_chunk(
                json!({"tool_calls": [{"index": 0, "function": {"name": "f", "arguments": "{}"}}]}),
                Some(&[3]),
                Some("tool_calls"),
            ),
            usage_chunk("chat.completion.chunk"),
        ]);
        let got = reassemble(Api::Chat, false, &stream).unwrap();
        let id = got["choices"][0]["message"]["tool_calls"][0]["id"]
            .as_str()
            .unwrap();
        assert!(id.starts_with("chatcmpl-tool-") && id.len() == 46, "{id}");
    }

    #[test]
    fn error_event_carries_status_and_body() {
        let error = json!({"error": {"message": "Internal server error", "type": "InternalServerError",
                                     "param": null, "code": 500}});
        let stream = format!(
            "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
            completion_chunk("a", &[1], None),
            error
        );
        match reassemble(Api::Completions, false, &stream) {
            Err(StreamError::Upstream(e)) => {
                assert_eq!(e.error.code, 500);
                assert_eq!(serde_json::to_value(&e).unwrap(), error);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn interruptions() {
        let cut = format!("data: {}\n\n", completion_chunk("a", &[1], None));
        let e = reassemble(Api::Completions, false, &cut).unwrap_err();
        assert!(matches!(e, StreamError::NoDone) && e.interrupted(), "{e:?}");

        let aborted = sse(&[completion_chunk("a", &[1], Some("abort"))]);
        let e = reassemble(Api::Completions, false, &aborted).unwrap_err();
        assert!(
            matches!(e, StreamError::Aborted) && e.interrupted(),
            "{e:?}"
        );

        let chat_aborted = sse(&[
            role_chunk(),
            chat_chunk(json!({"content": "a"}), Some(&[1]), Some("abort")),
        ]);
        assert!(matches!(
            reassemble(Api::Chat, false, &chat_aborted),
            Err(StreamError::Aborted)
        ));
    }

    /// Zeroes what differs between two runs of one request: IDs, timestamps
    /// and timings.
    fn run_independent(mut v: Value) -> Value {
        fn zero_numbers(v: &mut Value) {
            match v {
                Value::Number(_) => *v = json!(0),
                Value::Object(map) => map.values_mut().for_each(zero_numbers),
                Value::Array(items) => items.iter_mut().for_each(zero_numbers),
                _ => {}
            }
        }
        v["id"] = json!("");
        v["created"] = json!(0);
        if let Some(choices) = v["choices"].as_array_mut() {
            for choice in choices {
                if let Some(calls) = choice["message"]["tool_calls"].as_array_mut() {
                    for call in calls {
                        call["id"] = json!("");
                    }
                }
            }
        }
        zero_numbers(&mut v["metrics"]);
        v
    }

    #[test]
    fn recorded_streams_rebuild_the_recorded_responses() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/vllm");
        let mut cases = Vec::new();
        for version in std::fs::read_dir(&root).unwrap() {
            let version = version.unwrap().path();
            if !version.is_dir() {
                continue;
            }
            for config in std::fs::read_dir(&version).unwrap() {
                for case in std::fs::read_dir(config.unwrap().path()).unwrap() {
                    cases.push(case.unwrap().path());
                }
            }
        }
        cases.sort();
        assert!(
            !cases.is_empty(),
            "no recorded cases under {}",
            root.display()
        );

        for case in &cases {
            let read = |name: &str| std::fs::read(case.join(name)).unwrap();
            let json = |name: &str| serde_json::from_slice::<Value>(&read(name)).unwrap();
            let meta = json("meta.json");
            let api = match meta["endpoint"].as_str().unwrap() {
                "/v1/completions" => Api::Completions,
                "/v1/chat/completions" => Api::Chat,
                other => panic!("{}: endpoint {other}", case.display()),
            };
            let args: Vec<String> = serde_json::from_value(meta["server_args"].clone()).unwrap();
            let parser = args
                .iter()
                .enumerate()
                .find_map(|(i, a)| match a.strip_prefix("--tool-call-parser") {
                    Some("") => args.get(i + 1).cloned(),
                    Some(rest) => rest.strip_prefix('=').map(str::to_owned),
                    None => None,
                })
                .map(|name| serde_json::from_value::<ToolCallParser>(json!(name)).unwrap());
            let request = read("request.json");
            let planned = plan(api, parser, &request)
                .unwrap_or_else(|e| panic!("{}: ineligible: {e}", case.display()));
            assert_eq!(
                serde_json::from_slice::<Value>(&planned.body).unwrap(),
                json("sent.json"),
                "{}: rewritten body",
                case.display()
            );

            let stream = read("stream.sse");
            let recorded = json("response.json");
            for piece in [1, 7, 64, stream.len().max(1)] {
                let mut reassembly = plan(api, parser, &request).unwrap().reassembly;
                let rebuilt = stream
                    .chunks(piece)
                    .try_for_each(|c| reassembly.feed(c))
                    .and_then(|()| reassembly.finish());
                match (meta["response_status"].as_u64().unwrap(), rebuilt) {
                    (200, Ok(body)) => assert_eq!(
                        run_independent(response(body)),
                        run_independent(recorded.clone()),
                        "{}: piece {piece}",
                        case.display()
                    ),
                    (status, Err(StreamError::Upstream(error))) => {
                        assert_eq!(u64::from(error.error.code), status, "{}", case.display());
                        assert_eq!(
                            serde_json::to_value(&error).unwrap(),
                            recorded,
                            "{}",
                            case.display()
                        );
                    }
                    (status, other) => {
                        panic!("{}: status {status}, rebuilt {other:?}", case.display())
                    }
                }
            }
        }
    }

    fn saved(prompt: &[u32], tokens: &[u32], text: &str) -> Progress {
        Progress {
            prompt_token_ids: prompt.to_vec(),
            token_ids: tokens.to_vec(),
            text: text.into(),
            resumes: 1,
        }
    }

    #[test]
    fn a_continuation_extends_the_prompt_and_shrinks_the_limits() {
        let body = br#"{"model":"m","prompt":"hi","max_tokens":5,"min_tokens":3,"temperature":0}"#;
        let progress = saved(&[1, 2, 3], &[10, 11], "ab");
        let p = plan_resuming(Api::Completions, None, false, body, Some(&progress)).unwrap();
        assert!(p.resumed);
        let sent: Value = serde_json::from_slice(&p.body).unwrap();
        assert_eq!(
            sent,
            json!({"model": "m", "prompt": [1, 2, 3, 10, 11], "max_tokens": 3, "min_tokens": 1,
                   "temperature": 0, "stream": true, "stream_options": {"include_usage": true},
                   "return_token_ids": true})
        );

        let unset = br#"{"model":"m","prompt":"hi"}"#;
        let p = plan_resuming(Api::Completions, None, false, unset, Some(&progress)).unwrap();
        let sent: Value = serde_json::from_slice(&p.body).unwrap();
        assert_eq!(
            (sent["max_tokens"].clone(), sent.get("min_tokens")),
            (json!(14), None)
        );
        let null = br#"{"model":"m","prompt":"hi","max_tokens":null}"#;
        let p = plan_resuming(Api::Completions, None, false, null, Some(&progress)).unwrap();
        let sent: Value = serde_json::from_slice(&p.body).unwrap();
        assert_eq!(sent["max_tokens"], 14);
    }

    #[test]
    fn some_requests_restart_instead_of_continuing() {
        let progress = saved(&[1, 2, 3], &[10, 11], "ab");
        let restarts = |api: Api, body: Value| {
            let p = plan_resuming(
                api,
                None,
                false,
                body.to_string().as_bytes(),
                Some(&progress),
            )
            .unwrap();
            let sent: Value = serde_json::from_slice(&p.body).unwrap();
            assert!(!p.resumed, "{body}");
            assert_eq!(sent.get("prompt"), body.get("prompt"), "{body}");
            assert_eq!(sent.get("max_tokens"), body.get("max_tokens"), "{body}");
        };
        restarts(Api::Completions, json!({"prompt": "hi", "max_tokens": 2}));
        restarts(Api::Completions, json!({"prompt": "hi", "stop": "END"}));
        restarts(Api::Completions, json!({"prompt": "hi", "stop": ["END"]}));
        restarts(
            Api::Completions,
            json!({"prompt": "hi", "truncate_prompt_tokens": 8}),
        );
        restarts(
            Api::Chat,
            json!({"messages": [{"role": "user", "content": "hi"}]}),
        );
        let continues = |body: Value| {
            let p = plan_resuming(
                Api::Completions,
                None,
                false,
                body.to_string().as_bytes(),
                Some(&progress),
            )
            .unwrap();
            assert!(p.resumed, "{body}");
        };
        continues(json!({"prompt": "hi", "stop": null}));
        continues(json!({"prompt": "hi", "stop": []}));
        continues(json!({"prompt": "hi", "stop": ""}));
        continues(json!({"prompt": "hi", "stop_token_ids": [7]}));
    }

    #[test]
    fn a_resumed_completion_reads_as_one_generation() {
        let mut first = completion_chunk("cd", &[12], None);
        first["choices"][0]["prompt_token_ids"] = json!([1, 2, 3, 10, 11]);
        let mut usage = usage_chunk("text_completion");
        usage["usage"] = json!({"prompt_tokens": 5, "total_tokens": 8, "completion_tokens": 3,
                                "prompt_tokens_details": {"cached_tokens": 5}});
        let stream = sse(&[
            first,
            completion_chunk("e", &[13, 14], Some("length")),
            usage,
        ]);
        let progress = saved(&[1, 2, 3], &[10, 11], "ab");
        for caller_token_ids in [false, true] {
            let mut r = Reassembly::resume(caller_token_ids, &progress);
            r.feed(stream.as_bytes()).unwrap();
            let got = response(r.finish().unwrap());
            assert_eq!(got["choices"][0]["text"], "abcde");
            assert_eq!(got["choices"][0]["finish_reason"], "length");
            assert_eq!(
                got["usage"],
                json!({"prompt_tokens": 3, "total_tokens": 8, "completion_tokens": 5,
                       "prompt_tokens_details": {"cached_tokens": 3, "created_cache_tokens": null,
                                                 "multimodal_tokens": null},
                       "completion_tokens_details": null})
            );
            let (tokens, prompt) = if caller_token_ids {
                (json!([10, 11, 12, 13, 14]), json!([1, 2, 3]))
            } else {
                (Value::Null, Value::Null)
            };
            assert_eq!(got["choices"][0]["token_ids"], tokens);
            assert_eq!(got["choices"][0]["prompt_token_ids"], prompt);
        }
    }

    fn cut_after(chunks: &[Value]) -> String {
        chunks.iter().map(|c| format!("data: {c}\n\n")).collect()
    }

    #[test]
    fn progress_stops_where_text_and_tokens_split_cleanly() {
        let mut first = completion_chunk("He", &[10], None);
        first["choices"][0]["prompt_token_ids"] = json!([1, 2]);
        let stream = cut_after(&[
            first,
            completion_chunk("llo", &[11], None),
            completion_chunk("", &[12], None),
            completion_chunk("", &[13], None),
        ]);
        let mut r = Reassembly::new(Api::Completions, false, None, true);
        r.feed(stream.as_bytes()).unwrap();
        assert_eq!(r.progress(1), Some(saved(&[1, 2], &[10, 11], "Hello")));

        let multi = cut_after(&[completion_chunk(" 👋", &[14, 15], None)]);
        r.feed(multi.as_bytes()).unwrap();
        assert_eq!(r.progress(1), Some(saved(&[1, 2], &[10, 11], "Hello")));

        r.feed(cut_after(&[completion_chunk("!", &[16], None)]).as_bytes())
            .unwrap();
        assert_eq!(
            r.progress(2),
            Some(Progress {
                resumes: 2,
                ..saved(&[1, 2], &[10, 11, 12, 13, 14, 15, 16], "Hello 👋!")
            })
        );

        r.feed(cut_after(&[completion_chunk("", &[17], Some("stop"))]).as_bytes())
            .unwrap();
        assert_eq!(
            r.progress(2),
            None,
            "a finished generation has nothing to resume"
        );
    }

    #[test]
    fn progress_accumulates_across_resumes_and_needs_a_continuable_request() {
        let mut r = Reassembly::resume(false, &saved(&[1, 2], &[10], "a"));
        assert_eq!(r.progress(2).map(|p| p.token_ids), Some(vec![10]));
        let mut first = completion_chunk("b", &[11], None);
        first["choices"][0]["prompt_token_ids"] = json!([1, 2, 10]);
        r.feed(cut_after(&[first]).as_bytes()).unwrap();
        assert_eq!(
            r.progress(2),
            Some(Progress {
                resumes: 2,
                ..saved(&[1, 2], &[10, 11], "ab")
            })
        );

        let mut not_continuable = Reassembly::new(Api::Completions, false, None, false);
        let mut first = completion_chunk("a", &[10], None);
        first["choices"][0]["prompt_token_ids"] = json!([1, 2]);
        not_continuable
            .feed(cut_after(&[first]).as_bytes())
            .unwrap();
        assert_eq!(not_continuable.progress(1), None);

        let chunks = cut_after(&[
            role_chunk(),
            chat_chunk(json!({"reasoning": "hm"}), Some(&[3]), None),
            chat_chunk(json!({"content": ""}), Some(&[4, 5]), None),
        ]);
        let mut chat = Reassembly::new(Api::Chat, false, None, true);
        chat.feed(chunks.as_bytes()).unwrap();
        assert_eq!(
            chat.progress(1),
            Some(Progress {
                resumes: 1,
                ..saved(&[1, 2], &[3, 4, 5], "")
            }),
            "chat keeps every token, since vLLM derenders from token IDs"
        );
        let mut restarts = Reassembly::new(Api::Chat, false, None, false);
        restarts.feed(chunks.as_bytes()).unwrap();
        assert_eq!(restarts.progress(1), None);
    }

    /// The chat cases of the recorded corpus that carry vLLM's render and
    /// derender responses.
    fn rendered_chat_cases() -> Vec<std::path::PathBuf> {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/vllm");
        let mut cases = Vec::new();
        for version in std::fs::read_dir(&root).unwrap() {
            let version = version.unwrap().path();
            if !version.is_dir() {
                continue;
            }
            for config in std::fs::read_dir(&version).unwrap() {
                for case in std::fs::read_dir(config.unwrap().path()).unwrap() {
                    let case = case.unwrap().path();
                    if case.join("rendered.json").exists() {
                        cases.push(case);
                    }
                }
            }
        }
        cases.sort();
        cases
    }

    fn generate_events(tokens: &[u32], finish_reason: &str) -> String {
        let mut events = Vec::new();
        for (i, id) in tokens.iter().enumerate() {
            let finish = (i + 1 == tokens.len()).then_some(finish_reason);
            events.push(json!({"request_id": "g", "choices": [
                {"index": 0, "finish_reason": finish, "token_ids": [id], "logprobs": null}
            ], "usage": null}));
        }
        events.push(json!({"request_id": "g", "choices": [],
                           "usage": {"prompt_tokens": 1, "total_tokens": 1, "completion_tokens": 0}}));
        sse(&events)
    }

    /// What a resumed chat response may lack against the non-streamed one:
    /// the reasoning token count and prompt token details, which derender
    /// does not give, and the fingerprint and timings of the first attempt.
    fn without_resume_gaps(mut v: Value) -> Value {
        v = run_independent(v);
        for key in ["system_fingerprint", "metrics"] {
            v[key] = Value::Null;
        }
        for key in ["prompt_tokens_details", "completion_tokens_details"] {
            v["usage"][key] = Value::Null;
        }
        v
    }

    #[test]
    fn recorded_chat_resumes_from_every_cut() {
        let cases = rendered_chat_cases();
        assert!(cases.len() >= 9, "rendered chat cases: {cases:?}");
        let mut resumed_cuts = 0;
        for case in &cases {
            let read = |name: &str| std::fs::read(case.join(name)).unwrap();
            let json = |name: &str| serde_json::from_slice::<Value>(&read(name)).unwrap();
            let request = read("request.json");
            let recorded = json("response.json");
            let args: Vec<String> =
                serde_json::from_value(json("meta.json")["server_args"].clone()).unwrap();
            let parser = args
                .iter()
                .find_map(|a| a.strip_prefix("--tool-call-parser="))
                .map(|name| serde_json::from_value::<ToolCallParser>(json!(name)).unwrap());

            let stream = String::from_utf8(read("stream.sse")).unwrap();
            let chunks: Vec<&str> = stream.split_inclusive("\n\n").collect();
            let mut prompt = Vec::new();
            let mut output = Vec::new();
            let mut finish = String::new();
            for chunk in &chunks {
                let Some(data) = chunk.trim().strip_prefix("data: ") else {
                    continue;
                };
                if let Ok(v) = serde_json::from_str::<Value>(data) {
                    if let Some(ids) = v["prompt_token_ids"].as_array() {
                        prompt = ids.iter().map(|i| i.as_u64().unwrap() as u32).collect();
                    }
                    for choice in v["choices"].as_array().into_iter().flatten() {
                        output.extend(
                            choice["token_ids"]
                                .as_array()
                                .into_iter()
                                .flatten()
                                .map(|i| i.as_u64().unwrap() as u32),
                        );
                        if let Some(f) = choice["finish_reason"].as_str() {
                            finish = f.to_owned();
                        }
                    }
                }
            }
            let engine_finish = if finish == "tool_calls" {
                "stop"
            } else {
                finish.as_str()
            };

            for cut in 1..chunks.len() {
                let mut first = plan_resuming(Api::Chat, parser, true, &request, None)
                    .unwrap()
                    .reassembly;
                first.feed(chunks[..cut].concat().as_bytes()).unwrap();
                let Some(progress) = first.progress(1) else {
                    let stops = case.ends_with("h-stop-string");
                    let nothing_yet = first.progress(1).is_none() && cut <= 2;
                    let finished = cut + 3 >= chunks.len();
                    assert!(
                        stops || nothing_yet || finished,
                        "{}: no progress at cut {cut}",
                        case.display()
                    );
                    continue;
                };
                assert_eq!(progress.prompt_token_ids, prompt, "{}", case.display());
                let saved = progress.token_ids.len();
                assert_eq!(progress.token_ids, output[..saved], "{}", case.display());

                let mut continued = continue_chat(&read("rendered.json"), &progress, &request)
                    .unwrap_or_else(|e| panic!("{} cut {cut}: {e}", case.display()));
                assert_eq!(
                    (continued.target, continued.resumed),
                    (Target::Generate, true)
                );
                let body: Value = serde_json::from_slice(&continued.body).unwrap();
                let mut want_ids = prompt.clone();
                want_ids.extend_from_slice(&output[..saved]);
                assert_eq!(body["token_ids"], json!(want_ids), "{}", case.display());
                let rendered_max = json("rendered.json")["sampling_params"]["max_tokens"]
                    .as_u64()
                    .unwrap();
                assert_eq!(
                    body["sampling_params"]["max_tokens"],
                    json!(rendered_max - saved as u64)
                );
                assert_eq!(
                    (body["stream"].clone(), body["return_token_ids"].clone()),
                    (json!(true), json!(true))
                );

                if saved == output.len() {
                    continue;
                }
                continued
                    .reassembly
                    .feed(generate_events(&output[saved..], engine_finish).as_bytes())
                    .unwrap();
                let Finished::Generated {
                    generated,
                    caller_token_ids,
                } = continued.reassembly.finish().unwrap()
                else {
                    panic!("a continued chat generation yields tokens to derender");
                };
                assert_eq!(generated.token_ids, output, "{}", case.display());
                let resumed = chat::resumed_response(
                    &read("derendered.json"),
                    "chatcmpl-x".into(),
                    1,
                    generated,
                    caller_token_ids,
                )
                .unwrap();
                assert_eq!(
                    without_resume_gaps(serde_json::to_value(&resumed).unwrap()),
                    without_resume_gaps(recorded.clone()),
                    "{} resumed at cut {cut}",
                    case.display()
                );
                resumed_cuts += 1;
            }
        }
        eprintln!("{resumed_cuts} resumed cuts compared");
        assert!(
            resumed_cuts >= 200,
            "only {resumed_cuts} cuts were resumed and compared"
        );
    }

    #[test]
    fn a_chat_continuation_needs_the_saved_prompt_and_a_budget() {
        let rendered = json!({"request_id": "r", "token_ids": [1, 2, 3], "priority": 0,
                              "sampling_params": {"max_tokens": 5, "min_tokens": 3, "temperature": 0.0}})
        .to_string();
        let request = br#"{"messages":[{"role":"user","content":"hi"}],"return_token_ids":true}"#;
        let progress = saved(&[1, 2, 3], &[10, 11], "");
        let p = continue_chat(rendered.as_bytes(), &progress, request).unwrap();
        let body: Value = serde_json::from_slice(&p.body).unwrap();
        assert_eq!(
            body,
            json!({"request_id": "r", "token_ids": [1, 2, 3, 10, 11], "priority": 0,
                   "sampling_params": {"max_tokens": 3, "min_tokens": 1, "temperature": 0.0},
                   "stream": true, "stream_options": {"include_usage": true}, "return_token_ids": true})
        );

        let other_prompt = saved(&[1, 2, 4], &[10], "");
        assert_eq!(
            continue_chat(rendered.as_bytes(), &other_prompt, request).err(),
            Some(Restart::PromptChanged)
        );
        let spent = saved(&[1, 2, 3], &[10, 11, 12, 13, 14], "");
        assert_eq!(
            continue_chat(rendered.as_bytes(), &spent, request).err(),
            Some(Restart::Exhausted)
        );
        assert_eq!(
            continue_chat(b"{}", &progress, request).err(),
            Some(Restart::Unparsable)
        );
        assert_eq!(
            continue_chat(rendered.as_bytes(), &progress, b"nope").err(),
            Some(Restart::Unparsable)
        );
    }

    #[test]
    fn a_generate_stream_needs_a_finish_and_usage() {
        let progress = saved(&[1], &[10], "");
        let mut cut = Reassembly::generate(false, &progress);
        let events = generate_events(&[11, 12], "length");
        let without_done = events.trim_end_matches("data: [DONE]\n\n");
        cut.feed(
            without_done
                .split_inclusive("\n\n")
                .next()
                .unwrap()
                .as_bytes(),
        )
        .unwrap();
        assert_eq!(cut.progress(2).map(|p| p.token_ids), Some(vec![10, 11]));
        assert!(matches!(cut.finish(), Err(StreamError::NoDone)));

        let mut aborted = Reassembly::generate(false, &progress);
        aborted
            .feed(generate_events(&[11], "abort").as_bytes())
            .unwrap_err();

        let mut no_usage = Reassembly::generate(false, &progress);
        let events = generate_events(&[11], "stop");
        let usage_line = events.split_inclusive("\n\n").nth(1).unwrap();
        no_usage
            .feed(events.replace(usage_line, "").as_bytes())
            .unwrap();
        assert!(matches!(no_usage.finish(), Err(StreamError::MissingUsage)));
    }

    #[test]
    fn protocol_violations_are_not_interruptions() {
        let cases: Vec<(Api, String)> = vec![
            (Api::Completions, "data: [DONE]\n\n".into()),
            (Api::Completions, sse(&[completion_chunk("a", &[1], None)])),
            (
                Api::Completions,
                sse(&[completion_chunk("a", &[1], Some("stop"))]),
            ),
            (
                Api::Completions,
                sse(&[
                    completion_chunk("a", &[1], Some("stop")),
                    completion_chunk("b", &[2], None),
                ]),
            ),
            (
                Api::Completions,
                sse(&[json!({"id": "x", "created": 1, "model": "m",
                            "choices": [{"index": 0, "text": "a"}]})]),
            ),
            (
                Api::Completions,
                sse(&[json!({"id": "x", "created": 1, "model": "m",
                            "choices": [{"index": 1, "text": "a", "token_ids": [1]}]})]),
            ),
            (
                Api::Completions,
                "data: {not json\n\ndata: [DONE]\n\n".into(),
            ),
            (Api::Completions, "data: [DONE]\n\ndata: [DONE]\n\n".into()),
            (
                Api::Chat,
                sse(&[
                    chat_chunk(json!({"content": "a"}), Some(&[1]), Some("stop")),
                    usage_chunk("chat.completion.chunk"),
                ]),
            ),
            (
                Api::Chat,
                sse(&[
                    role_chunk(),
                    chat_chunk(json!({"content": "a"}), None, Some("stop")),
                    usage_chunk("chat.completion.chunk"),
                ]),
            ),
            (
                Api::Chat,
                sse(&[
                    role_chunk(),
                    chat_chunk(
                        json!({"tool_calls": [{"index": 0, "function": {"arguments": "{}"}}]}),
                        Some(&[1]),
                        Some("tool_calls"),
                    ),
                    usage_chunk("chat.completion.chunk"),
                ]),
            ),
        ];
        for (api, stream) in cases {
            let e = reassemble(api, false, &stream).unwrap_err();
            assert!(!e.interrupted(), "{stream}: {e:?}");
        }
    }
}
