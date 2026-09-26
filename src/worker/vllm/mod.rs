//! Sends a resumable request through vLLM's token layer. vLLM renders the
//! caller's completions or chat request into prompt token IDs, the gateway's
//! `/inference/v1/generate` streams the output token IDs, and vLLM derenders
//! the whole output into the response the caller's request would have got.
//! An interrupted generation continues from its saved tokens: the prompt
//! becomes the rendered prompt plus the saved output, and the token limits
//! shrink by the saved count.

pub mod derender;
pub mod generate;
pub mod protocol;
pub mod sse;

use std::collections::BTreeMap;

use bytes::Bytes;
use serde::Serialize;
use serde::de::{DeserializeOwned, IgnoredAny};
use serde_json::value::RawValue;

use crate::api::progress::Progress;
use crate::worker::vllm::generate::{Accumulator, Generated};
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

    pub fn render_path(self) -> &'static str {
        match self {
            Self::Completions => "/v1/completions/render",
            Self::Chat => "/v1/chat/completions/render",
        }
    }

    pub fn derender_path(self) -> &'static str {
        match self {
            Self::Completions => "/v1/completions/derender",
            Self::Chat => "/v1/chat/completions/derender",
        }
    }
}

/// Why a request is sent as submitted instead of through the token layer.
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
    #[error("rendered prompt text requested")]
    PromptText,
    #[error("kv_transfer_params")]
    KvTransfer,
    #[error("stop strings")]
    StopStrings,
    #[error("stop token IDs")]
    StopTokens,
    #[error("prompt truncation")]
    Truncation,
}

/// Why saved progress cannot be continued, so the generation restarts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Restart {
    #[error("the rendered prompt differs from the saved one")]
    PromptChanged,
}

/// A render response vLLM could not have sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("unparsable render response")]
pub struct Unrendered;

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

/// What the token layer needs from an eligible caller's request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Caller {
    pub api: Api,
    /// The caller asked for token IDs in the response.
    pub token_ids: bool,
}

/// Checks that the caller's `payload` for `api` can go through the token
/// layer and continue from saved output with the same result. Every
/// condition here is one derender cannot reproduce or a continuation would
/// sample differently under.
pub fn check(api: Api, payload: &[u8]) -> Result<Caller, Ineligible> {
    use serde_json::Value;

    let fields: Fields = serde_json::from_slice(payload).map_err(|_| Ineligible::Unparsable)?;
    let flag = |name| Ok::<_, Ineligible>(field::<bool>(&fields, name)?.unwrap_or(false));
    let penalty = |name| Ok::<_, Ineligible>(field::<f64>(&fields, name)?.unwrap_or(0.0) != 0.0);
    if flag("stream")? {
        return Err(Ineligible::CallerStreams);
    }
    if present(&fields, "stream_options")? {
        return Err(Ineligible::StreamOptions);
    }
    if field::<u64>(&fields, "n")?.unwrap_or(1) > 1
        || field::<u64>(&fields, "best_of")?.unwrap_or(1) > 1
    {
        return Err(Ineligible::Choices);
    }
    if flag("use_beam_search")? {
        return Err(Ineligible::BeamSearch);
    }
    if present(&fields, "seed")? {
        return Err(Ineligible::Seed);
    }
    let text_format = field::<Value>(&fields, "response_format")?
        .is_none_or(|f| f.get("type").and_then(Value::as_str) == Some("text"));
    if !text_format || present(&fields, "structured_outputs")? {
        return Err(Ineligible::StructuredOutput);
    }
    match field::<Value>(&fields, "tool_choice")? {
        None => {}
        Some(Value::String(choice)) if choice == "auto" || choice == "none" => {}
        Some(_) => return Err(Ineligible::ForcedToolChoice),
    }
    if penalty("presence_penalty")? || penalty("frequency_penalty")? {
        return Err(Ineligible::Penalty);
    }
    let logprobs = !matches!(
        field::<Value>(&fields, "logprobs")?,
        None | Some(Value::Bool(false))
    );
    if logprobs || present(&fields, "prompt_logprobs")? {
        return Err(Ineligible::Logprobs);
    }
    if flag("echo")? {
        return Err(Ineligible::Echo);
    }
    if present(&fields, "kv_transfer_params")? {
        return Err(Ineligible::KvTransfer);
    }
    let stops = match field::<Value>(&fields, "stop")? {
        None => false,
        Some(Value::String(s)) => !s.is_empty(),
        Some(Value::Array(a)) => !a.is_empty(),
        Some(_) => return Err(Ineligible::Unparsable),
    };
    if stops {
        return Err(Ineligible::StopStrings);
    }
    if field::<Vec<IgnoredAny>>(&fields, "stop_token_ids")?.is_some_and(|t| !t.is_empty()) {
        return Err(Ineligible::StopTokens);
    }
    if present(&fields, "truncate_prompt_tokens")? {
        return Err(Ineligible::Truncation);
    }
    match api {
        Api::Completions => check_prompt(&fields)?,
        Api::Chat => {
            if flag("return_prompt_text")? {
                return Err(Ineligible::PromptText);
            }
        }
    }
    Ok(Caller {
        api,
        token_ids: flag("return_token_ids")?,
    })
}

fn check_prompt(fields: &Fields) -> Result<(), Ineligible> {
    let prompt = fields.get("prompt").ok_or(Ineligible::PromptShape)?.get();
    if prompt.starts_with('"') || serde_json::from_str::<Vec<u32>>(prompt).is_ok() {
        Ok(())
    } else {
        Err(Ineligible::PromptShape)
    }
}

/// A field of the `/inference/v1/generate` body.
#[derive(Serialize)]
#[serde(untagged)]
enum Sent<'a> {
    Rendered(&'a RawValue),
    Flag(bool),
    Tokens(Vec<u32>),
    Sampling(serde_json::Map<String, serde_json::Value>),
    StreamOptions { include_usage: bool },
}

/// The one `GenerateRequest` in vLLM's render of a chat request, or of a
/// completions request (a list, one per prompt).
fn rendered_prompt(rendered: &[u8]) -> Result<Fields, Unrendered> {
    let raw: &RawValue = serde_json::from_slice(rendered).map_err(|_| Unrendered)?;
    if raw.get().starts_with('[') {
        let [fields] = serde_json::from_str::<[Fields; 1]>(raw.get()).map_err(|_| Unrendered)?;
        Ok(fields)
    } else {
        serde_json::from_str(raw.get()).map_err(|_| Unrendered)
    }
}

/// A request to send through the token layer.
pub struct Plan {
    pub caller: Caller,
    /// The `/inference/v1/generate` body, or `None` when the saved output
    /// already finished and only derendering is left.
    pub body: Option<Bytes>,
    pub reassembly: Reassembly,
    /// The plan continues saved progress instead of starting over.
    pub resumed: bool,
}

impl Plan {
    /// Derenders `progress`, whose generation already finished.
    pub fn finished(caller: Caller, progress: &Progress) -> Option<Self> {
        let finish_reason = progress.finish_reason.clone()?;
        Some(Self {
            caller,
            body: None,
            reassembly: Reassembly::finished(Generated {
                prompt_token_ids: progress.prompt_token_ids.clone(),
                token_ids: progress.token_ids.clone(),
                finish_reason,
                prompt_tokens_details: None,
            }),
            resumed: true,
        })
    }

    /// The generate body for vLLM's `rendered` prompt, continuing `progress`
    /// when there is some. Every rendered field is sent unchanged except the
    /// prompt, the token limits, and streaming with usage.
    pub fn generate(
        caller: Caller,
        rendered: &[u8],
        progress: Option<&Progress>,
    ) -> Result<Result<Self, Restart>, Unrendered> {
        use serde_json::Value;

        let fields = rendered_prompt(rendered)?;
        let prompt: Vec<u32> = field(&fields, "token_ids")
            .ok()
            .flatten()
            .ok_or(Unrendered)?;
        let mut sampling: serde_json::Map<String, Value> = field(&fields, "sampling_params")
            .ok()
            .flatten()
            .ok_or(Unrendered)?;
        let saved: &[u32] = match progress {
            Some(p) if p.prompt_token_ids != prompt => return Ok(Err(Restart::PromptChanged)),
            Some(p) => &p.token_ids,
            None => &[],
        };
        let count = saved.len() as u64;
        if let Some(max_tokens) = sampling.get("max_tokens").and_then(Value::as_u64) {
            let Some(left) = max_tokens.checked_sub(count).filter(|left| *left > 0) else {
                return Ok(Ok(Self {
                    caller,
                    body: None,
                    reassembly: Reassembly::finished(Generated {
                        prompt_token_ids: prompt,
                        token_ids: saved.to_vec(),
                        finish_reason: "length".to_owned(),
                        prompt_tokens_details: None,
                    }),
                    resumed: true,
                }));
            };
            sampling.insert("max_tokens".into(), left.into());
        }
        if let Some(min) = sampling.get("min_tokens").and_then(Value::as_u64) {
            sampling.insert("min_tokens".into(), min.saturating_sub(count).into());
        }
        let mut continued = prompt.clone();
        continued.extend_from_slice(saved);

        let mut sent: BTreeMap<&str, Sent<'_>> = fields
            .iter()
            .map(|(name, value)| (name.as_str(), Sent::Rendered(value)))
            .collect();
        sent.insert("token_ids", Sent::Tokens(continued));
        sent.insert("sampling_params", Sent::Sampling(sampling));
        sent.insert("stream", Sent::Flag(true));
        sent.insert(
            "stream_options",
            Sent::StreamOptions {
                include_usage: true,
            },
        );
        let body = serde_json::to_vec(&sent).map_err(|_| Unrendered)?;
        Ok(Ok(Self {
            caller,
            body: Some(Bytes::from(body)),
            reassembly: Reassembly::new(Accumulator::new(prompt, saved.to_vec())),
            resumed: progress.is_some(),
        }))
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
    #[error("stream ended without a finish reason")]
    Unfinished,
    #[error("stream ended without usage")]
    MissingUsage,
}

impl StreamError {
    /// The generation was cut short and can be sent again.
    pub fn interrupted(&self) -> bool {
        matches!(self, Self::NoDone | Self::Aborted)
    }
}

/// Collects a generate event stream into the whole output, saved tokens
/// included.
pub struct Reassembly {
    decoder: SseDecoder,
    events: Vec<String>,
    done: bool,
    accumulator: Accumulator,
}

impl Reassembly {
    fn new(accumulator: Accumulator) -> Self {
        Self {
            decoder: SseDecoder::default(),
            events: Vec::new(),
            done: false,
            accumulator,
        }
    }

    fn finished(generated: Generated) -> Self {
        Self {
            done: true,
            ..Self::new(Accumulator::finished(generated))
        }
    }

    /// The output to save, with `resumes` continuations so far.
    pub fn progress(&self, resumes: u32) -> Progress {
        let (prompt, tokens, finish_reason) = self.accumulator.progress();
        Progress {
            prompt_token_ids: prompt.to_vec(),
            token_ids: tokens.to_vec(),
            finish_reason: finish_reason.map(str::to_owned),
            resumes,
        }
    }

    pub fn feed(&mut self, bytes: &[u8]) -> Result<(), StreamError> {
        self.decoder.feed(bytes, &mut self.events)?;
        for data in self.events.drain(..) {
            if self.done {
                return Err(StreamError::AfterDone);
            }
            if data == DONE {
                self.done = true;
                continue;
            }
            match serde_json::from_str(&data) {
                Ok(chunk) => self.accumulator.push(chunk)?,
                Err(e) => {
                    return Err(serde_json::from_str::<ErrorResponse>(&data)
                        .map_or(StreamError::Malformed(e), StreamError::Upstream));
                }
            }
        }
        Ok(())
    }

    /// The whole output, once the stream has ended.
    pub fn finish(&self) -> Result<Generated, StreamError> {
        if !self.done {
            return Err(StreamError::NoDone);
        }
        self.accumulator.finish()
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use serde_json::{Value, json};

    use crate::api::progress::Progress;
    use crate::worker::vllm::generate::Generated;
    use crate::worker::vllm::{
        Api, Caller, Ineligible, Plan, Restart, StreamError, Unrendered, check, derender,
    };

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

    fn with(base: Value, extra: Value) -> Value {
        let mut body = base;
        body.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        body
    }

    #[test]
    fn eligibility() {
        use Ineligible::*;
        let completion = |extra| with(json!({"model": "m", "prompt": "hi"}), extra);
        let chat = |extra| {
            with(
                json!({"model": "m", "messages": [{"role": "user", "content": "hi"}]}),
                extra,
            )
        };
        let ineligible = [
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
            (json!({"frequency_penalty": -1}), Penalty),
            (json!({"logprobs": true}), Logprobs),
            (json!({"logprobs": 2}), Logprobs),
            (json!({"prompt_logprobs": 1}), Logprobs),
            (json!({"echo": true}), Echo),
            (json!({"kv_transfer_params": {}}), KvTransfer),
            (json!({"stop": "x"}), StopStrings),
            (json!({"stop": ["x"]}), StopStrings),
            (json!({"stop": 3}), Unparsable),
            (json!({"stop_token_ids": [7]}), StopTokens),
            (json!({"truncate_prompt_tokens": 10}), Truncation),
            (json!({"n": "two"}), Unparsable),
        ];
        for (extra, want) in ineligible {
            for (api, body) in [
                (Api::Completions, completion(extra.clone())),
                (Api::Chat, chat(extra.clone())),
            ] {
                assert_eq!(
                    check(api, body.to_string().as_bytes()),
                    Err(want),
                    "{api:?} {body}"
                );
            }
        }
        for prompt in [json!(["a", "b"]), json!([[1], [2]]), json!(null)] {
            let body = json!({"model": "m", "prompt": prompt});
            assert_eq!(
                check(Api::Completions, body.to_string().as_bytes()),
                Err(PromptShape)
            );
        }
        assert_eq!(
            check(Api::Completions, br#"{"model":"m"}"#),
            Err(PromptShape)
        );
        assert_eq!(
            check(
                Api::Chat,
                chat(json!({"return_prompt_text": true}))
                    .to_string()
                    .as_bytes()
            ),
            Err(PromptText)
        );
        assert_eq!(check(Api::Chat, b"[1]"), Err(Unparsable));

        let eligible = [
            (Api::Completions, completion(json!({"prompt": [1, 2, 3]}))),
            (
                Api::Completions,
                completion(
                    json!({"stop": [], "stop_token_ids": [], "n": 1, "best_of": 1,
                                  "logprobs": false, "presence_penalty": 0}),
                ),
            ),
            (
                Api::Chat,
                chat(
                    json!({"tools": [{"type": "function", "function": {"name": "f"}}],
                            "tool_choice": "auto"}),
                ),
            ),
            (Api::Chat, chat(json!({"tool_choice": "none"}))),
            (Api::Chat, chat(json!({"include_reasoning": false}))),
            (
                Api::Chat,
                chat(json!({"response_format": {"type": "text"}, "stop": ""})),
            ),
            (Api::Chat, chat(json!({"return_token_ids": false}))),
        ];
        for (api, body) in eligible {
            assert_eq!(
                check(api, body.to_string().as_bytes()),
                Ok(Caller {
                    api,
                    token_ids: false
                }),
                "{body}"
            );
        }
        assert_eq!(
            check(
                Api::Chat,
                chat(json!({"return_token_ids": true}))
                    .to_string()
                    .as_bytes()
            ),
            Ok(Caller {
                api: Api::Chat,
                token_ids: true
            })
        );
    }

    /// A case recorded through vLLM's token layer.
    struct Recorded {
        dir: PathBuf,
        api: Api,
        request: Vec<u8>,
        rendered: Vec<u8>,
        generate: Value,
        stream: Vec<u8>,
        derender: Value,
        derendered: Vec<u8>,
        response: Value,
    }

    fn read(dir: &Path, name: &str) -> Vec<u8> {
        std::fs::read(dir.join(name)).unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
    }

    fn json_of(bytes: &[u8]) -> Value {
        serde_json::from_slice(bytes).unwrap()
    }

    fn recorded() -> Vec<Recorded> {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/vllm/v0.30.0");
        let mut cases = Vec::new();
        for config in ["default", "extras"] {
            let mut dirs: Vec<_> = std::fs::read_dir(root.join(config))
                .unwrap()
                .map(|e| e.unwrap().path())
                .filter(|d| d.join("rendered.json").exists())
                .collect();
            dirs.sort();
            for dir in dirs {
                let meta = json_of(&read(&dir, "meta.json"));
                let api = match meta["endpoint"].as_str().unwrap() {
                    "/v1/completions" => Api::Completions,
                    _ => Api::Chat,
                };
                cases.push(Recorded {
                    api,
                    request: read(&dir, "request.json"),
                    rendered: read(&dir, "rendered.json"),
                    generate: json_of(&read(&dir, "generate.json")),
                    stream: read(&dir, "generate.sse"),
                    derender: json_of(&read(&dir, "derender.json")),
                    derendered: read(&dir, "derendered.json"),
                    response: json_of(&read(&dir, "response.json")),
                    dir,
                });
            }
        }
        assert!(cases.len() >= 30, "only {} recorded cases", cases.len());
        cases
    }

    fn fresh(case: &Recorded) -> Plan {
        let caller = check(case.api, &case.request).unwrap();
        Plan::generate(caller, &case.rendered, None)
            .unwrap()
            .unwrap()
    }

    fn generated(case: &Recorded) -> Generated {
        let mut plan = fresh(case);
        plan.reassembly.feed(&case.stream).unwrap();
        plan.reassembly.finish().unwrap()
    }

    /// What the processor's response has that the derendered one cannot:
    /// per-run IDs and timestamps, and what neither derender nor the generate
    /// stream reports (the system fingerprint, the stop reason, reasoning
    /// token details, prompt token details other than cached tokens,
    /// per-request metrics). Stop strings, whose stop reason and trimming
    /// derender would lose, never go through the token layer.
    fn comparable(mut v: Value) -> Value {
        v["id"] = json!("");
        v["created"] = json!(0);
        v["system_fingerprint"] = Value::Null;
        v["metrics"] = Value::Null;
        let cached = v["usage"]["prompt_tokens_details"]["cached_tokens"].clone();
        v["usage"]["prompt_tokens_details"] = if cached.is_null() {
            Value::Null
        } else {
            json!({ "cached_tokens": cached })
        };
        v["usage"]["completion_tokens_details"] = Value::Null;
        for choice in v["choices"].as_array_mut().unwrap() {
            choice["stop_reason"] = Value::Null;
            if let Some(calls) = choice["message"]["tool_calls"].as_array_mut() {
                for call in calls {
                    let id = call["id"].as_str().unwrap();
                    assert!(
                        id.starts_with("chatcmpl-tool-") && id.len() == 30,
                        "tool call id {id}"
                    );
                    call["id"] = json!("");
                }
            }
        }
        v
    }

    #[test]
    fn plans_send_vllm_the_recorded_generate_bodies() {
        for case in recorded() {
            let plan = fresh(&case);
            assert!(!plan.resumed);
            let body = json_of(plan.body.as_ref().unwrap());
            assert_eq!(body, case.generate, "{}", case.dir.display());
        }
    }

    #[test]
    fn recorded_generations_derender_into_the_non_streamed_responses() {
        for case in recorded() {
            let name = case.dir.file_name().unwrap().to_str().unwrap();
            let caller = check(case.api, &case.request).unwrap();
            let generated = generated(&case);
            let request =
                derender::request(case.api, &generated, &format!("gen-{name}"), &case.request)
                    .unwrap();
            assert_eq!(json_of(&request), case.derender, "{}", case.dir.display());

            let response = derender::response(caller, &case.derendered, generated).unwrap();
            let response = json_of(response.as_bytes());
            assert_eq!(
                comparable(response),
                comparable(case.response.clone()),
                "{}",
                case.dir.display()
            );
        }
    }

    /// The generate stream of the tokens after `from`, as vLLM would send
    /// the continuation.
    fn continuation_stream(tokens: &[u32], from: usize, finish: &str) -> String {
        let mut events = String::new();
        for (i, token) in tokens.iter().enumerate().skip(from) {
            let finish_reason = (i == tokens.len() - 1).then_some(finish);
            let chunk = json!({"request_id": "r", "choices": [{"index": 0, "token_ids": [token],
                               "finish_reason": finish_reason}], "usage": null});
            events += &format!("data: {chunk}\n\n");
        }
        let usage = json!({"request_id": "r", "choices": [],
                           "usage": {"prompt_tokens": 1, "total_tokens": 2, "completion_tokens": 1}});
        events + &format!("data: {usage}\n\ndata: [DONE]\n\n")
    }

    #[test]
    fn every_cut_resumes_into_the_same_generation() {
        let mut resumed = 0;
        for case in recorded() {
            let caller = check(case.api, &case.request).unwrap();
            let whole = generated(&case);
            let max_tokens = case.generate["sampling_params"]["max_tokens"]
                .as_u64()
                .unwrap() as usize;
            for cut in 0..whole.token_ids.len() {
                let saved = Progress {
                    prompt_token_ids: whole.prompt_token_ids.clone(),
                    token_ids: whole.token_ids[..cut].to_vec(),
                    finish_reason: None,
                    resumes: 1,
                };
                let mut plan = Plan::generate(caller, &case.rendered, Some(&saved))
                    .unwrap()
                    .unwrap();
                assert!(plan.resumed);
                let body = json_of(plan.body.as_ref().unwrap());
                let mut want = case.generate.clone();
                want["token_ids"] = json!(
                    [
                        whole.prompt_token_ids.clone(),
                        whole.token_ids[..cut].to_vec()
                    ]
                    .concat()
                );
                want["sampling_params"]["max_tokens"] = json!(max_tokens - cut);
                assert_eq!(body, want, "{} cut at {cut}", case.dir.display());

                let rest = continuation_stream(&whole.token_ids, cut, &whole.finish_reason);
                plan.reassembly.feed(rest.as_bytes()).unwrap();
                assert_eq!(
                    plan.reassembly.finish().unwrap(),
                    Generated {
                        prompt_tokens_details: None,
                        ..whole.clone()
                    },
                    "{} cut at {cut}",
                    case.dir.display()
                );
                resumed += 1;
            }
        }
        assert!(resumed > 400, "only {resumed} cuts resumed");
    }

    fn rendered(max_tokens: Option<u64>, min_tokens: Option<u64>) -> Vec<u8> {
        let mut sampling = json!({"temperature": 0.0});
        if let Some(max) = max_tokens {
            sampling["max_tokens"] = json!(max);
        }
        if let Some(min) = min_tokens {
            sampling["min_tokens"] = json!(min);
        }
        json!({"request_id": "r", "token_ids": [1, 2, 3], "sampling_params": sampling,
               "stream": false, "cache_salt": "s"})
        .to_string()
        .into_bytes()
    }

    const CHAT: Caller = Caller {
        api: Api::Chat,
        token_ids: false,
    };

    fn saved(prompt: &[u32], tokens: &[u32]) -> Progress {
        Progress {
            prompt_token_ids: prompt.to_vec(),
            token_ids: tokens.to_vec(),
            finish_reason: None,
            resumes: 1,
        }
    }

    #[test]
    fn a_continuation_extends_the_prompt_and_shrinks_the_limits() {
        let plan = Plan::generate(
            CHAT,
            &rendered(Some(5), Some(3)),
            Some(&saved(&[1, 2, 3], &[10, 11])),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            json_of(plan.body.as_ref().unwrap()),
            json!({"request_id": "r", "token_ids": [1, 2, 3, 10, 11], "cache_salt": "s",
                   "sampling_params": {"temperature": 0.0, "max_tokens": 3, "min_tokens": 1},
                   "stream": true, "stream_options": {"include_usage": true}})
        );
        assert_eq!(
            plan.reassembly.progress(4),
            Progress {
                prompt_token_ids: vec![1, 2, 3],
                token_ids: vec![10, 11],
                finish_reason: None,
                resumes: 4,
            }
        );

        let past_min = Plan::generate(
            CHAT,
            &rendered(None, Some(1)),
            Some(&saved(&[1, 2, 3], &[10, 11])),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            json_of(past_min.body.as_ref().unwrap())["sampling_params"],
            json!({"temperature": 0.0, "min_tokens": 0})
        );
    }

    #[test]
    fn a_spent_budget_finishes_with_length_without_generating() {
        let plan = Plan::generate(
            CHAT,
            &rendered(Some(2), None),
            Some(&saved(&[1, 2, 3], &[10, 11])),
        )
        .unwrap()
        .unwrap();
        assert!(plan.body.is_none() && plan.resumed);
        assert_eq!(
            plan.reassembly.finish().unwrap(),
            Generated {
                prompt_token_ids: vec![1, 2, 3],
                token_ids: vec![10, 11],
                finish_reason: "length".into(),
                prompt_tokens_details: None,
            }
        );
    }

    #[test]
    fn a_changed_prompt_restarts() {
        assert_eq!(
            Plan::generate(
                CHAT,
                &rendered(Some(5), None),
                Some(&saved(&[1, 2, 4], &[10]))
            )
            .unwrap()
            .err(),
            Some(Restart::PromptChanged)
        );
    }

    #[test]
    fn unrendered() {
        let bad: [&[u8]; 6] = [
            b"nope",
            b"{}",
            br#"{"token_ids": [1], "sampling_params": null}"#,
            br#"{"token_ids": "x", "sampling_params": {}}"#,
            br#"[]"#,
            br#"[{"token_ids": [1], "sampling_params": {}}, {"token_ids": [2], "sampling_params": {}}]"#,
        ];
        for body in bad {
            assert!(
                matches!(Plan::generate(CHAT, body, None), Err(Unrendered)),
                "{}",
                String::from_utf8_lossy(body)
            );
        }
        let one = br#"[{"token_ids": [1], "sampling_params": {}}]"#;
        assert!(matches!(Plan::generate(CHAT, one, None), Ok(Ok(_))));
    }

    #[test]
    fn finished_progress_only_derenders() {
        let mut progress = saved(&[1], &[10, 11]);
        assert!(Plan::finished(CHAT, &progress).is_none());
        progress.finish_reason = Some("stop".into());
        let plan = Plan::finished(CHAT, &progress).unwrap();
        assert!(plan.body.is_none() && plan.resumed);
        assert_eq!(plan.reassembly.progress(1), progress);
        assert_eq!(
            plan.reassembly.finish().unwrap(),
            Generated {
                prompt_token_ids: vec![1],
                token_ids: vec![10, 11],
                finish_reason: "stop".into(),
                prompt_tokens_details: None,
            }
        );
    }

    fn reassemble(stream: &str) -> Result<Generated, StreamError> {
        let mut plan = Plan::generate(
            CHAT,
            &rendered(Some(9), None),
            Some(&saved(&[1, 2, 3], &[10])),
        )
        .unwrap()
        .unwrap();
        plan.reassembly.feed(stream.as_bytes())?;
        plan.reassembly.finish()
    }

    fn event(choices: Value, usage: Value) -> String {
        format!(
            "data: {}\n\n",
            json!({"request_id": "r", "choices": choices, "usage": usage})
        )
    }

    fn tokens(ids: &[u32], finish: Option<&str>) -> String {
        event(
            json!([{"index": 0, "token_ids": ids, "finish_reason": finish}]),
            Value::Null,
        )
    }

    fn usage() -> String {
        event(
            json!([]),
            json!({"prompt_tokens": 1, "total_tokens": 2, "completion_tokens": 1}),
        )
    }

    #[test]
    fn a_cut_keeps_the_output_so_far() {
        let mut plan = Plan::generate(
            CHAT,
            &rendered(Some(9), None),
            Some(&saved(&[1, 2, 3], &[10])),
        )
        .unwrap()
        .unwrap();
        let stream = tokens(&[11], None) + &tokens(&[12], None);
        let (first, second) = stream.split_at(stream.len() / 2 + 3);
        plan.reassembly.feed(first.as_bytes()).unwrap();
        plan.reassembly.feed(second.as_bytes()).unwrap();
        let e = plan.reassembly.finish().unwrap_err();
        assert!(matches!(e, StreamError::NoDone) && e.interrupted(), "{e:?}");
        assert_eq!(plan.reassembly.progress(2).token_ids, vec![10, 11, 12]);

        let e = reassemble(&tokens(&[11], Some("abort"))).unwrap_err();
        assert!(
            matches!(e, StreamError::Aborted) && e.interrupted(),
            "{e:?}"
        );
    }

    #[test]
    fn a_finished_stream_before_derendering_keeps_its_finish() {
        let mut plan = Plan::generate(CHAT, &rendered(Some(9), None), None)
            .unwrap()
            .unwrap();
        let stream =
            tokens(&[11], None) + &tokens(&[12], Some("stop")) + &usage() + "data: [DONE]\n\n";
        plan.reassembly.feed(stream.as_bytes()).unwrap();
        assert_eq!(
            plan.reassembly.progress(1),
            Progress {
                prompt_token_ids: vec![1, 2, 3],
                token_ids: vec![11, 12],
                finish_reason: Some("stop".into()),
                resumes: 1,
            }
        );
    }

    #[test]
    fn protocol_violations_are_not_interruptions() {
        let done = "data: [DONE]\n\n";
        let error = r#"data: {"error": {"message": "boom", "type": "BadRequestError", "param": null, "code": 400}}"#;
        let cases = [
            done.to_owned(),
            tokens(&[11], Some("stop")) + done,
            tokens(&[11], None) + &usage() + done,
            tokens(&[11], Some("stop")) + &tokens(&[12], None) + &usage() + done,
            event(json!([{"index": 0, "finish_reason": "stop"}]), Value::Null) + &usage() + done,
            event(json!([{"index": 1, "token_ids": [11]}]), Value::Null) + done,
            "data: {not json\n\n".to_owned() + done,
            tokens(&[11], Some("stop")) + &usage() + done + done,
            format!("{error}\n\n{done}"),
        ];
        for stream in cases {
            let e = reassemble(&stream).unwrap_err();
            assert!(!e.interrupted(), "{stream}: {e:?}");
        }
        assert!(matches!(
            reassemble(&format!("{error}\n\n")),
            Err(StreamError::Upstream(e)) if e.error.code == 400
        ));
    }

    #[test]
    fn cached_tokens_are_capped_at_the_callers_prompt() {
        let chat = json!({"id": "x", "choices": [{"index": 0, "finish_reason": "stop",
                          "message": {"role": "assistant", "content": "hi"}}],
                          "usage": {"prompt_tokens": 3, "completion_tokens": 4,
                                    "prompt_tokens_details": null}});
        let generated = |cached: u64| Generated {
            prompt_token_ids: vec![1, 2, 3],
            token_ids: vec![7, 8, 9, 10],
            finish_reason: "stop".into(),
            prompt_tokens_details: json!({"cached_tokens": cached}).as_object().cloned(),
        };
        for (cached, want) in [(2, 2), (3, 3), (5, 3)] {
            let out = json_of(
                derender::response(CHAT, chat.to_string().as_bytes(), generated(cached))
                    .unwrap()
                    .as_bytes(),
            );
            assert_eq!(
                out["usage"]["prompt_tokens_details"],
                json!({"cached_tokens": want}),
                "{cached} cached"
            );
        }
        let none = Generated {
            prompt_tokens_details: None,
            ..generated(0)
        };
        let out = json_of(
            derender::response(CHAT, chat.to_string().as_bytes(), none)
                .unwrap()
                .as_bytes(),
        );
        assert_eq!(out["usage"]["prompt_tokens_details"], Value::Null);
    }

    #[test]
    fn caller_token_ids_are_kept_where_the_endpoint_puts_them() {
        let generated = Generated {
            prompt_token_ids: vec![1, 2],
            token_ids: vec![7, 8],
            finish_reason: "stop".into(),
            prompt_tokens_details: None,
        };
        let chat = json!({"id": "x", "choices": [{"index": 0, "finish_reason": "stop",
                          "message": {"role": "assistant", "content": "hi"}, "token_ids": null}],
                          "prompt_token_ids": null});
        let caller = Caller {
            api: Api::Chat,
            token_ids: true,
        };
        let out = json_of(
            derender::response(caller, chat.to_string().as_bytes(), generated.clone())
                .unwrap()
                .as_bytes(),
        );
        assert_eq!(out["prompt_token_ids"], json!([1, 2]));
        assert_eq!(out["choices"][0]["token_ids"], json!([7, 8]));

        let completion = json!({"id": "x", "choices": [{"index": 0, "finish_reason": "stop",
                                "text": "hi", "token_ids": null, "prompt_token_ids": null}]});
        let caller = Caller {
            api: Api::Completions,
            token_ids: true,
        };
        let out = json_of(
            derender::response(caller, completion.to_string().as_bytes(), generated.clone())
                .unwrap()
                .as_bytes(),
        );
        assert_eq!(out["choices"][0]["prompt_token_ids"], json!([1, 2]));
        assert_eq!(out["choices"][0]["token_ids"], json!([7, 8]));
        assert!(out.get("prompt_token_ids").is_none());

        let two = json!({"choices": [{"finish_reason": "stop"}, {"finish_reason": "stop"}]});
        assert!(matches!(
            derender::response(caller, two.to_string().as_bytes(), generated),
            Err(derender::Underendered::Choices(2))
        ));
    }
}
