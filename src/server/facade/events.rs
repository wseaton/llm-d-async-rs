//! The event stream a `stream: true` caller gets, built from the finished
//! non-streamed response in the chunk shapes vLLM streams: a role chunk,
//! then reasoning, content and each tool call, a chunk with the finish
//! reason, the usage chunk when the caller asked for it, and `[DONE]`.

use serde_json::{Map, Value, json};

use crate::worker::vllm::Api;

pub const DONE: &str = "data: [DONE]\n\n";

pub fn event(data: &Value) -> String {
    format!("data: {data}\n\n")
}

/// The chunks of `response`, a chat completion or completion, without
/// `[DONE]`. `None` when it is not a single-choice response of that API.
pub fn chunks(api: Api, response: &Value, include_usage: bool) -> Option<Vec<Value>> {
    let [choice] = response.get("choices")?.as_array()?.as_slice() else {
        return None;
    };
    let mut head = Map::new();
    for key in ["id", "created", "model", "system_fingerprint"] {
        if let Some(value) = response.get(key) {
            head.insert(key.to_owned(), value.clone());
        }
    }
    let object = match api {
        Api::Chat => "chat.completion.chunk",
        Api::Completions => "text_completion",
    };
    head.insert("object".into(), object.into());
    let chunk = |choice: Value| {
        let mut c = head.clone();
        c.insert("choices".into(), json!([choice]));
        Value::Object(c)
    };
    let finish = json!({
        "finish_reason": choice.get("finish_reason").cloned().unwrap_or(Value::Null),
        "stop_reason": choice.get("stop_reason").cloned().unwrap_or(Value::Null),
    });
    let mut out = Vec::new();
    match api {
        Api::Chat => {
            let message = choice.get("message")?;
            let delta =
                |d: Value| json!({"index": 0, "delta": d, "logprobs": null, "finish_reason": null});
            out.push(chunk(delta(json!({"role": "assistant", "content": ""}))));
            for field in ["reasoning", "content"] {
                if let Some(text) = message.get(field).and_then(Value::as_str)
                    && !text.is_empty()
                {
                    out.push(chunk(delta(json!({ field: text }))));
                }
            }
            let calls = message.get("tool_calls").and_then(Value::as_array);
            for (index, call) in calls.into_iter().flatten().enumerate() {
                let call = json!([{
                    "index": index,
                    "id": call.get("id"),
                    "type": "function",
                    "function": call.get("function"),
                }]);
                out.push(chunk(delta(json!({ "tool_calls": call }))));
            }
            let mut last = json!({"index": 0, "delta": {}, "logprobs": null});
            merge(&mut last, finish);
            out.push(chunk(last));
        }
        Api::Completions => {
            let text = choice.get("text").and_then(Value::as_str).unwrap_or("");
            let mut last = json!({"index": 0, "text": text, "logprobs": null});
            merge(&mut last, finish);
            out.push(chunk(last));
        }
    }
    if include_usage {
        let mut c = head.clone();
        c.insert("choices".into(), json!([]));
        c.insert(
            "usage".into(),
            response.get("usage").cloned().unwrap_or(Value::Null),
        );
        out.push(Value::Object(c));
    }
    Some(out)
}

fn merge(into: &mut Value, from: Value) {
    if let (Some(into), Value::Object(from)) = (into.as_object_mut(), from) {
        into.extend(from);
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use serde_json::{Value, json};

    use crate::server::facade::events::chunks;
    use crate::worker::vllm::Api;

    /// What a client assembles from chunks: the text of each message field,
    /// tool calls by index, the finish reason and the usage.
    fn assembled(api: Api, chunks: &[Value]) -> Value {
        let mut text = serde_json::Map::new();
        let mut calls: Vec<Value> = Vec::new();
        let mut finish = Value::Null;
        let mut usage = Value::Null;
        let mut role = Value::Null;
        for c in chunks {
            if let Some(u) = c.get("usage").filter(|u| !u.is_null()) {
                usage = u.clone();
            }
            for choice in c["choices"].as_array().unwrap() {
                if let Some(f) = choice.get("finish_reason").filter(|f| !f.is_null()) {
                    finish = f.clone();
                }
                let delta = match api {
                    Api::Chat => &choice["delta"],
                    Api::Completions => choice,
                };
                if let Some(r) = delta.get("role").filter(|r| !r.is_null()) {
                    role = r.clone();
                }
                for field in ["text", "content", "reasoning"] {
                    if let Some(t) = delta.get(field).and_then(Value::as_str) {
                        let entry = text.entry(field).or_insert(json!(""));
                        *entry = json!(format!("{}{t}", entry.as_str().unwrap()));
                    }
                }
                for call in delta["tool_calls"].as_array().into_iter().flatten() {
                    let i = call["index"].as_u64().unwrap() as usize;
                    if calls.len() <= i {
                        calls.push(json!({"name": "", "arguments": ""}));
                    }
                    for field in ["name", "arguments"] {
                        if let Some(t) = call["function"][field].as_str() {
                            let now = calls[i][field].as_str().unwrap().to_owned();
                            calls[i][field] = json!(format!("{now}{t}"));
                        }
                    }
                }
            }
        }
        json!({"role": role, "text": text, "tool_calls": calls, "finish": finish, "usage": usage})
    }

    fn sse_chunks(sse: &str) -> Vec<Value> {
        sse.split("\n\n")
            .filter_map(|e| e.strip_prefix("data: "))
            .filter(|d| *d != "[DONE]")
            .map(|d| serde_json::from_str(d).unwrap())
            .collect()
    }

    #[test]
    fn synthesized_streams_assemble_like_vllms_streams() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/vllm/v0.30.0");
        let mut compared = 0;
        for config in ["default", "extras"] {
            for dir in std::fs::read_dir(root.join(config)).unwrap() {
                let dir = dir.unwrap().path();
                let meta: Value =
                    serde_json::from_slice(&std::fs::read(dir.join("meta.json")).unwrap()).unwrap();
                if meta["response_status"] != 200 {
                    continue;
                }
                let api = match meta["endpoint"].as_str().unwrap() {
                    "/v1/completions" => Api::Completions,
                    _ => Api::Chat,
                };
                let response: Value =
                    serde_json::from_slice(&std::fs::read(dir.join("response.json")).unwrap())
                        .unwrap();
                let native = sse_chunks(&std::fs::read_to_string(dir.join("stream.sse")).unwrap());
                let ours = chunks(api, &response, true).unwrap();
                let mut want = assembled(api, &native);
                let mut got = assembled(api, &ours);
                for v in [&mut want, &mut got] {
                    v["usage"]["prompt_tokens_details"] = Value::Null;
                    v["usage"]["completion_tokens_details"] = Value::Null;
                    // vLLM strips content around tool calls only when it
                    // does not stream.
                    let called = !v["tool_calls"].as_array().unwrap().is_empty();
                    if let Some(content) = v["text"]["content"].as_str().filter(|_| called) {
                        v["text"]["content"] = json!(content.trim());
                    }
                }
                assert_eq!(got, want, "{}", dir.display());
                for c in &ours {
                    assert_eq!(c["id"], response["id"]);
                    assert_eq!(c["model"], response["model"]);
                }
                compared += 1;
            }
        }
        assert!(compared >= 30, "{compared}");
    }

    #[test]
    fn usage_only_when_asked() {
        let response = json!({"id": "c", "created": 1, "model": "m", "usage": {"prompt_tokens": 1},
            "choices": [{"index": 0, "text": "hi", "finish_reason": "stop", "stop_reason": null}]});
        let with = chunks(Api::Completions, &response, true).unwrap();
        let without = chunks(Api::Completions, &response, false).unwrap();
        assert_eq!(with.len(), without.len() + 1);
        assert_eq!(with.last().unwrap()["usage"], json!({"prompt_tokens": 1}));
        assert!(without.iter().all(|c| c.get("usage").is_none()));
        assert_eq!(without[0]["object"], "text_completion");
    }

    #[test]
    fn not_one_choice() {
        for response in [
            json!({}),
            json!({"choices": []}),
            json!({"choices": [{}, {}]}),
        ] {
            assert!(chunks(Api::Completions, &response, false).is_none());
        }
        assert!(chunks(Api::Chat, &json!({"choices": [{"text": "x"}]}), false).is_none());
    }
}
