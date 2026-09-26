//! The OpenAI-compatible routes, called with the official OpenAI Python SDK
//! against a real vLLM behind the processor.

use std::path::PathBuf;
use std::time::Duration;

use serde_json::{Value, json};

use crate::harness::{Postgres, Processor, Spec, eventually};
use crate::vllm::{Case, Gateway, Generate, Vllm, case};

const GENERATE: &str = "/inference/v1/generate";

fn transport(gateway: &Gateway) -> Value {
    json!({"poll_interval_ms": 100, "queues": [{
        "queue_name": "agents", "igw_base_url": gateway.url, "inference_objective": "batch",
        "resumable": true, "render_url": gateway.url,
    }]})
}

/// What the OpenAI SDK got from `create(**request)`.
async fn sdk(p: &Processor, api: &str, request: Value, headers: Value) -> Value {
    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/e2e/openai_client.py");
    let args = json!({"base_url": p.api, "api": api, "request": request, "headers": headers});
    let out = tokio::process::Command::new("uv")
        .args(["run", "--quiet", "--script"])
        .arg(&script)
        .arg(args.to_string())
        .output()
        .await
        .expect("uv runs the OpenAI SDK client");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout)
        .unwrap_or_else(|e| panic!("{e}: {}", String::from_utf8_lossy(&out.stdout)))
}

fn api(c: &Case) -> &'static str {
    if c.endpoint == "/v1/completions" {
        "completions"
    } else {
        "chat"
    }
}

async fn native(vllm: &Vllm, c: &Case) -> Value {
    let r = reqwest::Client::new()
        .post(format!("{}{}", vllm.url, c.endpoint))
        .header("content-type", "application/json")
        .body(c.request.to_string())
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success());
    serde_json::from_slice(&r.bytes().await.unwrap()).unwrap()
}

/// A non-streamed response, without what differs between runs or what
/// derender does not report.
fn comparable(mut v: Value) -> Value {
    for key in ["id", "created", "system_fingerprint"] {
        v[key] = Value::Null;
    }
    v["usage"]["completion_tokens_details"] = Value::Null;
    for choice in v["choices"].as_array_mut().unwrap() {
        choice["stop_reason"] = Value::Null;
        if let Some(calls) = choice
            .pointer_mut("/message/tool_calls")
            .and_then(Value::as_array_mut)
        {
            for call in calls {
                call["id"] = Value::Null;
            }
        }
    }
    v
}

/// What a streaming client should assemble from `response`.
fn assembled(response: &Value) -> Value {
    let choice = &response["choices"][0];
    let mut text = serde_json::Map::new();
    match choice.get("message") {
        Some(message) => {
            for field in ["content", "reasoning"] {
                if let Some(t) = message[field].as_str().filter(|t| !t.is_empty()) {
                    text.insert(field.into(), json!(t));
                }
            }
            if !text.contains_key("content") {
                text.insert("content".into(), json!(""));
            }
        }
        None => {
            text.insert("text".into(), choice["text"].clone());
        }
    }
    let calls: Vec<Value> = choice
        .pointer("/message/tool_calls")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|c| json!({"id": null, "name": c["function"]["name"], "arguments": c["function"]["arguments"]}))
        .collect();
    let mut usage = response["usage"].clone();
    usage["completion_tokens_details"] = Value::Null;
    json!({"text": text, "tool_calls": calls, "finish_reason": choice["finish_reason"], "usage": usage})
}

fn streamed(got: &Value) -> Value {
    let mut s = got["stream"].clone();
    assert!(s.is_object(), "{got}");
    let ids = s["ids"].as_array().unwrap();
    assert_eq!(ids.len(), 1, "one id across the stream");
    s.as_object_mut().unwrap().remove("ids");
    for call in s["tool_calls"].as_array_mut().unwrap() {
        assert!(call["id"].as_str().unwrap().starts_with("chatcmpl-tool-"));
        call["id"] = Value::Null;
    }
    s["usage"]["completion_tokens_details"] = Value::Null;
    s
}

async fn run(vllm: &Vllm, spec: impl FnOnce(Spec) -> Spec) {
    let gateway = Gateway::start(&vllm.url).await;
    let dir = tempfile::tempdir().unwrap();
    let p = Processor::start(dir.path(), spec(Spec::new(transport(&gateway)))).await;

    for c in [case("chat"), case("completion"), case("tool")] {
        let want = native(vllm, &c).await;
        let objective = json!({"x-llm-d-inference-objective": "interactive"});

        gateway.clear();
        let got = sdk(&p, api(&c), c.request.clone(), objective.clone()).await;
        assert_eq!(
            comparable(got["response"].clone()),
            comparable(want.clone()),
            "{}",
            c.endpoint
        );
        let generate = &gateway
            .forwarded()
            .into_iter()
            .find(|f| f.path == GENERATE)
            .unwrap();
        assert_eq!(
            generate.headers["x-llm-d-inference-objective"], "interactive",
            "the caller's objective replaces the queue's"
        );

        let mut streaming = c.request.clone();
        streaming["stream"] = json!(true);
        streaming["stream_options"] = json!({"include_usage": true});
        let got = sdk(&p, api(&c), streaming.clone(), json!({})).await;
        assert_eq!(streamed(&got), assembled(&want), "{} streamed", c.endpoint);

        gateway.clear();
        gateway.then(Generate::Cut(c.cut));
        let got = sdk(&p, api(&c), streaming, json!({})).await;
        assert_eq!(
            streamed(&got),
            assembled(&want),
            "{} streamed and evicted",
            c.endpoint
        );
        assert_eq!(gateway.forwarded_to(GENERATE).len(), 2);
        assert_eq!(
            gateway.forwarded()[0].body["stream"],
            json!(false),
            "a streaming caller's request is queued unstreamed"
        );
    }

    // A deadline the generation cannot meet: 504, streamed or not.
    let chat = case("chat");
    gateway.then(Generate::Stall(1));
    let got = sdk(
        &p,
        "chat",
        chat.request.clone(),
        json!({"x-llm-d-async-timeout": "2s"}),
    )
    .await;
    assert_eq!(got["status"], 504, "{got}");
    assert_eq!(got["body"]["code"], 504, "{got}");
    let mut streaming = chat.request.clone();
    streaming["stream"] = json!(true);
    gateway.then(Generate::Stall(1));
    let got = sdk(
        &p,
        "chat",
        streaming,
        json!({"x-llm-d-async-timeout": "2s"}),
    )
    .await;
    assert_eq!(got["error"], "APIError", "{got}");

    // Unknown queue and bad headers are the caller's error.
    let got = sdk(
        &p,
        "chat",
        chat.request.clone(),
        json!({"x-llm-d-async-queue": "nope"}),
    )
    .await;
    assert_eq!(got["status"], 400, "{got}");
    let got = sdk(
        &p,
        "chat",
        chat.request.clone(),
        json!({"x-llm-d-async-timeout": "soon"}),
    )
    .await;
    assert_eq!(got["status"], 400, "{got}");
}

#[tokio::test(flavor = "multi_thread")]
async fn openai_sdk_through_the_facade() {
    let Some(vllm) = Vllm::start().await else {
        return;
    };
    run(&vllm, |spec| spec).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn openai_sdk_through_the_facade_on_postgres() {
    let Some(db) = Postgres::schema().await else {
        return;
    };
    let Some(vllm) = Vllm::start().await else {
        return;
    };
    run(&vllm, |spec| spec.postgres(&db, "5s")).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_caller_that_goes_away_cancels_its_queued_request() {
    let Some(vllm) = Vllm::start().await else {
        return;
    };
    let gateway = Gateway::start(&vllm.url).await;
    let dir = tempfile::tempdir().unwrap();
    let mut transport = transport(&gateway);
    transport["queues"][0]["gate_type"] = json!("local-max-concurrency");
    transport["queues"][0]["gate_params"] = json!({"limit": 1});
    let p = Processor::start(dir.path(), Spec::new(transport)).await;
    let chat = case("chat");
    gateway.then(Generate::Pause(1, Duration::from_secs(4)));
    let base = p.api.clone();
    let request = chat.request.clone();
    let busy = tokio::spawn(async move {
        reqwest::Client::new()
            .post(format!("{base}/v1/chat/completions"))
            .header("content-type", "application/json")
            .body(request.to_string())
            .send()
            .await
            .unwrap()
            .status()
    });
    eventually("the first request to stall", || async {
        (!gateway.forwarded_to(GENERATE).is_empty()).then_some(())
    })
    .await;

    let impatient = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let base = p.api.clone();
    let queued = tokio::spawn({
        let body = chat.request.to_string();
        async move {
            impatient
                .post(format!("{base}/v1/chat/completions"))
                .header("content-type", "application/json")
                .body(body)
                .send()
                .await
        }
    });
    eventually("the second request to queue", || async {
        (p.queue_depth("agents").await == 1).then_some(())
    })
    .await;
    assert!(queued.await.unwrap().is_err(), "the caller gave up");
    assert_eq!(busy.await.unwrap(), 200);
    eventually("the abandoned request to leave the queue", || async {
        (p.queue_depth("agents").await == 0).then_some(())
    })
    .await;
    assert_eq!(
        gateway
            .forwarded()
            .iter()
            .filter(|f| f.path.ends_with("/render"))
            .count(),
        1,
        "the abandoned request was never sent"
    );
}
