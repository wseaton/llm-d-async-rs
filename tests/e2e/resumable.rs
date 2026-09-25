use serde_json::{Value, json};

use crate::harness::{Processor, Recorded, Reply, Spec, Upstream, now_secs, queue};

const RESULTS: &str = "result-list";

fn transport(queues: Value) -> Value {
    json!({"poll_interval_ms": 100, "queues": queues})
}

fn resumable(name: &str, upstream: &Upstream) -> Value {
    let mut q = queue(name, upstream);
    q["resumable"] = json!(true);
    q
}

fn submission(id: &str, endpoint: &str, payload: Value) -> Value {
    json!({
        "id": id,
        "created": now_secs(),
        "deadline": now_secs() + 60,
        "endpoint": endpoint,
        "payload": payload,
    })
}

fn completion_events(texts: &[&str], finish: bool) -> Vec<String> {
    let mut events = Vec::new();
    for (i, text) in texts.iter().enumerate() {
        let mut choice =
            json!({"index": 0, "text": text, "logprobs": null, "token_ids": [100 + i]});
        if i == 0 {
            choice["prompt_token_ids"] = json!([1, 2, 3]);
        }
        if finish && i == texts.len() - 1 {
            choice["finish_reason"] = json!("stop");
            choice["stop_reason"] = json!(null);
        }
        events.push(
            json!({"id": "cmpl-e2e", "object": "text_completion", "created": 1700000000,
                   "model": "m", "choices": [choice]})
            .to_string(),
        );
    }
    if finish {
        events.push(
            json!({"id": "cmpl-e2e", "object": "text_completion", "created": 1700000000,
                   "model": "m", "choices": [],
                   "usage": {"prompt_tokens": 3, "total_tokens": 3 + texts.len(),
                             "completion_tokens": texts.len()}})
            .to_string(),
        );
        events.push("[DONE]".into());
    }
    events
}

fn completion_result(text: &str, tokens: u64) -> Value {
    json!({
        "id": "cmpl-e2e", "object": "text_completion", "created": 1700000000, "model": "m",
        "choices": [{"index": 0, "text": text, "logprobs": null, "finish_reason": "stop",
                     "stop_reason": null, "token_ids": null, "prompt_logprobs": null,
                     "prompt_token_ids": null, "routed_experts": null}],
        "service_tier": null, "system_fingerprint": null,
        "usage": {"prompt_tokens": 3, "total_tokens": 3 + tokens, "completion_tokens": tokens,
                  "prompt_tokens_details": null, "completion_tokens_details": null},
        "kv_transfer_params": null, "ec_transfer_params": null, "metrics": null,
    })
}

fn chat_events() -> Vec<String> {
    let chunk = |delta: Value, extra: Value| {
        let mut choice = json!({"index": 0, "delta": delta, "logprobs": null});
        choice
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        json!({"id": "chatcmpl-e2e", "object": "chat.completion.chunk", "created": 1700000000,
               "model": "m", "choices": [choice]})
    };
    let mut first = chunk(json!({"role": "assistant", "content": ""}), json!({}));
    first["prompt_token_ids"] = json!([1, 2]);
    vec![
        first.to_string(),
        chunk(json!({"reasoning": "hmm"}), json!({"token_ids": [3]})).to_string(),
        chunk(
            json!({"content": "hi"}),
            json!({"token_ids": [4], "finish_reason": "stop", "stop_reason": null}),
        )
        .to_string(),
        json!({"id": "chatcmpl-e2e", "object": "chat.completion.chunk", "created": 1700000000,
               "model": "m", "choices": [],
               "usage": {"prompt_tokens": 2, "total_tokens": 4, "completion_tokens": 2}})
        .to_string(),
        "[DONE]".into(),
    ]
}

fn streamed(r: &Recorded) -> bool {
    r.json()["stream"] == json!(true)
}

fn result_json(result: &Value) -> Value {
    serde_json::from_str(result["payload"].as_str().unwrap()).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn resumable_queue_streams_eligible_requests_and_rebuilds_the_response() {
    let upstream = Upstream::start().await;
    upstream.reply_with(|r, _| match (r.path.as_str(), streamed(r)) {
        ("/v1/completions", true) => Reply::sse(&completion_events(&["Hel", "lo"], true)),
        ("/v1/chat/completions", true) => Reply::sse(&chat_events()),
        _ => Reply::json(200, json!({"as_submitted": r.json()})),
    });
    let dir = tempfile::tempdir().unwrap();
    let p = Processor::start(
        dir.path(),
        Spec::new(transport(json!([
            resumable("r", &upstream),
            queue("plain", &upstream)
        ]))),
    )
    .await;

    let payload = json!({"model": "m", "prompt": "hi", "temperature": 0.5, "max_tokens": 8});
    p.submit(submission("completion", "/v1/completions", payload.clone()))
        .await;
    let r = p.next_result(RESULTS).await;
    assert_eq!(r["status_code"], 200);
    assert_eq!(result_json(&r), completion_result("Hello", 2));
    let sent = upstream.recorded()[0].json();
    let mut want = payload.clone();
    want["stream"] = json!(true);
    want["stream_options"] = json!({"include_usage": true});
    want["return_token_ids"] = json!(true);
    assert_eq!(sent, want);

    let chat = json!({"model": "m", "messages": [{"role": "user", "content": "hi"}]});
    p.submit(submission("chat", "/v1/chat/completions", chat))
        .await;
    let r = result_json(&p.next_result(RESULTS).await);
    assert_eq!(r["object"], "chat.completion");
    assert_eq!(
        r["choices"][0]["message"],
        json!({"role": "assistant", "content": "hi", "refusal": null, "annotations": null,
               "audio": null, "function_call": null, "reasoning": "hmm"})
    );
    assert_eq!(r["usage"]["completion_tokens"], 2);

    let mut seeded = payload.clone();
    seeded["seed"] = json!(7);
    p.submit(submission("seeded", "/v1/completions", seeded.clone()))
        .await;
    let r = result_json(&p.next_result(RESULTS).await);
    assert_eq!(r, json!({"as_submitted": seeded}));

    let mut plain = submission("plain", "/v1/completions", payload.clone());
    plain["request_queue_name"] = json!("plain");
    p.submit(plain).await;
    let r = result_json(&p.next_result(RESULTS).await);
    assert_eq!(r, json!({"as_submitted": payload}));

    let embeddings = json!({"model": "m", "input": "hi"});
    p.submit(submission(
        "embeddings",
        "/v1/embeddings",
        embeddings.clone(),
    ))
    .await;
    let r = result_json(&p.next_result(RESULTS).await);
    assert_eq!(r, json!({"as_submitted": embeddings}));

    let q = [("queue_name", "r")];
    assert_eq!(
        p.metric("llm_d_async_async_request_retries_total", &q)
            .await,
        None
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_interrupted_chat_restarts_from_scratch() {
    let upstream = Upstream::start().await;
    upstream.reply_with(|_, n| {
        let events = chat_events();
        match n {
            0 => Reply::sse(&events[..2]).cut(),
            1 => Reply::sse(&events[..3]),
            _ => Reply::sse(&events),
        }
    });
    let dir = tempfile::tempdir().unwrap();
    let p = Processor::start(
        dir.path(),
        Spec::new(transport(json!([resumable("r", &upstream)]))),
    )
    .await;

    let chat = json!({"model": "m", "messages": [{"role": "user", "content": "hi"}]});
    p.submit(submission("chat", "/v1/chat/completions", chat))
        .await;
    let r = result_json(&p.next_result(RESULTS).await);
    assert_eq!(r["choices"][0]["message"]["content"], "hi");
    assert_eq!(r["choices"][0]["message"]["reasoning"], "hmm");
    let sent = upstream.recorded();
    assert_eq!(sent.len(), 3);
    assert!(
        sent.iter().all(|s| s.body == sent[0].body),
        "every attempt restarts"
    );
    let q = [("queue_name", "r")];
    assert_eq!(
        p.metric("llm_d_async_async_request_retries_total", &q)
            .await,
        Some(2.0)
    );
    assert_eq!(
        p.metric("llm_d_async_async_request_resumes_total", &q)
            .await,
        None
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn error_events_fail_like_the_non_streamed_request() {
    let error = |code: u16, kind: &str| {
        json!({"error": {"message": format!("{kind} happened"), "type": kind, "param": null,
                         "code": code}})
    };
    let upstream = Upstream::start().await;
    let (server_error, client_error) = (
        error(500, "InternalServerError"),
        error(400, "BadRequestError"),
    );
    upstream.reply_with(move |r, _| {
        let body = r.json();
        let mut events = completion_events(&["a"], false);
        match (body["model"].as_str().unwrap(), body["prompt"].is_string()) {
            ("flaky", true) => {
                events.push(server_error.to_string());
                events.push("[DONE]".into());
                Reply::sse(&events)
            }
            ("flaky", false) => Reply::sse(&continuation_events()),
            _ => {
                events.push(client_error.to_string());
                events.push("[DONE]".into());
                Reply::sse(&events)
            }
        }
    });
    let dir = tempfile::tempdir().unwrap();
    let p = Processor::start(
        dir.path(),
        Spec::new(transport(json!([resumable("r", &upstream)]))),
    )
    .await;

    p.submit(submission(
        "flaky",
        "/v1/completions",
        json!({"model": "flaky", "prompt": "hi"}),
    ))
    .await;
    let r = result_json(&p.next_result(RESULTS).await);
    assert_eq!(r["choices"][0]["text"], "a world");
    assert_eq!(
        upstream.recorded()[1].json()["prompt"],
        json!([1, 2, 3, 100])
    );

    p.submit(submission(
        "bad",
        "/v1/completions",
        json!({"model": "bad", "prompt": "hi"}),
    ))
    .await;
    let r = p.next_result(RESULTS).await;
    assert_eq!(r["status_code"], 400);
    assert_eq!(result_json(&r), error(400, "BadRequestError"));

    let q = [("queue_name", "r")];
    assert_eq!(
        p.metric("llm_d_async_async_request_resumes_total", &q)
            .await,
        Some(1.0)
    );
    assert_eq!(
        p.metric("llm_d_async_async_request_retries_total", &q)
            .await,
        None
    );
    assert_eq!(
        p.metric("llm_d_async_async_failed_requests_total", &q)
            .await,
        Some(1.0)
    );
}

/// The stream of a continuation of `completion_events(&["Hel", "lo"], _)`.
fn continuation_events() -> Vec<String> {
    let chunk = |text: &str, id: u32, first: bool, finish: bool| {
        let mut choice = json!({"index": 0, "text": text, "logprobs": null, "token_ids": [id]});
        if first {
            choice["prompt_token_ids"] = json!([1, 2, 3, 100, 101]);
        }
        if finish {
            choice["finish_reason"] = json!("stop");
            choice["stop_reason"] = json!(null);
        }
        json!({"id": "cmpl-e2e", "object": "text_completion", "created": 1700000000,
               "model": "m", "choices": [choice]})
        .to_string()
    };
    vec![
        chunk(" wor", 102, true, false),
        chunk("ld", 103, false, true),
        json!({"id": "cmpl-e2e", "object": "text_completion", "created": 1700000000,
               "model": "m", "choices": [],
               "usage": {"prompt_tokens": 5, "total_tokens": 7, "completion_tokens": 2}})
        .to_string(),
        "[DONE]".into(),
    ]
}

fn assert_continued(upstream: &Upstream, result: &Value) {
    let sent = upstream.recorded();
    assert_eq!(sent.len(), 2);
    let first = sent[0].json();
    assert_eq!(first["prompt"], "hi");
    assert_eq!(first["max_tokens"], 8);
    let second = sent[1].json();
    assert_eq!(second["prompt"], json!([1, 2, 3, 100, 101]));
    assert_eq!(second["max_tokens"], 6);
    assert_eq!(second["stream"], true);
    assert_eq!(second["temperature"], 0);

    assert_eq!(result["status_code"], 200);
    let r = result_json(result);
    assert_eq!(r["choices"][0]["text"], "Hello world");
    assert_eq!(r["choices"][0]["finish_reason"], "stop");
    assert_eq!(r["choices"][0]["token_ids"], Value::Null);
    assert_eq!(
        r["usage"],
        json!({"prompt_tokens": 3, "total_tokens": 7, "completion_tokens": 4,
               "prompt_tokens_details": null, "completion_tokens_details": null})
    );
}

fn continuing_payload() -> Value {
    json!({"model": "m", "prompt": "hi", "max_tokens": 8, "temperature": 0})
}

#[tokio::test(flavor = "multi_thread")]
async fn an_interrupted_completion_continues_from_its_saved_tokens() {
    let upstream = Upstream::start().await;
    upstream.reply_with(|_, n| match n {
        0 => Reply::sse(&completion_events(&["Hel", "lo"], false)).cut(),
        _ => Reply::sse(&continuation_events()),
    });
    let dir = tempfile::tempdir().unwrap();
    let p = Processor::start(
        dir.path(),
        Spec::new(transport(json!([resumable("r", &upstream)]))),
    )
    .await;

    p.submit(submission("cut", "/v1/completions", continuing_payload()))
        .await;
    let r = p.next_result(RESULTS).await;
    assert_continued(&upstream, &r);

    let q = [("queue_name", "r")];
    assert_eq!(
        p.metric("llm_d_async_async_request_resumes_total", &q)
            .await,
        Some(1.0)
    );
    assert_eq!(
        p.metric("llm_d_async_async_resumed_tokens_total", &q).await,
        Some(2.0)
    );
    assert_eq!(
        p.metric("llm_d_async_async_request_retries_total", &q)
            .await,
        None
    );
    assert_eq!(
        p.metric("llm_d_async_async_failed_requests_total", &q)
            .await,
        None
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_draining_replica_saves_progress_and_the_next_one_continues() {
    let upstream = Upstream::start().await;
    upstream.reply_with(|_, n| match n {
        0 => Reply::sse(&completion_events(&["Hel", "lo"], false)).stall(),
        _ => Reply::sse(&continuation_events()),
    });
    let dir = tempfile::tempdir().unwrap();
    let mut p = Processor::start(
        dir.path(),
        Spec::new(transport(json!([resumable("r", &upstream)]))).arg("--drain-timeout", "1s"),
    )
    .await;

    p.submit(submission(
        "drained",
        "/v1/completions",
        continuing_payload(),
    ))
    .await;
    upstream.wait_for_requests(1).await;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    p.signal("-TERM");
    assert!(p.wait_exit(crate::harness::WAIT).await.success());
    assert_eq!(upstream.count(), 1);

    p.spawn().await;
    let r = p.next_result(RESULTS).await;
    assert_continued(&upstream, &r);
    assert_eq!(
        p.metric(
            "llm_d_async_async_request_resumes_total",
            &[("queue_name", "r")]
        )
        .await,
        None,
        "the drained replica counted the resume"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_shed_continuation_keeps_its_progress() {
    let upstream = Upstream::start().await;
    upstream.reply_with(|_, n| match n {
        0 => Reply::sse(&completion_events(&["Hel", "lo"], false)).cut(),
        1 => Reply::json(429, json!({"error": "evicted by flow control"}))
            .header("retry-after", "0")
            .header("x-llm-d-request-dropped-reason", "evicted"),
        _ => Reply::sse(&continuation_events()),
    });
    let dir = tempfile::tempdir().unwrap();
    let p = Processor::start(
        dir.path(),
        Spec::new(transport(json!([resumable("r", &upstream)]))),
    )
    .await;

    p.submit(submission("shed", "/v1/completions", continuing_payload()))
        .await;
    let r = p.next_result(RESULTS).await;
    let sent = upstream.recorded();
    assert_eq!(sent.len(), 3);
    assert_eq!(sent[1].json()["prompt"], json!([1, 2, 3, 100, 101]));
    assert_eq!(
        sent[2].body, sent[1].body,
        "the shed continuation is sent again as is"
    );
    assert_eq!(result_json(&r)["choices"][0]["text"], "Hello world");
    let q = [("queue_name", "r")];
    for (metric, want) in [
        ("llm_d_async_async_request_resumes_total", 1.0),
        ("llm_d_async_async_resumed_tokens_total", 2.0),
        ("llm_d_async_async_request_retries_total", 1.0),
        ("llm_d_async_async_shedded_requests_total", 1.0),
    ] {
        assert_eq!(p.metric(metric, &q).await, Some(want), "{metric}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_interrupted_chat_continues_through_render_generate_and_derender() {
    let upstream = Upstream::start().await;
    upstream.reply_with(|r, _| match r.path.as_str() {
        "/v1/chat/completions" => Reply::sse(&chat_events()[..2]).cut(),
        "/v1/chat/completions/render" => Reply::json(
            200,
            json!({"request_id": "chatcmpl-r", "token_ids": [1, 2], "priority": 0,
                   "sampling_params": {"max_tokens": 10, "temperature": 0.0}}),
        ),
        "/inference/v1/generate" => {
            let chunk = |ids: &[u32], finish: Option<&str>| {
                json!({"request_id": "g", "choices": [
                    {"index": 0, "finish_reason": finish, "token_ids": ids, "logprobs": null}
                ], "usage": null})
                .to_string()
            };
            Reply::sse(&[
                chunk(&[4], None),
                chunk(&[5], Some("stop")),
                json!({"request_id": "g", "choices": [],
                       "usage": {"prompt_tokens": 3, "total_tokens": 5, "completion_tokens": 2}})
                .to_string(),
                "[DONE]".into(),
            ])
        }
        "/v1/chat/completions/derender" => Reply::json(
            200,
            json!({"id": "chatcmpl-d", "object": "chat.completion", "created": 1, "model": "m",
                   "choices": [{"index": 0, "finish_reason": "stop", "message": {
                       "role": "assistant", "content": "hi there", "reasoning": "hmm",
                       "tool_calls": [{"id": "x", "type": "function",
                                       "function": {"name": "f", "arguments": "{}"}}]}}],
                   "usage": {"prompt_tokens": 2, "total_tokens": 5, "completion_tokens": 3}}),
        ),
        other => Reply::json(404, json!({"error": other})),
    });
    let dir = tempfile::tempdir().unwrap();
    let mut q = resumable("r", &upstream);
    q["render_url"] = json!(upstream.url);
    q["tool_call_parser"] = json!("glm47");
    let p = Processor::start(dir.path(), Spec::new(transport(json!([q])))).await;

    let chat = json!({"model": "m", "messages": [{"role": "user", "content": "hi"}],
                      "tools": [{"type": "function", "function": {"name": "f", "parameters": {}}}],
                      "max_tokens": 10});
    p.submit(submission("chat", "/v1/chat/completions", chat.clone()))
        .await;
    let r = p.next_result(RESULTS).await;
    assert_eq!(r["status_code"], 200);
    let r = result_json(&r);

    let sent: Vec<(String, Value)> = upstream
        .recorded()
        .iter()
        .map(|s| (s.path.clone(), s.json()))
        .collect();
    let paths: Vec<&str> = sent.iter().map(|(p, _)| p.as_str()).collect();
    assert_eq!(
        paths,
        [
            "/v1/chat/completions",
            "/v1/chat/completions/render",
            "/inference/v1/generate",
            "/v1/chat/completions/derender"
        ]
    );
    assert_eq!(
        sent[1].1, chat,
        "render gets the caller's request as submitted"
    );
    let generate = &sent[2].1;
    assert_eq!(generate["token_ids"], json!([1, 2, 3]));
    assert_eq!(
        generate["sampling_params"],
        json!({"max_tokens": 9, "temperature": 0.0})
    );
    assert_eq!(
        (
            generate["stream"].clone(),
            generate["return_token_ids"].clone()
        ),
        (json!(true), json!(true))
    );
    let derender = &sent[3].1;
    assert_eq!(derender["chat_request"], chat);
    assert_eq!(derender["prompt_tokens"], 2);
    assert_eq!(
        derender["generate_response"]["prompt_token_ids"],
        json!([1, 2])
    );
    assert_eq!(
        derender["generate_response"]["choices"],
        json!([{"index": 0, "token_ids": [3, 4, 5], "finish_reason": "stop"}])
    );

    assert_eq!(r["object"], "chat.completion");
    let choice = &r["choices"][0];
    assert_eq!(choice["finish_reason"], "tool_calls");
    assert_eq!(choice["message"]["content"], "hi there");
    assert_eq!(choice["message"]["reasoning"], "hmm");
    let call = &choice["message"]["tool_calls"][0];
    assert!(
        call["id"].as_str().unwrap().starts_with("chatcmpl-tool-"),
        "{call}"
    );
    assert_eq!(call["function"], json!({"name": "f", "arguments": "{}"}));
    assert_eq!(
        r["usage"],
        json!({"prompt_tokens": 2, "total_tokens": 5, "completion_tokens": 3,
               "prompt_tokens_details": null, "completion_tokens_details": null})
    );
    let q = [("queue_name", "r")];
    assert_eq!(
        p.metric("llm_d_async_async_request_resumes_total", &q)
            .await,
        Some(1.0)
    );
}
