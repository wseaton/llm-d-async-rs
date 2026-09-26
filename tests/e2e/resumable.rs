//! Resumable queues against a real vLLM: the processor renders, generates
//! and derenders through it, and the gateway in between evicts streams.

use serde_json::{Value, json};

use crate::harness::{JsonBody, Processor, Spec, now_secs};
use crate::vllm::{Case, Gateway, Generate, Vllm, case};

const RESULTS: &str = "result-list";
const GENERATE: &str = "/inference/v1/generate";

fn transport(gateway: &Gateway) -> Value {
    json!({"poll_interval_ms": 100, "queues": [{
        "queue_name": "r", "igw_base_url": gateway.url,
        "resumable": true, "render_url": gateway.url,
        "generate_epp_profile": "decode",
    }]})
}

fn submission(id: &str, c: &Case) -> Value {
    json!({
        "id": id,
        "created": now_secs(),
        "deadline": now_secs() + 120,
        "endpoint": c.endpoint,
        "payload": c.request,
    })
}

fn body(result: &Value) -> Value {
    assert_eq!(result["status_code"], 200, "{result}");
    serde_json::from_str(result["payload"].as_str().unwrap()).unwrap()
}

/// What differs between two runs of one request, and what derender does not
/// report: the system fingerprint, the stop reason, and reasoning token
/// details.
fn comparable(mut v: Value) -> Value {
    v["id"] = json!("");
    v["created"] = json!(0);
    v["system_fingerprint"] = Value::Null;
    v["usage"]["completion_tokens_details"] = Value::Null;
    for choice in v["choices"].as_array_mut().unwrap() {
        choice["stop_reason"] = Value::Null;
        if let Some(calls) = choice
            .pointer_mut("/message/tool_calls")
            .and_then(Value::as_array_mut)
        {
            for call in calls {
                assert!(call["id"].as_str().unwrap().starts_with("chatcmpl-tool-"));
                call["id"] = json!("");
            }
        }
    }
    v
}

/// vLLM's own response to the non-streamed request.
async fn native(vllm: &Vllm, c: &Case) -> Value {
    let r = reqwest::Client::new()
        .post(format!("{}{}", vllm.url, c.endpoint))
        .json_body(&c.request)
        .send()
        .await
        .unwrap();
    assert!(r.status().is_success(), "{}", r.status());
    serde_json::from_slice(&r.bytes().await.unwrap()).unwrap()
}

/// The `EPP-Profile` header of each forwarded request, in order.
fn profiles(gateway: &Gateway) -> Vec<Option<String>> {
    gateway
        .forwarded()
        .iter()
        .map(|f| {
            f.headers
                .get("epp-profile")
                .map(|v| v.to_str().unwrap().to_owned())
        })
        .collect()
}

async fn metric(p: &Processor, name: &str) -> f64 {
    p.metric(name, &[("queue_name", "r")]).await.unwrap_or(0.0)
}

#[tokio::test(flavor = "multi_thread")]
async fn resumable_queues_against_real_vllm() {
    let Some(vllm) = Vllm::start().await else {
        return;
    };
    let gateway = Gateway::start(&vllm.url).await;
    let dir = tempfile::tempdir().unwrap();
    let mut p = Processor::start(
        dir.path(),
        Spec::new(transport(&gateway)).arg("--drain-timeout", "1s"),
    )
    .await;

    let cases = [case("chat"), case("completion"), case("tool")];
    let mut want = Vec::new();
    for c in &cases {
        want.push(comparable(native(&vllm, c).await));
    }
    assert_eq!(want[2]["choices"][0]["finish_reason"], "tool_calls");

    // A fresh attempt: render, one generate stream, derender.
    for (i, c) in cases.iter().enumerate() {
        gateway.clear();
        p.submit(submission(&format!("fresh-{i}"), c)).await;
        let got = body(&p.next_result(RESULTS).await);
        assert_eq!(comparable(got), want[i], "{}", c.endpoint);
        let paths: Vec<String> = gateway.forwarded().into_iter().map(|f| f.path).collect();
        assert_eq!(
            paths,
            [
                format!("{}/render", c.endpoint),
                GENERATE.to_owned(),
                format!("{}/derender", c.endpoint)
            ]
        );
        let generate = &gateway.forwarded_to(GENERATE)[0];
        assert_eq!(generate["token_ids"], json!(c.prompt_token_ids));
        assert_eq!(generate["stream"], true);
        assert_eq!(
            profiles(&gateway),
            [None, Some("decode".to_owned()), None],
            "only generate names the EPP profile"
        );
    }

    // Evicted mid-generation: the continuation carries the saved tokens,
    // and the response reads as one uninterrupted generation.
    for (i, c) in cases.iter().enumerate() {
        gateway.clear();
        gateway.then(Generate::Cut(c.cut));
        p.submit(submission(&format!("cut-{i}"), c)).await;
        let got = body(&p.next_result(RESULTS).await);
        assert_eq!(comparable(got), want[i], "{}", c.endpoint);
        let sent = gateway.forwarded_to(GENERATE);
        assert_eq!(sent.len(), 2, "{}", c.endpoint);
        let saved = &c.output_token_ids[..c.cut];
        assert_eq!(
            sent[1]["token_ids"],
            json!([c.prompt_token_ids.as_slice(), saved].concat())
        );
        let budget = |s: &Value| s["sampling_params"]["max_tokens"].as_u64().unwrap();
        assert_eq!(budget(&sent[0]) - budget(&sent[1]), c.cut as u64);
        assert_eq!(
            profiles(&gateway)
                .into_iter()
                .filter(|p| p.is_some())
                .count(),
            2,
            "the continuation names the EPP profile too"
        );
    }
    let cuts: usize = cases.iter().map(|c| c.cut).sum();
    assert_eq!(
        metric(&p, "llm_d_async_async_request_resumes_total").await,
        3.0
    );
    assert_eq!(
        metric(&p, "llm_d_async_async_resumed_tokens_total").await,
        cuts as f64
    );
    assert_eq!(
        metric(&p, "llm_d_async_async_request_retries_total").await,
        0.0
    );

    let chat = &cases[0];

    // A continuation that flow control sheds is sent again as it was.
    gateway.clear();
    gateway.then(Generate::Cut(chat.cut));
    gateway.then(Generate::Shed);
    p.submit(submission("shed", chat)).await;
    assert_eq!(comparable(body(&p.next_result(RESULTS).await)), want[0]);
    let sent = gateway.forwarded_to(GENERATE);
    assert_eq!(sent.len(), 3);
    let rendered_anew = |mut body: Value| {
        body["request_id"] = Value::Null;
        body
    };
    assert_eq!(
        rendered_anew(sent[2].clone()),
        rendered_anew(sent[1].clone()),
        "the shed continuation is sent again"
    );
    assert_eq!(
        metric(&p, "llm_d_async_async_shedded_requests_total").await,
        1.0
    );

    // A derender that fails after the generation finished derenders again,
    // without generating again.
    gateway.clear();
    gateway.fail_derenders(1);
    p.submit(submission("derender", chat)).await;
    assert_eq!(comparable(body(&p.next_result(RESULTS).await)), want[0]);
    assert_eq!(gateway.forwarded_to(GENERATE).len(), 1);
    assert_eq!(
        gateway
            .forwarded_to(&format!("{}/derender", chat.endpoint))
            .len(),
        2
    );

    // A request vLLM rejects fails with vLLM's error, as submitted it would.
    gateway.clear();
    let mut too_long = chat.clone();
    too_long.request["max_tokens"] = json!(1_000_000);
    p.submit(submission("too-long", &too_long)).await;
    let r = p.next_result(RESULTS).await;
    assert_eq!(r["status_code"], 400, "{r}");
    let error: Value = serde_json::from_str(r["payload"].as_str().unwrap()).unwrap();
    assert_eq!(error["error"]["code"], 400, "{error}");
    assert!(gateway.forwarded_to(GENERATE).is_empty());

    // What the token layer cannot reproduce goes to vLLM as submitted.
    gateway.clear();
    let mut seeded = chat.clone();
    seeded.request["seed"] = json!(7);
    p.submit(submission("seeded", &seeded)).await;
    assert_eq!(
        body(&p.next_result(RESULTS).await)["object"],
        "chat.completion"
    );
    let paths: Vec<String> = gateway.forwarded().into_iter().map(|f| f.path).collect();
    assert_eq!(paths, ["/v1/chat/completions"]);
    assert_eq!(gateway.forwarded()[0].body, seeded.request);
    assert_eq!(
        profiles(&gateway),
        [None],
        "a request sent as submitted is left to the gateway's routing"
    );

    // A draining replica saves the output so far; the next one continues.
    gateway.clear();
    gateway.then(Generate::Stall(chat.cut));
    p.submit(submission("drained", chat)).await;
    crate::harness::eventually("the stalled stream", || async {
        (gateway.forwarded_to(GENERATE).len() == 1).then_some(())
    })
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    p.signal("-TERM");
    assert!(p.wait_exit(crate::harness::WAIT).await.success());
    p.spawn().await;
    assert_eq!(comparable(body(&p.next_result(RESULTS).await)), want[0]);
    let sent = gateway.forwarded_to(GENERATE);
    assert_eq!(sent.len(), 2);
    assert_eq!(
        sent[1]["token_ids"],
        json!(
            [
                chat.prompt_token_ids.as_slice(),
                &chat.output_token_ids[..chat.cut]
            ]
            .concat()
        )
    );
}
