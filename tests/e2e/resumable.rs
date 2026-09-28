//! Resumable queues against a real vLLM: the processor renders, generates
//! and derenders through it, and the gateway in between evicts streams. The
//! same scenarios run on every store, so their durability can be compared.

use std::time::{Duration, Instant};

use serde_json::{Value, json};

use crate::harness::{JsonBody, Postgres, Processor, Redis, Spec, now_secs};
use crate::vllm::{Case, Gateway, Generate, Vllm, case};

const RESULTS: &str = "result-list";
const GENERATE: &str = "/inference/v1/generate";

fn transport(gateway: &Gateway) -> Value {
    json!({"poll_interval_ms": 100, "queues": [{
        "queue_name": "r", "igw_base_url": gateway.url,
        "render_url": gateway.url,
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

async fn metric(p: &Processor, name: &str) -> f64 {
    p.metric(name, &[("queue_name", "r")]).await.unwrap_or(0.0)
}

/// The store a run uses. Shared stores return a dead replica's claims
/// once this lease lapses.
enum Backend {
    Embedded,
    Postgres(Postgres),
    Redis(Redis),
}

const LEASE: &str = "3s";

impl Backend {
    fn name(&self) -> &'static str {
        match self {
            Self::Embedded => "embedded",
            Self::Postgres(_) => "postgres",
            Self::Redis(_) => "redis",
        }
    }

    fn spec(&self, gateway: &Gateway) -> Spec {
        let spec = Spec::new(transport(gateway)).arg("--drain-timeout", "1s");
        match self {
            Self::Embedded => spec,
            Self::Postgres(db) => spec.postgres(db, LEASE),
            Self::Redis(r) => spec.redis(r, LEASE),
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn resumable_queues_on_the_embedded_store() {
    scenarios(Backend::Embedded).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn resumable_queues_on_postgres() {
    let Some(db) = Postgres::schema().await else {
        return;
    };
    scenarios(Backend::Postgres(db)).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn resumable_queues_on_redis() {
    let Some(r) = Redis::start().await else {
        return;
    };
    scenarios(Backend::Redis(r)).await;
}

async fn scenarios(backend: Backend) {
    let Some(vllm) = Vllm::start().await else {
        return;
    };
    let gateway = Gateway::start(&vllm.url).await;
    let dir = tempfile::tempdir().unwrap();
    let mut p = Processor::start(dir.path(), backend.spec(&gateway)).await;

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

    // A replica killed mid-generation saved nothing: its claim returns to
    // the queue when the store gives it up, and the generation restarts.
    gateway.clear();
    gateway.then(Generate::Stall(chat.cut));
    p.submit(submission("killed", chat)).await;
    crate::harness::eventually("the stalled stream", || async {
        (gateway.forwarded_to(GENERATE).len() == 1).then_some(())
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let killed = Instant::now();
    p.kill().await;
    p.spawn().await;
    assert_eq!(comparable(body(&p.next_result(RESULTS).await)), want[0]);
    let redelivered = killed.elapsed();
    let sent = gateway.forwarded_to(GENERATE);
    assert_eq!(sent.len(), 2);
    assert_eq!(sent[1]["token_ids"], json!(chat.prompt_token_ids));
    eprintln!(
        "{}: a request held by a killed replica finished {redelivered:?} after the kill",
        backend.name()
    );

    if let Backend::Redis(r) = &backend {
        go_producer_request(r, &gateway, chat, &want[0]).await;
    }
}

/// A request submitted the way upstream's Go producer submits it is served
/// through the token layer and answered into its per-request mailbox, the
/// way the llm-d-router coordinator's async-broker reads it.
async fn go_producer_request(r: &Redis, gateway: &Gateway, chat: &Case, want: &Value) {
    gateway.clear();
    gateway.then(Generate::Cut(chat.cut));
    let (id, token) = ("acme:job-1", "9f86d081884c7d659a2feaa0c55ad015");
    let mailbox = format!("results:req:{id}");
    let deadline = now_secs() + 120;
    let member = json!({
        "internal": {"request_token": token, "request_queue_name": "r", "result_queue_name": mailbox},
        "request_kind": "redis",
        "data": {
            "id": id, "created": now_secs(), "deadline": deadline, "payload": chat.request,
            "metadata": {"userid": "acme"}, "endpoint": chat.endpoint,
            "request_queue_name": "r", "result_queue_name": mailbox,
        },
    });
    let mut conn = r.conn().await;
    let () = ::redis::pipe()
        .atomic()
        .del(format!("request-cancel:{id}"))
        .ignore()
        .cmd("SET")
        .arg(format!("request-active:{id}"))
        .arg(token)
        .arg("EX")
        .arg(120)
        .ignore()
        .zadd("r", member.to_string(), deadline)
        .ignore()
        .query_async(&mut conn)
        .await
        .unwrap();
    let head: Vec<String> = crate::harness::eventually("the mailbox result", || {
        let mut conn = conn.clone();
        let mailbox = mailbox.clone();
        async move {
            let head: Vec<String> = ::redis::cmd("LRANGE")
                .arg(&mailbox)
                .arg(0)
                .arg(0)
                .query_async(&mut conn)
                .await
                .ok()?;
            (!head.is_empty()).then_some(head)
        }
    })
    .await;
    let result: Value = serde_json::from_str(&head[0]).unwrap();
    assert_eq!(result["id"], id);
    assert_eq!(result["request_token"], token);
    assert_eq!(comparable(body(&result)), *want);
    let sent = gateway.forwarded_to(GENERATE);
    assert_eq!(sent.len(), 2, "evicted once and resumed");
    let active: bool = ::redis::cmd("EXISTS")
        .arg(format!("request-active:{id}"))
        .query_async(&mut conn)
        .await
        .unwrap();
    assert!(!active, "the coordinator sees the request finished");
}
