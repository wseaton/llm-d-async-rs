use std::time::Duration;

use axum::http::StatusCode;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::harness::{JsonBody, Processor, Reply, Spec, Upstream, now_secs, queue, request};

const RESULTS: &str = "result-list";

fn transport(queues: serde_json::Value) -> serde_json::Value {
    json!({"poll_interval_ms": 100, "queues": queues})
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn random_bytes(n: usize) -> Vec<u8> {
    (0..n).map(|_| rand::random::<u8>()).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn json_request_round_trip() {
    let upstream = Upstream::start().await;
    upstream.reply_with(|_, _| {
        Reply::json(
            200,
            json!({"choices": [], "usage": {"prompt_tokens": 7, "completion_tokens": 3}}),
        )
    });
    let dir = tempfile::tempdir().unwrap();
    let mut q = queue("q", &upstream);
    q["inference_objective"] = json!("obj");
    let p = Processor::start(dir.path(), Spec::new(transport(json!([q])))).await;

    // Payload whitespace must reach the gateway byte for byte.
    let body = format!(
        r#"{{"id":"r1","deadline":{},"payload":{{"model":"m",  "prompt": "hi"}},
            "headers":{{"X-Custom":"1","x-llm-d-inference-fairness-id":"spoofed"}},
            "metadata":{{"userid":"tenant-a"}},"endpoint":"/v1/chat/completions"}}"#,
        now_secs() + 60
    );
    let r = p
        .http
        .post(format!("{}/v1/requests", p.api))
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::ACCEPTED);
    let submitted: serde_json::Value = serde_json::from_slice(&r.bytes().await.unwrap()).unwrap();
    let token = submitted["request_token"].as_str().unwrap().to_owned();

    let result = p.next_result(RESULTS).await;
    assert_eq!(result["id"], "r1");
    assert_eq!(result["status_code"], 200);
    assert_eq!(result["request_token"], token.as_str());
    let payload: serde_json::Value =
        serde_json::from_str(result["payload"].as_str().unwrap()).unwrap();
    assert_eq!(payload["usage"]["completion_tokens"], 3);

    let seen = upstream.recorded();
    assert_eq!(seen.len(), 1);
    let seen = &seen[0];
    assert_eq!(seen.path, "/v1/chat/completions");
    assert_eq!(&seen.body[..], br#"{"model":"m",  "prompt": "hi"}"#);
    assert_eq!(seen.header("content-type"), Some("application/json"));
    assert_eq!(seen.header("x-gateway-inference-objective"), Some("obj"));
    assert_eq!(
        seen.header("x-llm-d-inference-fairness-id"),
        Some("tenant-a")
    );
    assert_eq!(seen.header("x-custom"), Some("1"));

    let q = [
        ("queue_name", "q"),
        ("queue_id", "q"),
        ("pool_name", "default"),
    ];
    p.wait_metric("llm_d_async_async_successful_requests_total", &q, 1.0)
        .await;
    assert_eq!(
        p.metric("llm_d_async_async_dispatched_requests_total", &q)
            .await,
        Some(1.0)
    );
    assert_eq!(
        p.metric("llm_d_async_async_request_total", &q).await,
        Some(1.0)
    );
    let out = [("queue_name", "q"), ("direction", "output")];
    assert_eq!(
        p.metric("llm_d_async_async_tokens_total", &out).await,
        Some(3.0)
    );
    assert_eq!(
        p.metric("llm_d_async_async_inflight_requests", &q).await,
        Some(0.0)
    );
    assert_eq!(
        p.metric("llm_d_async_async_queue_depth", &q).await,
        Some(0.0)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn large_binary_payload_streams_through_and_result_comes_back_by_reference() {
    let upstream = Upstream::start().await;
    let audio = random_bytes(3 << 20);
    let reply_audio = audio.clone();
    upstream.reply_with(move |_, _| Reply::bytes(200, "audio/mpeg", reply_audio.clone()));
    let dir = tempfile::tempdir().unwrap();
    let mut q = queue("q", &upstream);
    q["request_path_url"] = json!("/v1/audio/speech");
    let p = Processor::start(dir.path(), Spec::new(transport(json!([q])))).await;

    let payload = random_bytes(5 << 20);
    let form = reqwest::multipart::Form::new()
        .text(
            "request",
            json!({"id": "tts-1", "deadline": now_secs() + 120}).to_string(),
        )
        .part(
            "payload",
            reqwest::multipart::Part::bytes(payload.clone())
                .mime_str("audio/wav")
                .unwrap(),
        );
    let r = p
        .http
        .post(format!("{}/v1/requests", p.api))
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::ACCEPTED);
    let token = serde_json::from_slice::<serde_json::Value>(&r.bytes().await.unwrap()).unwrap()
        ["request_token"]
        .as_str()
        .unwrap()
        .to_owned();

    let claim = crate::harness::eventually("a result", || p.claim(RESULTS, 60_000)).await;
    let result = &claim["result"];
    assert_eq!(result["status_code"], 200);
    assert_eq!(result["payload"], "");
    assert_eq!(result["content_type"], "audio/mpeg");
    assert_eq!(result["payload_size"], audio.len());
    assert_eq!(result["payload_sha256"], hex(&Sha256::digest(&audio)));
    let payload_ref = result["payload_ref"].as_str().unwrap();
    let name = payload_ref
        .strip_prefix("blob://results/")
        .expect("a result blob reference");
    let attempt = name
        .strip_prefix(&format!("{token}-"))
        .expect("named after the request token and claim attempt");
    assert!(attempt.parse::<u64>().is_ok(), "{payload_ref}");

    let blob = p.get(&format!("/v1/blobs/results/{name}")).await;
    assert_eq!(blob.status(), StatusCode::OK);
    assert_eq!(blob.headers()["content-type"], "audio/mpeg");
    assert_eq!(blob.bytes().await.unwrap().as_ref(), audio.as_slice());

    let seen = &upstream.recorded()[0];
    assert_eq!(seen.path, "/v1/audio/speech");
    assert_eq!(seen.header("content-type"), Some("audio/wav"));
    assert_eq!(seen.body.len(), payload.len());
    assert!(
        seen.body.as_ref() == payload.as_slice(),
        "payload bytes changed in transit"
    );

    // The request body blob is gone once the result is written; the result
    // body blob once it is acknowledged.
    let requests_dir = p.data_dir().join("blobs/requests");
    assert_eq!(std::fs::read_dir(&requests_dir).unwrap().count(), 0);
    assert_eq!(p.ack(RESULTS, &claim).await, StatusCode::NO_CONTENT);
    let gone = p.get(&format!("/v1/blobs/results/{name}")).await;
    assert_eq!(gone.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn unacknowledged_result_is_redelivered_and_stale_owner_fenced() {
    let upstream = Upstream::start().await;
    let dir = tempfile::tempdir().unwrap();
    let p = Processor::start(
        dir.path(),
        Spec::new(transport(json!([queue("q", &upstream)]))),
    )
    .await;
    p.submit(request("r1", 60)).await;

    let first = crate::harness::eventually("a result", || p.claim(RESULTS, 500)).await;
    assert!(
        p.claim_waiting(RESULTS, 500, 0).await.is_none(),
        "a leased result must not be handed out twice"
    );
    tokio::time::sleep(Duration::from_millis(700)).await;
    let second = crate::harness::eventually("redelivery", || p.claim(RESULTS, 60_000)).await;
    assert_eq!(second["claim_id"], first["claim_id"]);
    assert_ne!(second["owner_token"], first["owner_token"]);
    assert_eq!(second["result"], first["result"]);

    assert_eq!(p.ack(RESULTS, &first).await, StatusCode::CONFLICT);
    assert_eq!(p.ack(RESULTS, &second).await, StatusCode::NO_CONTENT);
    assert_eq!(p.ack(RESULTS, &second).await, StatusCode::NO_CONTENT);
    let (renew, _) = p
        .post(
            &format!("/v1/results/{RESULTS}/claims/{}/renew", second["claim_id"]),
            json!({"owner_token": second["owner_token"]}),
        )
        .await;
    assert_eq!(renew, StatusCode::CONFLICT);
}

#[tokio::test(flavor = "multi_thread")]
async fn batch_submit_is_all_or_nothing() {
    let upstream = Upstream::start().await;
    let dir = tempfile::tempdir().unwrap();
    let p = Processor::start(
        dir.path(),
        Spec::new(transport(json!([queue("q", &upstream)]))),
    )
    .await;

    let bad = json!([request("a", 60), {"id": "b", "deadline": now_secs() - 5}]);
    let (status, _) = p.post("/v1/requests/batch", bad).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let good = json!([request("a", 60), request("b", 60), request("c", 60)]);
    let (status, body) = p.post("/v1/requests/batch", good).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(body.as_array().unwrap().len(), 3);
    let mut ids = Vec::new();
    for _ in 0..3 {
        ids.push(
            p.next_result(RESULTS).await["id"]
                .as_str()
                .unwrap()
                .to_owned(),
        );
    }
    ids.sort();
    assert_eq!(ids, ["a", "b", "c"]);
    assert_eq!(upstream.count(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn destructive_pop_and_result_ttl() {
    let upstream = Upstream::start().await;
    let dir = tempfile::tempdir().unwrap();
    let mut short = queue("short", &upstream);
    short["result_queue_name"] = json!("short-results");
    short["result_ttl_seconds"] = json!(1);
    let p = Processor::start(
        dir.path(),
        Spec::new(transport(json!([queue("q", &upstream), short]))),
    )
    .await;

    p.submit(request("kept", 60)).await;
    let r = p
        .http
        .post(format!("{}/v1/results/{RESULTS}/pop?wait_ms=10000", p.api))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    let v: serde_json::Value = serde_json::from_slice(&r.bytes().await.unwrap()).unwrap();
    assert_eq!(v["id"], "kept");
    p.no_result_within(RESULTS, Duration::from_millis(200))
        .await;

    let mut expiring = request("expiring", 60);
    expiring["request_queue_name"] = json!("short");
    p.submit(expiring).await;
    crate::harness::eventually("the short-lived result", || async {
        let v: serde_json::Value = serde_json::from_slice(
            &p.get("/v1/results/short-results/depth")
                .await
                .bytes()
                .await
                .unwrap(),
        )
        .unwrap();
        (v["depth"] == 1).then_some(())
    })
    .await;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    p.no_result_within("short-results", Duration::from_millis(100))
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn submissions_are_validated() {
    let upstream = Upstream::start().await;
    let dir = tempfile::tempdir().unwrap();
    let p = Processor::start(
        dir.path(),
        Spec::new(transport(json!([queue("q", &upstream)])))
            .arg("--max-payload-bytes", "1000")
            .arg("--max-json-body-bytes", "4096"),
    )
    .await;

    let cases = [
        json!({"deadline": now_secs() + 60}),
        json!({"id": "", "deadline": now_secs() + 60}),
        json!({"id": "a\u{0}b", "deadline": now_secs() + 60}),
        json!({"id": "x"}),
        json!({"id": "x", "deadline": 0}),
        json!({"id": "x", "deadline": now_secs() - 1}),
        json!({"id": "x", "deadline": now_secs() + 60, "request_queue_name": "nope"}),
    ];
    for case in cases {
        let (status, body) = p.submit_status(case.clone()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{case} -> {body}");
        assert!(body["error"].is_string());
    }

    let broken = p
        .http
        .post(format!("{}/v1/requests", p.api))
        .header("content-type", "application/json")
        .body(format!(
            r#"{{"id":"x","deadline":{},"payload":{{bad}}}}"#,
            now_secs() + 60
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(broken.status(), StatusCode::BAD_REQUEST);

    let huge_json = json!({"id": "j", "deadline": now_secs() + 60, "payload": "x".repeat(5000)});
    assert_eq!(
        p.submit_status(huge_json).await.0,
        StatusCode::PAYLOAD_TOO_LARGE
    );
    let over_payload = json!({"id": "j", "deadline": now_secs() + 60, "payload": "x".repeat(1500)});
    assert_eq!(
        p.submit_status(over_payload).await.0,
        StatusCode::PAYLOAD_TOO_LARGE
    );

    let no_payload = reqwest::multipart::Form::new().text(
        "request",
        json!({"id": "m", "deadline": now_secs() + 60}).to_string(),
    );
    let r = p
        .http
        .post(format!("{}/v1/requests", p.api))
        .multipart(no_payload)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::BAD_REQUEST);

    let too_big = reqwest::multipart::Form::new()
        .text(
            "request",
            json!({"id": "m", "deadline": now_secs() + 60}).to_string(),
        )
        .part("payload", reqwest::multipart::Part::bytes(vec![7u8; 2000]));
    let r = p
        .http
        .post(format!("{}/v1/requests", p.api))
        .multipart(too_big)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::PAYLOAD_TOO_LARGE);

    for dir in ["blobs/requests", "blobs/tmp"] {
        assert_eq!(
            std::fs::read_dir(p.data_dir().join(dir)).unwrap().count(),
            0,
            "{dir} not empty"
        );
    }
    assert_eq!(p.queue_depth("q").await, 0);
    assert_eq!(upstream.count(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn inference_failures_map_to_results() {
    let upstream = Upstream::start().await;
    let shed_attempts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    upstream.reply_with(move |r, _| match r.json()["prompt"].as_str().unwrap() {
        "bad" => Reply::json(400, json!({"error": "bad prompt"})),
        "shed" if shed_attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 => {
            Reply::json(429, json!({"error": "busy"}))
                .header("retry-after", "0")
                .header("x-llm-d-request-dropped-reason", "queue-ttl")
        }
        "slow" => Reply::json(200, json!({})).delayed(Duration::from_secs(10)),
        _ => Reply::json(200, json!({"ok": true})),
    });
    let dir = tempfile::tempdir().unwrap();
    let mut dead = json!({"queue_name": "dead", "igw_base_url": format!("http://127.0.0.1:{}", crate::harness::free_port())});
    dead["result_queue_name"] = json!("dead-results");
    let p = Processor::start(
        dir.path(),
        Spec::new(transport(json!([queue("q", &upstream), dead]))),
    )
    .await;

    p.submit(request("bad", 60)).await;
    let r = p.next_result(RESULTS).await;
    assert_eq!(
        (r["id"].as_str(), r["status_code"].as_u64()),
        (Some("bad"), Some(400))
    );
    assert!(r["payload"].as_str().unwrap().contains("bad prompt"));

    p.submit(request("shed", 60)).await;
    let r = p.next_result(RESULTS).await;
    assert_eq!(
        (r["id"].as_str(), r["status_code"].as_u64()),
        (Some("shed"), Some(200))
    );
    let q = [("queue_name", "q")];
    assert_eq!(
        p.metric("llm_d_async_async_request_retries_total", &q)
            .await,
        Some(1.0)
    );
    assert_eq!(
        p.metric("llm_d_async_async_shedded_requests_total", &q)
            .await,
        Some(1.0)
    );
    assert_eq!(
        p.metric("llm_d_async_async_failed_requests_total", &q)
            .await,
        Some(1.0)
    );
    // A retry is one more dispatch, not one more request.
    assert_eq!(
        p.metric("llm_d_async_async_request_total", &q).await,
        Some(2.0)
    );
    assert_eq!(
        p.metric("llm_d_async_async_dispatched_requests_total", &q)
            .await,
        Some(3.0)
    );

    let mut unreachable = request("unreachable", 60);
    unreachable["request_queue_name"] = json!("dead");
    p.submit(unreachable).await;
    let r = p.next_result("dead-results").await;
    assert_eq!(r["error_code"], "INFERENCE_ERROR");
    assert!(r.get("status_code").is_none());

    p.submit(request("slow", 2)).await;
    let r = p.next_result(RESULTS).await;
    assert_eq!(
        (r["id"].as_str(), r["error_code"].as_str()),
        (Some("slow"), Some("DEADLINE_EXCEEDED"))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelled_and_expired_requests_never_dispatch() {
    let upstream = Upstream::start().await;
    let dir = tempfile::tempdir().unwrap();
    let mut q = queue("q", &upstream);
    q["gate_type"] = json!("budget-key");
    q["gate_params"] = json!({"budget_key": "hold"});
    let p = Processor::start(dir.path(), Spec::new(transport(json!([q])))).await;
    assert_eq!(
        p.put("/v1/admin/budgets/hold", json!(0)).await,
        StatusCode::NO_CONTENT
    );

    p.submit(request("cancel-me", 60)).await;
    p.submit(request("expire-me", 2)).await;
    let (status, body) = p
        .post(
            "/v1/requests/cancel",
            json!({"ids": ["cancel-me", "unknown"]}),
        )
        .await;
    assert_eq!(
        (status, body["cancelled"].as_u64()),
        (StatusCode::OK, Some(1))
    );

    let labels = [("queue_name", "q")];
    p.wait_metric("llm_d_async_async_broker_backlog", &labels, 2.0)
        .await;
    assert_eq!(
        p.metric("llm_d_async_async_dispatch_budget", &labels).await,
        Some(0.0)
    );
    assert!(
        p.metric(
            "llm_d_async_async_gate_decisions_total",
            &[("queue_name", "q"), ("reason", "gate_closed")]
        )
        .await
        .unwrap()
            > 0.0
    );
    let expired_bucket = [("queue_name", "q"), ("le", "0")];
    tokio::time::sleep(Duration::from_millis(2500)).await;
    p.wait_metric(
        "llm_d_async_async_deadline_proximity_millis_bucket",
        &expired_bucket,
        1.0,
    )
    .await;

    assert_eq!(
        p.put("/v1/admin/budgets/hold", json!(1)).await,
        StatusCode::NO_CONTENT
    );
    let mut codes = Vec::new();
    for _ in 0..2 {
        let r = p.next_result(RESULTS).await;
        codes.push((
            r["id"].as_str().unwrap().to_owned(),
            r["error_code"].as_str().unwrap().to_owned(),
        ));
    }
    codes.sort();
    assert_eq!(
        codes,
        [
            ("cancel-me".to_owned(), "CANCELLED".to_owned()),
            ("expire-me".to_owned(), "DEADLINE_EXCEEDED".to_owned())
        ]
    );
    assert_eq!(upstream.count(), 0);
    p.wait_metric("llm_d_async_async_broker_backlog", &labels, 0.0)
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn admin_rejects_invalid_commands() {
    let upstream = Upstream::start().await;
    let dir = tempfile::tempdir().unwrap();
    let p = Processor::start(
        dir.path(),
        Spec::new(transport(json!([queue("q", &upstream)]))),
    )
    .await;
    let expired = json!({
        "api_version": "llm-d.ai/v1alpha1", "pool_id": "default", "max_admission_rps": 1.0,
        "valid_until_unix_ms": 1, "decision_id": "d"
    });
    assert_eq!(
        p.put("/v1/admin/dispatch-rates/ctl", expired).await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        p.get("/v1/admin/dispatch-rates/ctl").await.status(),
        StatusCode::NOT_FOUND
    );
    let r = p
        .http
        .put(format!("{}/v1/admin/budgets/b", p.api))
        .json_body(&json!("half"))
        .send()
        .await
        .unwrap();
    assert!(r.status().is_client_error());
}

#[tokio::test(flavor = "multi_thread")]
async fn abandoned_upload_leaves_no_files() {
    use tokio::io::AsyncWriteExt;

    let upstream = Upstream::start().await;
    let dir = tempfile::tempdir().unwrap();
    let p = Processor::start(
        dir.path(),
        Spec::new(transport(json!([queue("q", &upstream)]))),
    )
    .await;

    let boundary = "XBOUNDARYX";
    let envelope = json!({"id": "gone", "deadline": now_secs() + 60}).to_string();
    let head = format!(
        "--{boundary}\r\nContent-Disposition: form-data; name=\"request\"\r\n\r\n{envelope}\r\n\
         --{boundary}\r\nContent-Disposition: form-data; name=\"payload\"\r\nContent-Type: audio/wav\r\n\r\n"
    );
    let addr = p.api.trim_start_matches("http://").to_owned();
    let mut conn = tokio::net::TcpStream::connect(addr).await.unwrap();
    let http_head = format!(
        "POST /v1/requests HTTP/1.1\r\nHost: x\r\nContent-Type: multipart/form-data; boundary={boundary}\r\n\
         Content-Length: {}\r\n\r\n{head}",
        head.len() + (10 << 20)
    );
    conn.write_all(http_head.as_bytes()).await.unwrap();
    // Past the inline limit, so the body spills to a file, then hang up.
    conn.write_all(&vec![1u8; 512 << 10]).await.unwrap();
    conn.flush().await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(conn);

    crate::harness::eventually("staged files to be removed", || async {
        let empty = |d: &str| std::fs::read_dir(p.data_dir().join(d)).unwrap().count() == 0;
        (empty("blobs/tmp") && empty("blobs/requests")).then_some(())
    })
    .await;
    assert_eq!(p.queue_depth("q").await, 0);
    p.submit(request("after", 60)).await;
    assert_eq!(p.next_result(RESULTS).await["id"], "after");
}
