use std::time::{Duration, Instant};

use axum::http::StatusCode;
use serde_json::json;

use crate::harness::{Processor, Reply, Spec, Upstream, eventually, now_secs, queue, request};

const RESULTS: &str = "result-list";

fn transport(queues: serde_json::Value) -> serde_json::Value {
    json!({"poll_interval_ms": 100, "queues": queues})
}

#[tokio::test(flavor = "multi_thread")]
async fn killed_process_redelivers_in_flight_work_on_restart() {
    let upstream = Upstream::start().await;
    upstream.reply_with(|_, n| {
        if n < 2 {
            Reply::hang()
        } else {
            Reply::json(200, json!({"n": n}))
        }
    });
    let dir = tempfile::tempdir().unwrap();
    let mut p = Processor::start(
        dir.path(),
        Spec::new(transport(json!([queue("q", &upstream)]))),
    )
    .await;

    p.submit(request("small", 120)).await;
    let big = vec![0xabu8; 1 << 20];
    let form = reqwest::multipart::Form::new()
        .text(
            "request",
            json!({"id": "big", "deadline": now_secs() + 120}).to_string(),
        )
        .part(
            "payload",
            reqwest::multipart::Part::bytes(big.clone())
                .mime_str("image/png")
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
    upstream.wait_for_requests(2).await;

    p.kill().await;
    p.spawn().await;

    let mut ids = vec![
        p.next_result(RESULTS).await["id"]
            .as_str()
            .unwrap()
            .to_owned(),
        p.next_result(RESULTS).await["id"]
            .as_str()
            .unwrap()
            .to_owned(),
    ];
    ids.sort();
    assert_eq!(ids, ["big", "small"]);
    let seen = upstream.recorded();
    assert_eq!(seen.len(), 4, "each request is delivered once per process");
    let bigs: Vec<_> = seen
        .iter()
        .filter(|r| r.header("content-type") == Some("image/png"))
        .collect();
    assert_eq!(bigs.len(), 2);
    assert!(bigs.iter().all(|r| r.body.as_ref() == big.as_slice()));
    assert_eq!(
        std::fs::read_dir(p.data_dir().join("blobs/requests"))
            .unwrap()
            .count(),
        0
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn sigterm_waits_for_in_flight_requests() {
    let upstream = Upstream::start().await;
    upstream.reply_with(|_, _| Reply::json(200, json!({})).delayed(Duration::from_millis(1500)));
    let dir = tempfile::tempdir().unwrap();
    let mut p = Processor::start(
        dir.path(),
        Spec::new(transport(json!([queue("q", &upstream)]))),
    )
    .await;
    p.submit(request("r1", 60)).await;
    upstream.wait_for_requests(1).await;

    let stopping = Instant::now();
    p.signal("-TERM");
    let status = p.wait_exit(Duration::from_secs(20)).await;
    assert!(status.success(), "{status:?}");
    assert!(
        stopping.elapsed() >= Duration::from_millis(1000),
        "exited before the request finished"
    );

    p.spawn().await;
    let result = p.next_result(RESULTS).await;
    assert_eq!(
        (result["id"].as_str(), result["status_code"].as_u64()),
        (Some("r1"), Some(200))
    );
    assert_eq!(upstream.count(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn drain_timeout_returns_in_flight_requests_to_the_queue() {
    let upstream = Upstream::start().await;
    upstream.reply_with(|_, n| {
        if n == 0 {
            Reply::json(200, json!({"first": true})).delayed(Duration::from_secs(30))
        } else {
            Reply::json(200, json!({"first": false}))
        }
    });
    let dir = tempfile::tempdir().unwrap();
    let mut p = Processor::start(
        dir.path(),
        Spec::new(transport(json!([queue("q", &upstream)]))).arg("--drain-timeout", "500ms"),
    )
    .await;
    p.submit(request("r1", 120)).await;
    upstream.wait_for_requests(1).await;

    p.signal("-TERM");
    let status = p.wait_exit(Duration::from_secs(10)).await;
    assert!(status.success(), "{status:?}");

    p.spawn().await;
    let result = p.next_result(RESULTS).await;
    assert_eq!(result["id"], "r1");
    let body: serde_json::Value =
        serde_json::from_str(result["payload"].as_str().unwrap()).unwrap();
    assert_eq!(body["first"], false);
    assert_eq!(upstream.count(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn queues_hot_reload_from_the_config_file() {
    let upstream = Upstream::start().await;
    let dir = tempfile::tempdir().unwrap();
    let p = Processor::start(
        dir.path(),
        Spec::new(transport(json!([queue("a", &upstream)])))
            .arg("--transport-config-watch-interval", "200ms"),
    )
    .await;
    let to = |queue: &str, id: &str| {
        let mut r = request(id, 60);
        r["request_queue_name"] = json!(queue);
        r
    };
    assert_eq!(
        p.submit_status(to("b", "early")).await.0,
        StatusCode::BAD_REQUEST
    );

    let write = |queues: serde_json::Value| {
        std::fs::write(p.transport_path(), transport(queues).to_string()).unwrap();
    };
    write(json!([queue("a", &upstream), queue("b", &upstream)]));
    eventually("queue b to appear", || async {
        (p.submit_status(to("b", "on-b")).await.0 == StatusCode::ACCEPTED).then_some(())
    })
    .await;
    assert_eq!(p.next_result(RESULTS).await["id"], "on-b");
    let success = [("result", "success")];
    assert!(
        p.metric("llm_d_async_async_queue_config_reloads_total", &success)
            .await
            .unwrap()
            >= 1.0
    );

    std::fs::write(p.transport_path(), "{not json").unwrap();
    p.wait_metric(
        "llm_d_async_async_queue_config_reloads_total",
        &[("result", "error")],
        1.0,
    )
    .await;
    p.submit(to("a", "still-a")).await;
    assert_eq!(p.next_result(RESULTS).await["id"], "still-a");

    write(json!([queue("b", &upstream)]));
    eventually("queue a to disappear", || async {
        let listed: serde_json::Value =
            serde_json::from_slice(&p.get("/v1/queues").await.bytes().await.unwrap()).unwrap();
        (listed.as_array().unwrap().len() == 1).then_some(())
    })
    .await;
    assert_eq!(
        p.submit_status(to("a", "late")).await.0,
        StatusCode::BAD_REQUEST
    );
    p.submit(to("b", "b-after")).await;
    assert_eq!(p.next_result(RESULTS).await["id"], "b-after");

    // Changing a non-queue field needs a restart and is rejected.
    std::fs::write(
        p.transport_path(),
        json!({"poll_interval_ms": 50, "queues": [queue("b", &upstream)]}).to_string(),
    )
    .unwrap();
    p.wait_metric(
        "llm_d_async_async_queue_config_reloads_total",
        &[("result", "error")],
        2.0,
    )
    .await;
}
