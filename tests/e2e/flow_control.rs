use std::time::Duration;

use axum::http::StatusCode;
use serde_json::json;

use crate::harness::{Processor, Reply, Spec, Upstream, now_millis_i64, queue, request};

const RESULTS: &str = "result-list";

fn transport(queues: serde_json::Value) -> serde_json::Value {
    json!({"poll_interval_ms": 100, "queues": queues})
}

fn gated(
    name: &str,
    upstream: &Upstream,
    gate_type: &str,
    params: serde_json::Value,
) -> serde_json::Value {
    let mut q = queue(name, upstream);
    q["gate_type"] = json!(gate_type);
    q["gate_params"] = params;
    q
}

#[tokio::test(flavor = "multi_thread")]
async fn tier_priority_serves_interactive_lanes_first() {
    let upstream = Upstream::start().await;
    let dir = tempfile::tempdir().unwrap();
    let mut batch = queue("batch", &upstream);
    batch["labels"] = json!({"tier": "batch"});
    let mut interactive = queue("interactive", &upstream);
    interactive["labels"] = json!({"tier": "interactive"});
    let p = Processor::start(
        dir.path(),
        Spec::new(transport(json!([batch, interactive])))
            .pools(json!([{
                "id": "default", "workers": 1,
                "gate_type": "wait-on-refuse",
                "gate_params": {"gate": {"gate_type": "budget-key", "gate_params": {"budget_key": "pool"}}}
            }]))
            .merge_policy(json!({
                "type": "tier-priority",
                "parameters": {
                    "priority_header": "x-priority",
                    "lane_objectives": {"overflow-interactive": "critical"}
                }
            })),
    )
    .await;
    assert_eq!(
        p.put("/v1/admin/budgets/pool", json!(0)).await,
        StatusCode::NO_CONTENT
    );

    for i in 1..=6 {
        let mut r = request(&format!("b{i}"), 120);
        r["request_queue_name"] = json!("batch");
        p.submit(r).await;
    }
    crate::harness::eventually("batch requests claimed", || async {
        (p.queue_depth("batch").await == 0).then_some(())
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    for i in 1..=3 {
        let mut r = request(&format!("i{i}"), 120);
        r["request_queue_name"] = json!("interactive");
        p.submit(r).await;
    }
    crate::harness::eventually("interactive requests claimed", || async {
        (p.queue_depth("interactive").await == 0).then_some(())
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(upstream.count(), 0, "the pool gate must hold every request");

    assert_eq!(
        p.put("/v1/admin/budgets/pool", json!(1)).await,
        StatusCode::NO_CONTENT
    );
    let seen = upstream.wait_for_requests(9).await;
    let order: Vec<String> = seen
        .iter()
        .map(|r| {
            r.json()["prompt"]
                .as_str()
                .unwrap()
                .chars()
                .next()
                .unwrap()
                .to_string()
        })
        .collect();
    // One request parked in the worker, two in the merged channel and one in
    // the scheduler's hand were batch before the interactive ones arrived.
    assert_eq!(order.join(""), "bbbbiiibb");
    for r in &seen {
        if r.json()["prompt"].as_str().unwrap().starts_with('i') {
            assert_eq!(r.header("x-priority"), Some("3"));
            assert_eq!(r.header("x-llm-d-inference-objective"), Some("critical"));
        } else {
            assert_eq!(r.header("x-priority"), Some("5"));
            assert_eq!(r.header("x-llm-d-inference-objective"), None);
        }
    }
    let pool = [
        ("pool_name", "default"),
        ("queue_name", ""),
        ("reason", "gate_closed"),
    ];
    assert!(
        p.metric("llm_d_async_async_gate_decisions_total", &pool)
            .await
            .unwrap()
            >= 1.0
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn tenant_quota_serializes_one_tenant_only() {
    let upstream = Upstream::start().await;
    upstream.reply_with(|_, _| Reply::json(200, json!({})).delayed(Duration::from_millis(500)));
    let dir = tempfile::tempdir().unwrap();
    let q = gated(
        "q",
        &upstream,
        "quota",
        json!({"mode": "concurrency", "limit": 1}),
    );
    let p = Processor::start(dir.path(), Spec::new(transport(json!([q])))).await;

    let mut batch = Vec::new();
    for (id, tenant) in [("a1", "a"), ("a2", "a"), ("a3", "a"), ("b1", "b")] {
        let mut r = request(id, 60);
        r["metadata"] = json!({"userid": tenant});
        batch.push(r);
    }
    let (status, _) = p.post("/v1/requests/batch", json!(batch)).await;
    assert_eq!(status, StatusCode::ACCEPTED);
    for _ in 0..4 {
        p.next_result(RESULTS).await;
    }
    let seen = upstream.recorded();
    let arrivals = |tenant: &str| {
        let mut at: Vec<_> = seen
            .iter()
            .filter(|r| r.json()["prompt"].as_str().unwrap().starts_with(tenant))
            .map(|r| r.at)
            .collect();
        at.sort();
        at
    };
    let a = arrivals("a");
    assert_eq!(a.len(), 3);
    for pair in a.windows(2) {
        assert!(
            pair[1] - pair[0] >= Duration::from_millis(450),
            "tenant a ran concurrently"
        );
    }
    let b = arrivals("b");
    assert!(
        b[0].duration_since(a[0]) < Duration::from_millis(400),
        "tenant b waited on tenant a"
    );
    let labels = [("queue_name", "q"), ("reason", "quota_exhausted")];
    assert!(
        p.metric("llm_d_async_async_gate_decisions_total", &labels)
            .await
            .unwrap()
            >= 1.0
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn leased_rate_fails_closed_then_paces_admission() {
    let upstream = Upstream::start().await;
    let dir = tempfile::tempdir().unwrap();
    let p = Processor::start(
        dir.path(),
        Spec::new(transport(json!([queue("q", &upstream)]))).pools(json!([{
            "id": "default", "workers": 8,
            "gate_type": "wait-on-refuse",
            "gate_params": {"gate": {"gate_type": "leased-rate", "gate_params": {"control_key": "ctl"}}}
        }])),
    )
    .await;
    let batch: Vec<_> = (0..8).map(|i| request(&format!("r{i}"), 60)).collect();
    p.post("/v1/requests/batch", json!(batch)).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(upstream.count(), 0, "no lease must mean no dispatch");
    let pool = [("pool_name", "default")];
    assert_eq!(
        p.metric("llm_d_async_async_drain_limit_lease_valid", &pool)
            .await,
        Some(0.0)
    );

    let lease = json!({
        "api_version": "llm-d.ai/v1alpha1", "pool_id": "default", "max_admission_rps": 2.0,
        "valid_until_unix_ms": now_millis_i64() + 60_000, "decision_id": "d1"
    });
    assert_eq!(
        p.put("/v1/admin/dispatch-rates/ctl", lease).await,
        StatusCode::NO_CONTENT
    );
    let seen = upstream.wait_for_requests(8).await;
    let mut at: Vec<_> = seen.iter().map(|r| r.at).collect();
    at.sort();
    // A 2-token bucket at 2 rps: two at once, then one every 500ms.
    let spread = at[7] - at[0];
    assert!(
        spread >= Duration::from_millis(2500),
        "8 requests in {spread:?} exceeds 2 rps"
    );
    assert_eq!(
        p.metric("llm_d_async_async_drain_limit_lease_valid", &pool)
            .await,
        Some(1.0)
    );
    assert_eq!(
        p.metric("llm_d_async_async_drain_limit_rps", &pool).await,
        Some(2.0)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn pool_concurrency_gate_caps_in_flight() {
    let upstream = Upstream::start().await;
    upstream.reply_with(|_, _| Reply::json(200, json!({})).delayed(Duration::from_millis(300)));
    let dir = tempfile::tempdir().unwrap();
    let p = Processor::start(
        dir.path(),
        Spec::new(transport(json!([queue("q", &upstream)]))).pools(json!([{
            "id": "default", "workers": 8,
            "gate_type": "local-max-concurrency",
            "gate_params": {"limit": 2, "gating_mode": "blocking"}
        }])),
    )
    .await;
    let batch: Vec<_> = (0..6).map(|i| request(&format!("r{i}"), 60)).collect();
    p.post("/v1/requests/batch", json!(batch)).await;
    for _ in 0..6 {
        p.next_result(RESULTS).await;
    }
    assert_eq!(upstream.max_inflight(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn prometheus_query_gate_follows_the_metric() {
    let upstream = Upstream::start().await;
    upstream.set_prometheus_value(Some("0"));
    let dir = tempfile::tempdir().unwrap();
    let q = gated(
        "q",
        &upstream,
        "prometheus-query",
        json!({"query": "async_budget", "pool": "ip"}),
    );
    let p = Processor::start(
        dir.path(),
        Spec::new(transport(json!([q]))).arg("--prometheus-url", &upstream.url),
    )
    .await;
    p.submit(request("r1", 60)).await;
    let labels = [("queue_name", "q"), ("inference_pool", "ip")];
    p.wait_metric(
        "llm_d_async_async_gate_metric_source_available",
        &labels,
        1.0,
    )
    .await;
    assert_eq!(
        p.metric("llm_d_async_async_gate_metric_value", &labels)
            .await,
        Some(0.0)
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(upstream.count(), 0);

    upstream.set_prometheus_value(None);
    p.wait_metric(
        "llm_d_async_async_gate_metric_source_available",
        &labels,
        0.0,
    )
    .await;
    assert_eq!(upstream.count(), 0, "the default fallback budget is closed");

    upstream.set_prometheus_value(Some("0.5"));
    assert_eq!(p.next_result(RESULTS).await["status_code"], 200);
    p.wait_metric(
        "llm_d_async_async_dispatch_budget",
        &[("queue_name", "q")],
        0.5,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn prometheus_budget_gate_closes_at_the_baseline() {
    let upstream = Upstream::start().await;
    upstream.set_prometheus_value(Some("0.04"));
    let dir = tempfile::tempdir().unwrap();
    let q = gated(
        "q",
        &upstream,
        "prometheus-budget",
        json!({"pool": "ip", "baseline": 0.05}),
    );
    let p = Processor::start(
        dir.path(),
        Spec::new(transport(json!([q]))).arg("--prometheus-url", &upstream.url),
    )
    .await;
    p.submit(request("r1", 60)).await;
    let labels = [("queue_name", "q"), ("inference_pool", "ip")];
    p.wait_metric("llm_d_async_async_gate_metric_threshold", &labels, 0.05)
        .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(upstream.count(), 0);
    upstream.set_prometheus_value(Some("0.9"));
    assert_eq!(p.next_result(RESULTS).await["status_code"], 200);
}

#[tokio::test(flavor = "multi_thread")]
async fn endpoint_scrape_gate_reads_saturation() {
    let upstream = Upstream::start().await;
    upstream.set_metrics_text(
        "# TYPE vllm:num_requests_running gauge\nvllm:num_requests_running{pod=\"a\"} 4\n",
    );
    let dir = tempfile::tempdir().unwrap();
    let q = gated(
        "q",
        &upstream,
        "endpoint-scrape",
        json!({"url": format!("{}/metrics", upstream.url), "metric": "vllm:num_requests_running", "max_count_per_pod": 4}),
    );
    let p = Processor::start(dir.path(), Spec::new(transport(json!([q])))).await;
    p.submit(request("r1", 60)).await;
    p.wait_metric(
        "llm_d_async_async_dispatch_budget",
        &[("queue_name", "q")],
        0.0,
    )
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(upstream.count(), 0);
    upstream.set_metrics_text("vllm:num_requests_running{pod=\"a\"} 1\n");
    assert_eq!(p.next_result(RESULTS).await["status_code"], 200);
    p.wait_metric(
        "llm_d_async_async_dispatch_budget",
        &[("queue_name", "q")],
        0.75,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn tier_admission_drops_interactive_overflow_when_saturated() {
    let upstream = Upstream::start().await;
    upstream.reply_with(|r, _| {
        if r.json()["prompt"] == "holder" {
            Reply::hang()
        } else {
            Reply::json(200, json!({}))
        }
    });
    let dir = tempfile::tempdir().unwrap();
    let mut q = gated(
        "q",
        &upstream,
        "quota",
        json!({"mode": "concurrency", "limit": 1, "gating_mode": "classifying"}),
    );
    q["labels"] = json!({"tier": "interactive"});
    let p = Processor::start(
        dir.path(),
        Spec::new(transport(json!([q]))).pools(json!([{
            "id": "default", "workers": 4,
            "gate_type": "tier-priority-admission",
            "gate_params": {"saturation_gate": "budget-key", "saturation_gate_params": {"budget_key": "sat"}}
        }])),
    )
    .await;
    // The holder takes the tenant's only quota slot and never finishes.
    let mut holder = request("holder", 60);
    holder["metadata"] = json!({"userid": "t"});
    p.submit(holder).await;
    upstream.wait_for_requests(1).await;

    // Saturated now: an interactive request over its quota is shed.
    assert_eq!(
        p.put("/v1/admin/budgets/sat", json!(0)).await,
        StatusCode::NO_CONTENT
    );
    let mut over = request("over", 60);
    over["metadata"] = json!({"userid": "t"});
    p.submit(over).await;
    let result = p.next_result(RESULTS).await;
    assert_eq!(result["id"], "over");
    assert_eq!(
        result["payload"],
        r#"{"error": "Too Many Requests", "code": 429}"#
    );
    assert!(result.get("error_code").is_none());
    let dropped = [
        ("pool_name", "default"),
        ("queue_name", ""),
        ("reason", "dropped"),
    ];
    assert_eq!(
        p.metric("llm_d_async_async_gate_decisions_total", &dropped)
            .await,
        Some(1.0)
    );

    // A reserved request waits for capacity instead of being shed.
    let mut reserved = request("reserved", 60);
    reserved["metadata"] = json!({"userid": "other"});
    p.submit(reserved).await;
    p.no_result_within(RESULTS, Duration::from_millis(800))
        .await;
    assert_eq!(
        p.put("/v1/admin/budgets/sat", json!(1)).await,
        StatusCode::NO_CONTENT
    );
    assert_eq!(p.next_result(RESULTS).await["id"], "reserved");
    assert_eq!(upstream.count(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn blocking_queue_gate_admits_past_its_own_batch_and_stops_cleanly() {
    let upstream = Upstream::start().await;
    upstream.reply_with(|_, _| Reply::json(200, json!({})).delayed(Duration::from_millis(300)));
    let dir = tempfile::tempdir().unwrap();
    // A poll peeks 10 rows but the gate has 2 slots: the consumer must send
    // the first two on before waiting for a third.
    let q = gated(
        "q",
        &upstream,
        "local-max-concurrency",
        json!({"limit": 2, "gating_mode": "blocking"}),
    );
    let mut p = Processor::start(dir.path(), Spec::new(transport(json!([q])))).await;
    let batch: Vec<_> = (0..6).map(|i| request(&format!("r{i}"), 60)).collect();
    p.post("/v1/requests/batch", json!(batch)).await;
    for _ in 0..6 {
        p.next_result(RESULTS).await;
    }
    assert_eq!(upstream.max_inflight(), 2);

    // A consumer parked on the gate must not hold up shutdown.
    let batch: Vec<_> = (0..6).map(|i| request(&format!("s{i}"), 60)).collect();
    p.post("/v1/requests/batch", json!(batch)).await;
    upstream.wait_for_requests(8).await;
    p.signal("-TERM");
    assert!(p.wait_exit(Duration::from_secs(10)).await.success());
}
