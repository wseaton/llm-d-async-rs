use axum::http::StatusCode;
use serde_json::json;

use crate::harness::{Processor, Spec, Upstream, queue};

#[tokio::test(flavor = "multi_thread")]
async fn health_and_metrics_answer_on_ipv4_and_ipv6() {
    let upstream = Upstream::start().await;
    let dir = tempfile::tempdir().unwrap();
    let p = Processor::start(
        dir.path(),
        Spec::new(json!({"queues": [queue("q", &upstream)]})),
    )
    .await;

    for host in ["127.0.0.1", "[::1]"] {
        let health = p.health.replace("127.0.0.1", host);
        let metrics = p.metrics.replace("127.0.0.1", host);

        let r = p
            .http
            .get(format!("{health}/healthz"))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK, "{host}");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&r.bytes().await.unwrap()).unwrap(),
            json!({"status": "ok"}),
            "{host}"
        );

        let r = p.http.get(format!("{health}/readyz")).send().await.unwrap();
        assert_eq!(r.status(), StatusCode::OK, "{host}");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&r.bytes().await.unwrap()).unwrap(),
            json!({"status": "ready"}),
            "{host}"
        );

        let r = p
            .http
            .get(format!("{metrics}/metrics"))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::OK, "{host}");
        assert!(
            r.text()
                .await
                .unwrap()
                .contains("async_gate_decisions_total"),
            "{host}"
        );
    }
}
