//! Several processors sharing one queue through Postgres.

use std::time::{Duration, Instant};

use axum::http::StatusCode;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::harness::{
    Postgres, Processor, Reply, Spec, Upstream, eventually, now_secs, queue, request,
};

const RESULTS: &str = "result-list";
const Q: &str = "q";
const LEASE: &str = "3s";

fn transport(upstream: &Upstream) -> Value {
    json!({"poll_interval_ms": 50, "batch_size": 16, "queues": [queue(Q, upstream)]})
}

async fn start(dir: &tempfile::TempDir, name: &str, spec: Spec) -> Processor {
    let dir = dir.path().join(name);
    std::fs::create_dir_all(&dir).unwrap();
    Processor::start(&dir, spec).await
}

async fn result_ids(p: &Processor, n: usize) -> Vec<String> {
    let mut ids = Vec::new();
    for _ in 0..n {
        ids.push(
            p.next_result(RESULTS).await["id"]
                .as_str()
                .unwrap()
                .to_owned(),
        );
    }
    ids.sort();
    ids
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn replicas_split_a_queue_and_deliver_each_request_once() {
    let Some(db) = Postgres::schema().await else {
        return;
    };
    let (up_a, up_b) = (Upstream::start().await, Upstream::start().await);
    for up in [&up_a, &up_b] {
        up.reply_with(|r, _| {
            Reply::json(200, json!({"echo": r.json()["prompt"]})).delayed(Duration::from_millis(50))
        });
    }
    let dir = tempfile::tempdir().unwrap();
    let a = start(&dir, "a", Spec::new(transport(&up_a)).postgres(&db, LEASE)).await;
    let b = start(&dir, "b", Spec::new(transport(&up_b)).postgres(&db, LEASE)).await;
    db.wait_split(Q, 2).await;

    let n = 60;
    for i in 0..n {
        let via = if i % 2 == 0 { &a } else { &b };
        via.submit(request(&format!("r{i:02}"), 120)).await;
    }
    let ids = result_ids(&b, n).await;
    let want: Vec<String> = (0..n).map(|i| format!("r{i:02}")).collect();
    assert_eq!(ids, want);
    assert_eq!(
        up_a.count() + up_b.count(),
        n,
        "each request sent exactly once"
    );
    assert!(
        up_a.count() > 0 && up_b.count() > 0,
        "both replicas did work"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_killed_replicas_requests_finish_on_the_survivor() {
    let Some(db) = Postgres::schema().await else {
        return;
    };
    let (up_a, up_b) = (Upstream::start().await, Upstream::start().await);
    up_a.reply_with(|_, _| Reply::hang());
    up_b.reply_with(|_, n| Reply::json(200, json!({"n": n})));
    let dir = tempfile::tempdir().unwrap();
    let mut a = start(&dir, "a", Spec::new(transport(&up_a)).postgres(&db, LEASE)).await;
    let b = start(&dir, "b", Spec::new(transport(&up_b)).postgres(&db, LEASE)).await;
    db.wait_split(Q, 2).await;

    let n = 24;
    for i in 0..n {
        b.submit(request(&format!("r{i:02}"), 120)).await;
    }
    let stuck = eventually("the doomed replica to take work", || async {
        let c = up_a.count();
        (c > 0 && up_a.count() + up_b.count() == n).then_some(c)
    })
    .await;
    a.kill().await;

    let ids = result_ids(&b, n).await;
    assert_eq!(ids.len(), n);
    assert_eq!(up_a.count(), stuck, "the dead replica sent nothing more");
    assert_eq!(
        up_b.count(),
        n,
        "the survivor redelivered what the dead one held"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn sigterm_hands_partitions_over_without_waiting_for_the_lease() {
    let Some(db) = Postgres::schema().await else {
        return;
    };
    let upstream = Upstream::start().await;
    upstream.reply_with(|_, n| Reply::json(200, json!({"n": n})));
    let dir = tempfile::tempdir().unwrap();
    let lease = "12s";
    let mut a = start(
        &dir,
        "a",
        Spec::new(transport(&upstream)).postgres(&db, lease),
    )
    .await;
    let b = start(
        &dir,
        "b",
        Spec::new(transport(&upstream)).postgres(&db, lease),
    )
    .await;
    db.wait_split(Q, 2).await;

    a.signal("-TERM");
    assert!(a.wait_exit(Duration::from_secs(20)).await.success());
    let started = Instant::now();
    db.wait_split(Q, 1).await;
    assert!(
        started.elapsed() < Duration::from_secs(8),
        "handoff took {:?}, about as long as the 12s lease",
        started.elapsed()
    );
    for i in 0..10 {
        b.submit(request(&format!("r{i}"), 120)).await;
    }
    assert_eq!(result_ids(&b, 10).await.len(), 10);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tenant_quota_holds_across_replicas() {
    let Some(db) = Postgres::schema().await else {
        return;
    };
    let upstream = Upstream::start().await;
    upstream
        .reply_with(|_, n| Reply::json(200, json!({"n": n})).delayed(Duration::from_millis(150)));
    let mut q = queue(Q, &upstream);
    q["gate_type"] = json!("quota");
    q["gate_params"] = json!({"mode": "concurrency", "limit": 2, "attribute": "tenant"});
    let transport = json!({"poll_interval_ms": 20, "batch_size": 16, "queues": [q]});
    let dir = tempfile::tempdir().unwrap();
    let a = start(&dir, "a", Spec::new(transport.clone()).postgres(&db, LEASE)).await;
    let b = start(&dir, "b", Spec::new(transport).postgres(&db, LEASE)).await;
    db.wait_split(Q, 2).await;

    let n = 16;
    for i in 0..n {
        let mut r = request(&format!("t{i:02}"), 120);
        r["metadata"] = json!({"tenant": "acme"});
        a.submit(r).await;
    }
    assert_eq!(result_ids(&b, n).await.len(), n);
    assert_eq!(upstream.count(), n);
    assert!(
        upstream.max_inflight() <= 2,
        "tenant ran {} at once across replicas",
        upstream.max_inflight()
    );
    assert_eq!(upstream.max_inflight(), 2);
}

async fn large_bodies_cross_replicas(blob_store: Option<&str>) {
    let Some(db) = Postgres::schema().await else {
        return;
    };
    let upstream = Upstream::start().await;
    let audio: Vec<u8> = (0..(3 << 20)).map(|i| (i % 251) as u8).collect();
    let reply = audio.clone();
    upstream.reply_with(move |_, _| Reply::bytes(200, "audio/mpeg", reply.clone()));
    let dir = tempfile::tempdir().unwrap();
    let spec = |db: &Postgres| {
        let spec = Spec::new(transport(&upstream)).postgres(db, LEASE);
        match blob_store {
            Some(url) => spec.arg("--blob-store", url),
            None => spec,
        }
    };
    let a = start(&dir, "a", spec(&db)).await;
    let b = start(&dir, "b", spec(&db)).await;

    let payload: Vec<u8> = (0..(2 << 20)).map(|i| (i % 241) as u8).collect();
    let form = reqwest::multipart::Form::new()
        .text(
            "request",
            json!({"id": "big", "deadline": now_secs() + 120}).to_string(),
        )
        .part(
            "payload",
            reqwest::multipart::Part::bytes(payload.clone())
                .mime_str("audio/wav")
                .unwrap(),
        );
    let r = a
        .http
        .post(format!("{}/v1/requests", a.api))
        .multipart(form)
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::ACCEPTED);

    let claim = eventually("a result", || b.claim(RESULTS, 60_000)).await;
    let result = &claim["result"];
    assert_eq!(result["payload_size"], audio.len());
    assert_eq!(result["payload_sha256"], hex(&Sha256::digest(&audio)));
    let name = result["payload_ref"]
        .as_str()
        .unwrap()
        .strip_prefix("blob://results/")
        .unwrap()
        .to_owned();
    for via in [&a, &b] {
        let blob = via.get(&format!("/v1/blobs/results/{name}")).await;
        assert_eq!(blob.status(), StatusCode::OK);
        assert_eq!(blob.headers()["content-type"], "audio/mpeg");
        assert_eq!(blob.bytes().await.unwrap().as_ref(), audio.as_slice());
    }
    let seen = &upstream.recorded()[0];
    assert_eq!(seen.header("content-type"), Some("audio/wav"));
    assert!(
        seen.body.as_ref() == payload.as_slice(),
        "payload changed in transit"
    );

    assert_eq!(a.ack(RESULTS, &claim).await, StatusCode::NO_CONTENT);
    let gone = b.get(&format!("/v1/blobs/results/{name}")).await;
    assert_eq!(gone.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn large_bodies_cross_replicas_in_postgres() {
    large_bodies_cross_replicas(None).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn large_bodies_cross_replicas_in_an_object_store() {
    let blobs = tempfile::tempdir().unwrap();
    large_bodies_cross_replicas(Some(&format!("file://{}", blobs.path().display()))).await;
    let left: Vec<_> = walk(blobs.path());
    assert!(left.is_empty(), "blobs left behind: {left:?}");
}

fn walk(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            out.extend(walk(&path));
        } else {
            out.push(path);
        }
    }
    out
}
