//! Runs the real `llm-d-async` binary against a stand-in inference gateway.
//! The stand-in is a real HTTP server; nothing inside the processor is
//! replaced.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{Query, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde_json::{Value, json};
use tokio::process::{Child, Command};
use tokio::sync::Notify;

pub const WAIT: Duration = Duration::from_secs(30);

pub fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

pub fn now_millis_i64() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Polls `check` until it returns `Some`, failing after `WAIT`.
pub async fn eventually<T, F, Fut>(what: &str, mut check: F) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<T>>,
{
    let deadline = Instant::now() + WAIT;
    loop {
        if let Some(v) = check().await {
            return v;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[derive(Debug, Clone)]
pub struct Recorded {
    pub path: String,
    pub headers: HeaderMap,
    pub body: Bytes,
    pub at: Instant,
}

impl Recorded {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }

    pub fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap()
    }
}

#[derive(Debug, Clone)]
pub struct Reply {
    pub status: u16,
    pub content_type: String,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
    pub delay: Duration,
    /// Never answer.
    pub hang: bool,
    /// Close the connection after `delay` without answering, as a model pod
    /// dying mid-generation does.
    pub die: bool,
    /// Send the headers and body, then break the connection instead of
    /// ending the body.
    pub cut: bool,
    /// Send the headers and body, then keep the body open.
    pub stall: bool,
}

impl Reply {
    pub fn json(status: u16, body: Value) -> Self {
        Self {
            status,
            content_type: "application/json".into(),
            headers: Vec::new(),
            body: Bytes::from(body.to_string()),
            delay: Duration::ZERO,
            hang: false,
            die: false,
            cut: false,
            stall: false,
        }
    }

    pub fn bytes(status: u16, content_type: &str, body: Vec<u8>) -> Self {
        Self {
            content_type: content_type.into(),
            body: Bytes::from(body),
            ..Self::json(status, json!(null))
        }
    }

    pub fn delayed(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    pub fn hang() -> Self {
        Self {
            hang: true,
            ..Self::json(200, json!(null))
        }
    }

    /// An event stream of `events`, each sent as one `data:` line.
    pub fn sse(events: &[String]) -> Self {
        let body: String = events.iter().map(|e| format!("data: {e}\n\n")).collect();
        Self::bytes(200, "text/event-stream", body.into_bytes())
    }

    pub fn cut(mut self) -> Self {
        self.cut = true;
        self
    }

    pub fn stall(mut self) -> Self {
        self.stall = true;
        self
    }

    pub fn die_after(delay: Duration) -> Self {
        Self {
            die: true,
            ..Self::json(200, json!(null)).delayed(delay)
        }
    }
}

/// Decides the reply to the `n`th (0-based) request.
type Behavior = Arc<dyn Fn(&Recorded, usize) -> Reply + Send + Sync>;

struct UpstreamState {
    recorded: Mutex<Vec<Recorded>>,
    behavior: Mutex<Behavior>,
    inflight: AtomicUsize,
    max_inflight: AtomicUsize,
    arrived: Notify,
    prometheus_value: Mutex<Option<String>>,
    metrics_text: Mutex<String>,
}

/// A stand-in inference gateway. It also answers `/api/v1/query` like
/// Prometheus and serves a configurable `/metrics` page.
#[derive(Clone)]
pub struct Upstream {
    pub url: String,
    state: Arc<UpstreamState>,
}

async fn record(State(state): State<Arc<UpstreamState>>, request: Request) -> Response {
    let path = request.uri().path().to_owned();
    let headers = request.headers().clone();
    let body = axum::body::to_bytes(request.into_body(), usize::MAX)
        .await
        .unwrap();
    let recorded = Recorded {
        path,
        headers,
        body,
        at: Instant::now(),
    };
    let reply = {
        let mut all = state.recorded.lock().unwrap();
        let behavior = state.behavior.lock().unwrap().clone();
        let reply = behavior(&recorded, all.len());
        all.push(recorded);
        reply
    };
    state.arrived.notify_waiters();
    let now = state.inflight.fetch_add(1, Ordering::SeqCst) + 1;
    state.max_inflight.fetch_max(now, Ordering::SeqCst);
    if reply.hang {
        std::future::pending::<()>().await;
    }
    tokio::time::sleep(reply.delay).await;
    state.inflight.fetch_sub(1, Ordering::SeqCst);
    if reply.die {
        std::panic::resume_unwind(Box::new("upstream died"));
    }
    let mut response = Response::builder().status(reply.status);
    if !reply.content_type.is_empty() {
        response = response.header("content-type", reply.content_type);
    }
    for (k, v) in reply.headers {
        response = response.header(k, v);
    }
    if reply.stall {
        use tokio_stream::StreamExt;
        let body = tokio_stream::iter([Ok::<_, std::io::Error>(reply.body)])
            .chain(tokio_stream::pending());
        return response.body(Body::from_stream(body)).unwrap();
    }
    if reply.cut {
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        tokio::spawn(async move {
            let _ = tx.send(Ok(reply.body)).await;
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = tx
                .send(Err(std::io::Error::other("upstream cut the stream")))
                .await;
        });
        return response
            .body(Body::from_stream(
                tokio_stream::wrappers::ReceiverStream::new(rx),
            ))
            .unwrap();
    }
    response.body(Body::from(reply.body)).unwrap()
}

async fn prometheus(
    State(state): State<Arc<UpstreamState>>,
    Query(q): Query<BTreeMap<String, String>>,
) -> impl IntoResponse {
    assert!(q.contains_key("query"));
    let value = state.prometheus_value.lock().unwrap().clone();
    let result = match value {
        Some(v) => json!([{"metric": {}, "value": [now_secs(), v]}]),
        None => json!([]),
    };
    axum::Json(json!({"status": "success", "data": {"resultType": "vector", "result": result}}))
}

async fn metrics_page(State(state): State<Arc<UpstreamState>>) -> impl IntoResponse {
    (StatusCode::OK, state.metrics_text.lock().unwrap().clone())
}

impl Upstream {
    pub async fn start() -> Self {
        let state = Arc::new(UpstreamState {
            recorded: Mutex::new(Vec::new()),
            behavior: Mutex::new(Arc::new(|_, _| {
                Reply::json(200, json!({"choices": [{"text": "ok"}]}))
            })),
            inflight: AtomicUsize::new(0),
            max_inflight: AtomicUsize::new(0),
            arrived: Notify::new(),
            prometheus_value: Mutex::new(None),
            metrics_text: Mutex::new(String::new()),
        });
        let router = Router::new()
            .route("/api/v1/query", get(prometheus))
            .route("/metrics", get(metrics_page))
            .fallback(record)
            .with_state(Arc::clone(&state));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, router).await });
        Self { url, state }
    }

    pub fn reply_with(&self, behavior: impl Fn(&Recorded, usize) -> Reply + Send + Sync + 'static) {
        *self.state.behavior.lock().unwrap() = Arc::new(behavior);
    }

    pub fn recorded(&self) -> Vec<Recorded> {
        self.state.recorded.lock().unwrap().clone()
    }

    pub fn count(&self) -> usize {
        self.state.recorded.lock().unwrap().len()
    }

    pub fn max_inflight(&self) -> usize {
        self.state.max_inflight.load(Ordering::SeqCst)
    }

    pub async fn wait_for_requests(&self, n: usize) -> Vec<Recorded> {
        eventually(&format!("{n} upstream requests"), || async {
            let all = self.recorded();
            (all.len() >= n).then_some(all)
        })
        .await
    }

    pub fn set_prometheus_value(&self, value: Option<&str>) {
        *self.state.prometheus_value.lock().unwrap() = value.map(str::to_owned);
    }

    pub fn set_metrics_text(&self, text: &str) {
        *self.state.metrics_text.lock().unwrap() = text.to_owned();
    }
}

/// Configuration files and flags for one processor.
pub struct Spec {
    pub transport: Value,
    pub pools: Option<Value>,
    pub merge_policy: Option<Value>,
    pub args: Vec<String>,
}

impl Spec {
    pub fn new(transport: Value) -> Self {
        Self {
            transport,
            pools: None,
            merge_policy: None,
            args: Vec::new(),
        }
    }

    pub fn pools(mut self, pools: Value) -> Self {
        self.pools = Some(pools);
        self
    }

    pub fn merge_policy(mut self, policy: Value) -> Self {
        self.merge_policy = Some(policy);
        self
    }

    pub fn arg(mut self, flag: &str, value: &str) -> Self {
        self.args.push(flag.into());
        self.args.push(value.into());
        self
    }

    /// Runs on the shared Postgres store at `db`, with large bodies in the
    /// test's object store. Partitions move between replicas a third of
    /// `lease` apart.
    pub fn postgres(self, db: &Postgres, lease: &str) -> Self {
        let blobs = db.blob_url();
        self.postgres_with_blobs(db, lease, &blobs)
    }

    pub fn postgres_with_blobs(self, db: &Postgres, lease: &str, blob_store: &str) -> Self {
        self.arg("--store", "postgres")
            .arg("--database-url", &db.url)
            .arg("--database-max-connections", "4")
            .arg("--partition-lease-ttl", lease)
            .arg("--blob-store", blob_store)
    }
}

/// One test's schema in the Postgres at `TEST_DATABASE_URL`. Without the
/// variable, Postgres tests are skipped unless `REQUIRE_POSTGRES` is set.
pub struct Postgres {
    pub url: String,
    client: tokio_postgres::Client,
    /// Stands in for the S3 bucket the replicas share.
    pub blobs: tempfile::TempDir,
}

impl Postgres {
    pub async fn schema() -> Option<Self> {
        let Ok(base) = std::env::var("TEST_DATABASE_URL") else {
            assert!(
                std::env::var_os("REQUIRE_POSTGRES").is_none(),
                "REQUIRE_POSTGRES is set but TEST_DATABASE_URL is not"
            );
            eprintln!("TEST_DATABASE_URL not set; skipping a Postgres test");
            return None;
        };
        let schema = format!("e_{:016x}", rand::random::<u64>());
        let sep = if base.contains('?') { '&' } else { '?' };
        let url = format!("{base}{sep}options=-c%20search_path%3D{schema}");
        let (client, connection) = tokio_postgres::connect(&base, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(connection);
        client
            .batch_execute(&format!(
                "CREATE SCHEMA {schema}; SET search_path = {schema}"
            ))
            .await
            .unwrap();
        Some(Self {
            url,
            client,
            blobs: tempfile::tempdir().unwrap(),
        })
    }

    pub fn blob_url(&self) -> String {
        format!("file://{}", self.blobs.path().display())
    }

    /// Partitions of `queue` held per owner.
    pub async fn partition_owners(&self, queue: &str) -> BTreeMap<String, i64> {
        self.client
            .query(
                "SELECT owner, count(*) FROM lda_partitions WHERE queue = $1 GROUP BY owner",
                &[&queue],
            )
            .await
            .map(|rows| rows.iter().map(|r| (r.get(0), r.get(1))).collect())
            .unwrap_or_default()
    }

    /// Waits until `n` replicas each hold an equal share of `queue`.
    pub async fn wait_split(&self, queue: &str, n: i64) {
        eventually(&format!("{n} replicas to split {queue}"), || async {
            let owners = self.partition_owners(queue).await;
            let live: Vec<i64> = owners
                .iter()
                .filter(|(o, _)| !o.is_empty())
                .map(|(_, c)| *c)
                .collect();
            (live.len() == n as usize && live.iter().all(|c| *c == 64 / n)).then_some(())
        })
        .await;
    }
}

/// A queue entry pointing at `upstream`.
pub fn queue(name: &str, upstream: &Upstream) -> Value {
    json!({"queue_name": name, "igw_base_url": upstream.url})
}

pub struct Processor {
    child: Option<Child>,
    args: Vec<String>,
    dir: PathBuf,
    pub api: String,
    pub health: String,
    pub metrics: String,
    pub http: reqwest::Client,
}

impl Processor {
    pub async fn start(dir: &Path, spec: Spec) -> Self {
        let write = |name: &str, v: &Value| {
            let path = dir.join(name);
            std::fs::write(&path, serde_json::to_vec_pretty(v).unwrap()).unwrap();
            path.display().to_string()
        };
        let mut args = vec![
            "--data-dir".to_owned(),
            dir.join("data").display().to_string(),
            "--transport-config-file".to_owned(),
            write("transport.json", &spec.transport),
            "--metrics-backlog-poll-interval".to_owned(),
            "200ms".to_owned(),
            "--prometheus-cache-ttl".to_owned(),
            "0s".to_owned(),
        ];
        if let Some(pools) = &spec.pools {
            args.push("--pool-config-file".into());
            args.push(write("pools.json", pools));
        }
        if let Some(policy) = &spec.merge_policy {
            args.push("--request-merge-policy-config-file".into());
            args.push(write("merge.json", policy));
        }
        args.extend(spec.args);
        let mut p = Self {
            child: None,
            args,
            dir: dir.to_owned(),
            api: String::new(),
            health: String::new(),
            metrics: String::new(),
            http: reqwest::Client::new(),
        };
        p.spawn().await;
        p
    }

    fn log_path(&self) -> PathBuf {
        self.dir.join("processor.log")
    }

    pub fn transport_path(&self) -> PathBuf {
        self.dir.join("transport.json")
    }

    pub fn data_dir(&self) -> PathBuf {
        self.dir.join("data")
    }

    /// Starts the binary (again, after an exit) and waits until it is ready.
    /// Ports are picked fresh each time; if another test grabbed one first,
    /// the process exits on bind and is started again on new ports.
    pub async fn spawn(&mut self) {
        for _ in 0..5 {
            let (api, health, metrics) = (free_port(), free_port(), free_port());
            self.api = format!("http://127.0.0.1:{api}");
            self.health = format!("http://127.0.0.1:{health}");
            self.metrics = format!("http://127.0.0.1:{metrics}");
            let log = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.log_path())
                .unwrap();
            let mut child = Command::new(env!("CARGO_BIN_EXE_llm-d-async"))
                .args(&self.args)
                .args(["--api-addr", &format!("127.0.0.1:{api}")])
                .args(["--health-port", &health.to_string()])
                .args(["--metrics-port", &metrics.to_string()])
                .env(
                    "RUST_LOG",
                    std::env::var("E2E_LOG").unwrap_or_else(|_| "info".into()),
                )
                .env_remove("OTEL_EXPORTER_OTLP_ENDPOINT")
                .stdout(Stdio::from(log.try_clone().unwrap()))
                .stderr(Stdio::from(log))
                .kill_on_drop(true)
                .spawn()
                .unwrap();
            let ready = format!("{}/readyz", self.health);
            let deadline = Instant::now() + WAIT;
            let started = loop {
                if child.try_wait().unwrap().is_some() {
                    break false;
                }
                if let Ok(r) = self.http.get(&ready).send().await
                    && r.status() == StatusCode::OK
                {
                    break true;
                }
                assert!(
                    Instant::now() < deadline,
                    "timed out waiting for processor readiness"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            };
            self.child = Some(child);
            if started {
                return;
            }
        }
        panic!("processor failed to start");
    }

    fn pid(&self) -> String {
        self.child.as_ref().unwrap().id().unwrap().to_string()
    }

    pub fn signal(&self, signal: &str) {
        let status = std::process::Command::new("kill")
            .args([signal, &self.pid()])
            .status()
            .unwrap();
        assert!(status.success());
    }

    pub async fn wait_exit(&mut self, timeout: Duration) -> ExitStatus {
        let child = self.child.as_mut().unwrap();
        tokio::time::timeout(timeout, child.wait())
            .await
            .expect("processor did not exit in time")
            .unwrap()
    }

    pub async fn kill(&mut self) {
        self.signal("-KILL");
        self.wait_exit(WAIT).await;
    }

    pub async fn submit_status(&self, body: Value) -> (StatusCode, Value) {
        let r = self
            .http
            .post(format!("{}/v1/requests", self.api))
            .json_body(&body)
            .send()
            .await
            .unwrap();
        let status = r.status();
        (
            status,
            serde_json::from_slice(&r.bytes().await.unwrap()).unwrap_or(Value::Null),
        )
    }

    /// Submits and returns the request token.
    pub async fn submit(&self, body: Value) -> String {
        let (status, v) = self.submit_status(body).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{v}");
        v["request_token"].as_str().unwrap().to_owned()
    }

    /// Claims one result, waiting up to a second. `None` when there is none.
    pub async fn claim(&self, route: &str, lease_ms: u64) -> Option<Value> {
        self.claim_waiting(route, lease_ms, 1000).await
    }

    pub async fn claim_waiting(&self, route: &str, lease_ms: u64, wait_ms: u64) -> Option<Value> {
        let r = self
            .http
            .post(format!(
                "{}/v1/results/{route}/claims?wait_ms={wait_ms}&lease_ms={lease_ms}",
                self.api
            ))
            .send()
            .await
            .unwrap();
        if r.status() == StatusCode::NO_CONTENT {
            return None;
        }
        assert_eq!(r.status(), StatusCode::OK);
        Some(serde_json::from_slice(&r.bytes().await.unwrap()).unwrap())
    }

    pub async fn ack(&self, route: &str, claim: &Value) -> StatusCode {
        self.http
            .post(format!(
                "{}/v1/results/{route}/claims/{}/ack",
                self.api, claim["claim_id"]
            ))
            .json_body(&json!({"owner_token": claim["owner_token"]}))
            .send()
            .await
            .unwrap()
            .status()
    }

    /// Claims, acknowledges and returns the next result of `route`.
    pub async fn next_result(&self, route: &str) -> Value {
        let claim = eventually(&format!("a result on {route}"), || {
            self.claim(route, 60_000)
        })
        .await;
        assert_eq!(self.ack(route, &claim).await, StatusCode::NO_CONTENT);
        claim["result"].clone()
    }

    /// Asserts no result arrives on `route` within `wait`.
    pub async fn no_result_within(&self, route: &str, wait: Duration) {
        let r = self
            .http
            .post(format!(
                "{}/v1/results/{route}/pop?wait_ms={}",
                self.api,
                wait.as_millis()
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), StatusCode::NO_CONTENT, "unexpected result");
    }

    pub async fn put(&self, path: &str, body: Value) -> StatusCode {
        self.http
            .put(format!("{}{path}", self.api))
            .json_body(&body)
            .send()
            .await
            .unwrap()
            .status()
    }

    pub async fn post(&self, path: &str, body: Value) -> (StatusCode, Value) {
        let r = self
            .http
            .post(format!("{}{path}", self.api))
            .json_body(&body)
            .send()
            .await
            .unwrap();
        let status = r.status();
        (
            status,
            serde_json::from_slice(&r.bytes().await.unwrap()).unwrap_or(Value::Null),
        )
    }

    pub async fn get(&self, path: &str) -> reqwest::Response {
        self.http
            .get(format!("{}{path}", self.api))
            .send()
            .await
            .unwrap()
    }

    pub async fn metrics_text(&self) -> String {
        self.http
            .get(format!("{}/metrics", self.metrics))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap()
    }

    /// The value of the series `name` whose labels include `labels`.
    pub async fn metric(&self, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
        let text = self.metrics_text().await;
        text.lines().find_map(|line| {
            let (series, value) = line.rsplit_once(' ')?;
            let (series_name, rest) = series.split_once('{').unwrap_or((series, "}"));
            if series_name != name {
                return None;
            }
            let rest = rest.strip_suffix('}')?;
            let matches = labels
                .iter()
                .all(|(k, v)| rest.split(',').any(|pair| pair == format!("{k}=\"{v}\"")));
            matches.then(|| value.parse().ok()).flatten()
        })
    }

    pub async fn wait_metric(&self, name: &str, labels: &[(&str, &str)], want: f64) {
        eventually(&format!("{name}{labels:?} == {want}"), || async {
            (self.metric(name, labels).await == Some(want)).then_some(())
        })
        .await;
    }

    pub async fn queue_depth(&self, queue: &str) -> u64 {
        let queues: Value =
            serde_json::from_slice(&self.get("/v1/queues").await.bytes().await.unwrap()).unwrap();
        queues
            .as_array()
            .unwrap()
            .iter()
            .find(|q| q["queue_name"] == queue)
            .map(|q| q["depth"].as_u64().unwrap())
            .unwrap_or(0)
    }
}

impl Drop for Processor {
    fn drop(&mut self) {
        if std::thread::panicking()
            && let Ok(log) = std::fs::read_to_string(self.log_path())
        {
            let tail: Vec<&str> = log.lines().rev().take(80).collect();
            eprintln!("--- processor log (last 80 lines) ---");
            for line in tail.into_iter().rev() {
                eprintln!("{line}");
            }
        }
    }
}

/// `RequestBuilder::json` needs reqwest's `json` feature; this is all we use of it.
pub trait JsonBody {
    fn json_body(self, v: &Value) -> Self;
}

impl JsonBody for reqwest::RequestBuilder {
    fn json_body(self, v: &Value) -> Self {
        self.header("content-type", "application/json")
            .body(v.to_string())
    }
}

/// A request body for queue `queue` due in `secs` seconds.
pub fn request(id: &str, secs: i64) -> Value {
    json!({
        "id": id,
        "created": now_secs(),
        "deadline": now_secs() + secs,
        "payload": {"model": "m", "prompt": id},
    })
}
