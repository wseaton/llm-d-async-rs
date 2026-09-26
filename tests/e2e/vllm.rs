//! A real vLLM 0.30 frontend serving a GPU-free engine (`vllm-vcr play`)
//! that replays the scripted outputs in `tests/fixtures/vllm/v0.30.0/resume`,
//! and a gateway in front of it that forwards everything and can cut, stall
//! or refuse `/inference/v1/generate` streams the way llm-d flow control
//! evicts them.
//!
//! Set `VLLM_BIN` (the vLLM 0.30.0 CLI) and `VCR_BIN` (vllm-vcr built for
//! the 0.30 protocol) to run these tests; `REQUIRE_VLLM=1` fails instead of
//! skipping without them. See `tests/fixtures/vllm/README.md`.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::Response;
use serde_json::{Value, json};
use tokio::process::{Child, Command};
use tokio_stream::StreamExt;

use crate::harness::free_port;

const MODEL: &str = "zai-org/GLM-4.7";
const MAX_MODEL_LEN: &str = "8192";
const VOCAB_SIZE: &str = "151552";
const STARTUP: Duration = Duration::from_secs(420);

fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/vllm/v0.30.0/resume")
}

/// A scripted case from `cases.json`.
#[derive(Clone, Debug)]
pub struct Case {
    pub endpoint: String,
    pub request: Value,
    pub cut: usize,
    pub prompt_token_ids: Vec<u32>,
    pub output_token_ids: Vec<u32>,
}

pub fn case(name: &str) -> Case {
    let cases: Vec<Value> =
        serde_json::from_slice(&std::fs::read(fixtures().join("cases.json")).unwrap()).unwrap();
    let c = cases
        .into_iter()
        .find(|c| c["name"] == name)
        .unwrap_or_else(|| panic!("no case {name}"));
    let ids = |v: &Value| -> Vec<u32> { serde_json::from_value(v.clone()).unwrap() };
    Case {
        endpoint: c["endpoint"].as_str().unwrap().to_owned(),
        request: c["request"].clone(),
        cut: c["cut"].as_u64().unwrap() as usize,
        prompt_token_ids: ids(&c["prompt_token_ids"]),
        output_token_ids: ids(&c["output_token_ids"]),
    }
}

pub struct Vllm {
    pub url: String,
    children: Vec<Child>,
    _logs: tempfile::TempDir,
}

impl Vllm {
    /// Starts the stack, or `None` when its binaries are not configured.
    pub async fn start() -> Option<Self> {
        let (Ok(vllm), Ok(vcr)) = (std::env::var("VLLM_BIN"), std::env::var("VCR_BIN")) else {
            assert!(
                std::env::var("REQUIRE_VLLM").is_err(),
                "REQUIRE_VLLM is set but VLLM_BIN or VCR_BIN is not"
            );
            eprintln!("skipping: set VLLM_BIN and VCR_BIN to run against real vLLM");
            return None;
        };
        let logs = tempfile::tempdir().unwrap();
        let log = |name: &str| std::fs::File::create(logs.path().join(name)).unwrap();
        let handshake = free_port();
        let port = free_port();
        let engine = Command::new(vcr)
            .args(["play", "--handshake-address"])
            .arg(format!("tcp://127.0.0.1:{handshake}"))
            .args(["--vocab-size", VOCAB_SIZE, "--tokens-per-block", "16"])
            .args(["--output-token-chunk-size", "1"])
            .args(["--time-to-first-token", "20", "--inter-token-latency", "5"])
            .arg("--replay-tokens")
            .arg(fixtures().join("trace.jsonl"))
            .args(["--replay-match", "prefix"])
            .stdout(Stdio::from(log("vcr.log")))
            .stderr(Stdio::from(log("vcr.err")))
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let frontend = Command::new(vllm)
            .args(["serve", MODEL])
            .args(["--data-parallel-size=1", "--data-parallel-size-local=0"])
            .arg("--data-parallel-address=127.0.0.1")
            .arg(format!("--data-parallel-rpc-port={handshake}"))
            .arg("--host=127.0.0.1")
            .arg(format!("--port={port}"))
            .arg(format!("--max-model-len={MAX_MODEL_LEN}"))
            .args(["--reasoning-parser=glm47", "--tool-call-parser=glm47"])
            .args(["--enable-auto-tool-choice", "--enable-scale-out"])
            .env("HF_HUB_OFFLINE", "1")
            .stdout(Stdio::from(log("frontend.log")))
            .stderr(Stdio::from(log("frontend.err")))
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let stack = Self {
            url: format!("http://127.0.0.1:{port}"),
            children: vec![frontend, engine],
            _logs: logs,
        };
        stack.wait_ready().await;
        Some(stack)
    }

    async fn wait_ready(&self) {
        let http = reqwest::Client::new();
        let deadline = Instant::now() + STARTUP;
        loop {
            let ready = http
                .get(format!("{}/v1/models", self.url))
                .send()
                .await
                .is_ok_and(|r| r.status().is_success());
            if ready {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "vLLM never became ready; logs in {}",
                self._logs.path().display()
            );
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}

impl Drop for Vllm {
    fn drop(&mut self) {
        for child in &mut self.children {
            let _ = child.start_kill();
        }
    }
}

/// What the gateway does with the next generate stream.
#[derive(Clone, Copy, Debug)]
pub enum Generate {
    /// Ends the stream without `[DONE]` after this many output token chunks.
    Cut(usize),
    /// Holds the stream open after this many output token chunks.
    Stall(usize),
    /// Holds the stream after this many output token chunks for a while,
    /// then goes on.
    Pause(usize, Duration),
    /// Refuses with 429, as flow control sheds.
    Shed,
}

/// A request the gateway forwarded.
#[derive(Clone, Debug)]
pub struct Forwarded {
    pub path: String,
    pub headers: axum::http::HeaderMap,
    pub body: Value,
}

#[derive(Default)]
struct GatewayState {
    upstream: String,
    generate: Mutex<VecDeque<Generate>>,
    failing_derenders: Mutex<usize>,
    forwarded: Mutex<Vec<Forwarded>>,
}

#[derive(Clone)]
pub struct Gateway {
    pub url: String,
    state: Arc<GatewayState>,
}

impl Gateway {
    pub async fn start(upstream: &str) -> Self {
        let state = Arc::new(GatewayState {
            upstream: upstream.to_owned(),
            ..GatewayState::default()
        });
        let router = Router::new()
            .fallback(forward)
            .with_state(Arc::clone(&state));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, router).await });
        Self { url, state }
    }

    /// Applies `action` to the next generate stream, after earlier ones.
    pub fn then(&self, action: Generate) {
        self.state.generate.lock().unwrap().push_back(action);
    }

    /// Fails the next `n` derender requests with 503.
    pub fn fail_derenders(&self, n: usize) {
        *self.state.failing_derenders.lock().unwrap() = n;
    }

    pub fn forwarded(&self) -> Vec<Forwarded> {
        self.state.forwarded.lock().unwrap().clone()
    }

    pub fn forwarded_to(&self, path: &str) -> Vec<Value> {
        self.forwarded()
            .into_iter()
            .filter(|f| f.path == path)
            .map(|f| f.body)
            .collect()
    }

    /// Forgets forwarded requests and drops actions no stream used.
    pub fn clear(&self) {
        self.state.forwarded.lock().unwrap().clear();
        self.state.generate.lock().unwrap().clear();
        *self.state.failing_derenders.lock().unwrap() = 0;
    }
}

fn is_token_chunk(event: &[u8]) -> bool {
    let Some(data) = event.strip_prefix(b"data: ") else {
        return false;
    };
    serde_json::from_slice::<Value>(data)
        .ok()
        .and_then(|v| v["choices"].as_array().map(|c| !c.is_empty()))
        .unwrap_or(false)
}

async fn forward(State(state): State<Arc<GatewayState>>, request: Request) -> Response {
    let path = request.uri().path().to_owned();
    let query = request
        .uri()
        .query()
        .map(|q| format!("?{q}"))
        .unwrap_or_default();
    let method = request.method().clone();
    let headers = request.headers().clone();
    let body = axum::body::to_bytes(request.into_body(), usize::MAX)
        .await
        .unwrap();
    state.forwarded.lock().unwrap().push(Forwarded {
        path: path.clone(),
        headers: headers.clone(),
        body: serde_json::from_slice(&body).unwrap_or(Value::Null),
    });
    let action = if path == "/inference/v1/generate" {
        state.generate.lock().unwrap().pop_front()
    } else {
        None
    };
    if path.ends_with("/derender") {
        let mut failing = state.failing_derenders.lock().unwrap();
        if *failing > 0 {
            *failing -= 1;
            return Response::builder()
                .status(StatusCode::SERVICE_UNAVAILABLE)
                .body(Body::from(json!({"error": "derender down"}).to_string()))
                .unwrap();
        }
    }
    if let Some(Generate::Shed) = action {
        return Response::builder()
            .status(StatusCode::TOO_MANY_REQUESTS)
            .header("retry-after", "0")
            .header("x-llm-d-request-dropped-reason", "evicted")
            .body(Body::from(
                json!({"error": "evicted by flow control"}).to_string(),
            ))
            .unwrap();
    }

    let mut upstream = reqwest::Client::new()
        .request(method, format!("{}{path}{query}", state.upstream))
        .body(body);
    for (name, value) in &headers {
        if name != "host" && name != "content-length" {
            upstream = upstream.header(name, value);
        }
    }
    let reply = upstream.send().await.unwrap();
    let mut response = Response::builder().status(reply.status().as_u16());
    for (name, value) in reply.headers() {
        if name != "content-length" && name != "transfer-encoding" {
            response = response.header(name, value);
        }
    }
    let Some(action) = action else {
        let stream = reply
            .bytes_stream()
            .map(|c| c.map_err(std::io::Error::other));
        return response.body(Body::from_stream(stream)).unwrap();
    };

    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Bytes, std::io::Error>>(16);
    tokio::spawn(async move {
        let limit = match action {
            Generate::Cut(n) | Generate::Stall(n) | Generate::Pause(n, _) => n,
            Generate::Shed => unreachable!("shed replies before forwarding"),
        };
        let mut upstream = reply.bytes_stream();
        let mut pending = Vec::new();
        let mut sent = 0;
        while let Some(Ok(chunk)) = upstream.next().await {
            pending.extend_from_slice(&chunk);
            while let Some(end) = pending.windows(2).position(|w| w == b"\n\n") {
                let event: Vec<u8> = pending.drain(..end + 2).collect();
                if sent == limit {
                    match action {
                        Generate::Stall(_) => std::future::pending::<()>().await,
                        Generate::Pause(_, pause) => tokio::time::sleep(pause).await,
                        _ => {
                            let _ = tx
                                .send(Err(std::io::Error::other("gateway evicted the stream")))
                                .await;
                            return;
                        }
                    }
                }
                sent += usize::from(is_token_chunk(event.trim_ascii_end()));
                if tx.send(Ok(Bytes::from(event))).await.is_err() {
                    return;
                }
            }
        }
    });
    response
        .body(Body::from_stream(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        ))
        .unwrap()
}
