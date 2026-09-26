//! Praxis AI in front of the processor, driven with the official OpenAI SDK's
//! Responses API: foreground calls through the processor's OpenAI-compatible
//! routes, background responses through Praxis's response store, and a real
//! vLLM behind a gateway that evicts streams.
//!
//! Set `PRAXIS_AI_BIN` to a `praxis-ai` built with the `full` and
//! `store-sqlite` features (and the vLLM variables in `vllm.rs`) to run it;
//! `REQUIRE_PRAXIS=1` fails instead of skipping without it, and
//! `PRAXIS_LOG=<file>` keeps Praxis's log.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tokio::process::{Child, Command};

use crate::harness::{Processor, Spec, free_port};
use crate::vllm::{Gateway, Generate, Vllm, case};

const GENERATE: &str = "/inference/v1/generate";
const MODEL: &str = "zai-org/GLM-4.7";

struct Praxis {
    url: String,
    child: Child,
}

impl Drop for Praxis {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

/// Praxis AI's configuration: Responses in, Chat Completions to the
/// processor for foreground calls, the processor's request API for
/// background ones.
fn config(listen: u16, processor: &str, database: &Path) -> String {
    let processor_addr = processor.trim_start_matches("http://");
    format!(
        r#"
listeners:
  - name: gateway
    address: "127.0.0.1:{listen}"
    filter_chains: [responses]
filter_chains:
  - name: responses
    filters:
      - filter: openai_responses_format
        on_invalid: reject
        background: continue
      - filter: openai_responses_validate
      - filter: state_owner
        mode: single_tenant
        tenant_id: default
      - filter: openai_response_store
        backend: sqlite
        database_url: "sqlite://{database}?mode=rwc"
        responses_table: openai_responses
        conversations_table: openai_conversations
        background:
          processor_url: "{processor}"
          allow_private_processor_url: true
          objective: batch
          deadline_secs: 600
          truncation_auto: disabled
          reasoning:
            dialect: vllm
            summary: omit
      - filter: openai_responses_rehydrate
      - filter: iterative_request_router
        initial_step: inference
        max_iterations: 1
        timeout_ms: 900000
        steps:
          - name: inference
            filters:
              - filter: openai_stream_events
                timeout_secs: 900
              - filter: responses_to_chat_completions
                stream_timeout_secs: 0
                truncation_auto: disabled
                reasoning:
                  dialect: vllm
                  summary: omit
              - filter: path_rewrite
                replace:
                  pattern: "^/v1/responses/?$"
                  replacement: "/v1/chat/completions"
                conditions:
                  - when:
                      path_prefix: "/v1/responses"
                      methods: [POST]
              - filter: headers
                request_set:
                  - name: x-llm-d-inference-objective
                    value: interactive
                  - name: x-llm-d-async-timeout
                    value: 10m
              - filter: router
                routes:
                  - path_prefix: "/"
                    cluster: llm-d-async
              - filter: load_balancer
                clusters:
                  - name: llm-d-async
                    endpoints:
                      - "{processor_addr}"
            on_result:
              - default: true
                done: true
insecure_options:
  allow_private_endpoints: true
"#,
        database = database.display(),
    )
}

impl Praxis {
    async fn start(dir: &Path, processor: &str) -> Option<Self> {
        let Ok(bin) = std::env::var("PRAXIS_AI_BIN") else {
            assert!(
                std::env::var("REQUIRE_PRAXIS").is_err(),
                "REQUIRE_PRAXIS is set but PRAXIS_AI_BIN is not"
            );
            eprintln!("skipping: set PRAXIS_AI_BIN to run against Praxis AI");
            return None;
        };
        let port = free_port();
        let path = dir.join("praxis.yaml");
        let data = dir.join("praxis-data");
        std::fs::create_dir_all(&data).unwrap();
        std::fs::write(&path, config(port, processor, &data.join("responses.db"))).unwrap();
        let log_path =
            std::env::var("PRAXIS_LOG").map_or_else(|_| dir.join("praxis.log"), PathBuf::from);
        let log = std::fs::File::create(log_path).unwrap();
        let child = Command::new(bin)
            .env("RUST_LOG", "info")
            .arg("-c")
            .arg(&path)
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        while tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_err()
        {
            assert!(Instant::now() < deadline, "Praxis AI never listened");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Some(Self {
            url: format!("http://127.0.0.1:{port}"),
            child,
        })
    }

    async fn sdk(&self, scenario: &str, request: Value) -> Value {
        let script =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/e2e/responses_client.py");
        let args = json!({"base_url": self.url, "scenario": scenario, "request": request});
        let out = Command::new("uv")
            .args(["run", "--quiet", "--script"])
            .arg(&script)
            .arg(args.to_string())
            .output()
            .await
            .expect("uv runs the OpenAI SDK client");
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        serde_json::from_slice(&out.stdout)
            .unwrap_or_else(|e| panic!("{e}: {}", String::from_utf8_lossy(&out.stdout)))
    }
}

/// The Responses request Praxis translates into the chat case's request.
fn chat_request() -> Value {
    json!({"model": MODEL, "instructions": "Resume case chat. Answer in two sentences.",
           "input": "Tell me about Paris.", "max_output_tokens": 128, "temperature": 0})
}

/// The Responses request Praxis translates into the tool case's request.
fn tool_request() -> Value {
    json!({"model": MODEL, "instructions": "Resume case tool. Use tools for live data.",
           "input": "What is the weather in Paris right now?", "max_output_tokens": 128,
           "temperature": 0,
           "tools": [{"type": "function", "name": "get_weather",
                      "description": "Look up the current weather for a city.",
                      "parameters": {"type": "object", "properties": {"city": {"type": "string"}},
                                     "required": ["city"]}}]})
}

const ANSWER: &str = "Paris is the capital of France and sits on the Seine. It is known for the Eiffel Tower and the Louvre.";

fn objective(gateway: &Gateway) -> String {
    let generate = gateway
        .forwarded()
        .into_iter()
        .find(|f| f.path == GENERATE)
        .expect("a generate call");
    generate.headers["x-llm-d-inference-objective"]
        .to_str()
        .unwrap()
        .to_owned()
}

fn assert_resumed(gateway: &Gateway) {
    let chat = case("chat");
    let sent = gateway.forwarded_to(GENERATE);
    assert_eq!(sent.len(), 2, "evicted once, continued once");
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

#[tokio::test(flavor = "multi_thread")]
async fn praxis_ai_in_front_of_the_processor() {
    let Some(vllm) = Vllm::start().await else {
        return;
    };
    let gateway = Gateway::start(&vllm.url).await;
    let dir = tempfile::tempdir().unwrap();
    let p = Processor::start(
        dir.path(),
        Spec::new(json!({"poll_interval_ms": 100, "queues": [{
            "queue_name": "agents", "igw_base_url": gateway.url, "inference_objective": "batch",
            "resumable": true, "render_url": gateway.url,
        }]})),
    )
    .await;
    let Some(praxis) = Praxis::start(dir.path(), &p.api).await else {
        return;
    };
    let chat_cut = case("chat").cut;

    // Proxying a normal API into the queue: a foreground Responses call.
    gateway.clear();
    let got = praxis.sdk("foreground", chat_request()).await;
    assert_eq!(got["status"], "completed", "{got}");
    assert_eq!(got["text"], ANSWER);
    assert_eq!(got["output_types"], json!(["reasoning", "message"]));
    assert_eq!(objective(&gateway), "interactive");

    // Evicted mid-generation, invisibly to the caller.
    gateway.clear();
    gateway.then(Generate::Cut(chat_cut));
    let got = praxis.sdk("foreground", chat_request()).await;
    assert_eq!(got["text"], ANSWER, "{got}");
    assert_resumed(&gateway);

    // Background mode: queued, in progress, completed, and evicted on the way.
    gateway.clear();
    gateway.then(Generate::Cut(chat_cut));
    let got = praxis.sdk("background", chat_request()).await;
    assert_eq!(got["status"], "completed", "{got}");
    assert_eq!(got["seen"][0], "queued");
    assert_eq!(got["seen"].as_array().unwrap().last().unwrap(), "completed");
    assert_eq!(got["background"], true);
    assert_eq!(got["text"], ANSWER);
    assert_eq!(got["retrieved_again"]["text"], ANSWER);
    assert!(got["id"].as_str().unwrap().starts_with("resp_"));
    assert_eq!(objective(&gateway), "batch");
    assert_resumed(&gateway);

    // Cancelling a background response that is still generating.
    gateway.clear();
    gateway.then(Generate::Stall(1));
    let got = praxis.sdk("cancel", chat_request()).await;
    assert_eq!(
        got,
        json!({"created": "queued", "cancelled": "cancelled",
               "cancelled_again": "cancelled", "retrieved": "cancelled"})
    );

    // Function calls, foreground and background, and the next turn after one.
    // A cancel that landed before dispatch left its stall unused.
    gateway.clear();
    for scenario in ["follow_up", "background"] {
        let got = praxis.sdk(scenario, tool_request()).await;
        let first = if scenario == "follow_up" {
            &got["first"]
        } else {
            &got
        };
        assert_eq!(first["status"], "completed", "{got}");
        let call = &first["function_calls"][0];
        assert_eq!(call["name"], "get_weather");
        assert_eq!(call["arguments"], "{\"city\": \"Paris\"}");
        assert!(
            call["call_id"]
                .as_str()
                .unwrap()
                .starts_with("chatcmpl-tool-")
        );
        if scenario == "follow_up" {
            assert_eq!(got["follow_up"]["status"], "completed", "{got}");
        }
    }

    // A background turn continuing a background tool call, and a foreground
    // turn continuing that.
    gateway.clear();
    let got = praxis.sdk("background_chain", tool_request()).await;
    assert_eq!(
        got["first"]["function_calls"][0]["name"], "get_weather",
        "{got}"
    );
    // The later turns are not in the scripted trace, so the engine serves
    // random tokens that may run into max_output_tokens.
    let finished =
        |turn: &Value| matches!(turn["status"].as_str(), Some("completed" | "incomplete"));
    assert!(finished(&got["second"]), "{got}");
    assert_eq!(got["second"]["background"], true);
    assert!(finished(&got["third"]), "{got}");
    let continued = &gateway.forwarded_to("/v1/chat/completions/render")[1];
    let roles: Vec<&str> = continued["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["role"].as_str().unwrap())
        .collect();
    // The follow-up resends its instructions, as agents do (they do not carry
    // over a previous_response_id); the earlier turn's reasoning and tool
    // call come from the stored history.
    assert_eq!(
        roles,
        ["system", "user", "assistant", "tool"],
        "{continued}"
    );
    assert_eq!(
        continued["messages"][2]["tool_calls"][0]["function"]["name"],
        "get_weather"
    );
    assert!(
        continued["messages"][2]["reasoning"].is_string(),
        "{continued}"
    );

    // What agent clients send that Chat Completions cannot represent runs
    // anyway: no reasoning summary, no truncation.
    let mut agentic = chat_request();
    agentic["reasoning"] = json!({"summary": "auto"});
    agentic["truncation"] = json!("auto");
    for scenario in ["foreground", "background"] {
        let got = praxis.sdk(scenario, agentic.clone()).await;
        assert_eq!(got["status"], "completed", "{scenario}: {got}");
        assert_eq!(got["text"], ANSWER, "{scenario}");
    }

    // What background mode does not run is refused up front.
    let mut streaming = chat_request();
    streaming["stream"] = json!(true);
    let got = praxis.sdk("background", streaming).await;
    assert_eq!(got["status_code"], 400, "{got}");
}
