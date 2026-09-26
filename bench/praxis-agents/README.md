# Agents and LLM tools through Praxis AI and llm-d-async

This experiment puts [Praxis AI](https://github.com/praxis-proxy/ai) in front of
llm-d-async so that ordinary agents, SDKs and eval harnesses get queueing,
priority, deadlines and eviction-safe generation without code changes.

```text
 OpenAI SDK / agent ──► Praxis AI ──────────────► llm-d-async ──────────► gateway + EPP ──► vLLM
   Responses API        Responses ⇄ Chat           durable queues          llm-d-router      render
                        response store             deadlines, retries,     flow control,     generate
                        background lifecycle       resume from tokens      eviction          derender
```

Status (2026-09-26): all three goals work end to end against a real vLLM 0.30
frontend with the official OpenAI SDK, with generate streams evicted
mid-generation. They have not yet been run on a GPU cluster.

## Goals

| Goal | How | Status |
|---|---|---|
| **1. Proxy normal APIs into async** | Praxis translates Responses to Chat Completions and calls llm-d-async's OpenAI-compatible routes, which queue the request and hold the connection until its result | works |
| **2. Background mode** (`background: true`) | Praxis's response store submits the request to llm-d-async, stores it `queued`, and on `GET /v1/responses/{id}` claims the result or reports `in_progress`; `POST …/cancel` cancels | works |
| **3. Seamless eviction and resume** | llm-d-async sends every attempt through vLLM render → `/inference/v1/generate` → derender; an evicted stream continues from the rendered prompt plus the saved tokens | works, token-identical |

## What was built

### llm-d-async (this repository)

- **One resume path.** A resumable queue sends completions and chat requests
  through vLLM's token layer: `render_url` renders the request to prompt token
  IDs, the gateway streams `/inference/v1/generate`, and `render_url` derenders
  the output into the response the caller asked for. An interrupted generation
  retries from prompt + saved tokens with `max_tokens`/`min_tokens` reduced.
  vLLM's own tool and reasoning parsers run in derender, so any parser works. A
  generation that finished but failed to derender keeps its tokens and only
  derenders on retry. See the main README's *Resumable queues*.
- **OpenAI-compatible routes.** `POST /v1/chat/completions` and
  `POST /v1/completions` submit, wait and answer. Headers:
  `x-llm-d-async-queue`, `x-llm-d-async-timeout` (Go duration),
  `x-llm-d-inference-objective`, `x-llm-d-inference-fairness-id`. `stream: true`
  callers are queued unstreamed (so they can resume), get `: keepalive` every
  10 s, then the whole response as chunks. A caller that disconnects cancels
  its request, which only works before dispatch.
- **Results by request.** A submission with `"result_delivery": "request"`
  gets its own result route `@request-<id>`, claimed, renewed and acknowledged
  like any route. `GET /v1/requests/{id}` reports `queued`, `in_progress` or
  `done`. Result writes wake only the long polls on their route, across
  Postgres replicas too.
- **Usage.** Cached prompt tokens come from the generate stream (vLLM with
  `--enable-prompt-tokens-details`), capped at the caller's prompt after a
  resume.
- **Objective header.** The queue's objective is sent as
  `x-llm-d-inference-objective`; a request's own header replaces it.

### Praxis AI (patches in [`praxis-ai/`](praxis-ai))

Against `praxis-proxy/ai` at `b9d60167` (Praxis core 0.7.0), applied in order
with `git apply`:

1. [`0001`](praxis-ai/0001-reload-config-only-when-its-content-changed.patch):
   **config watcher fix.** The watcher observed the config's directory and
   reloaded every pipeline on any write there, so a SQLite response store next
   to the config reloaded continuously. It now reloads only when the config's
   content changed, and still sees atomic replaces and ConfigMap symlink swaps.
   Includes a regression test that fails without the fix.
2. [`0002`](praxis-ai/0002-run-background-responses-through-llm-d-async.patch):
   **background mode and agent compatibility.**
   - `openai_response_store` gains a `background:` section
     (`processor_url`, `queue`, `objective`, `deadline_secs`, `timeout_ms`,
     `allow_private_processor_url`, `reasoning`, `truncation_auto`).
   - `openai_responses_format` and `openai_responses_request` take
     `background: continue` (default `reject`).
   - `POST /v1/responses/{id}/cancel` is served.
   - Background continuations (`previous_response_id`) replay stored history
     like foreground rehydration.
   - `reasoning.summary: omit` runs requests that ask for a reasoning summary
     the backend cannot produce, returning reasoning without one.
     `truncation_auto: disabled` runs `truncation: "auto"` untruncated and
     reports `disabled`. Both default to rejecting, as before; agent clients
     commonly send both.
   - Example config `examples/configs/openai/responses/background-llm-d-async.yaml`
     with functional tests, unit tests, and regenerated filter docs.

## Configuration

llm-d-async queue (`transport.json`):

```json
{"queues": [{
  "queue_name": "agents", "igw_base_url": "http://gateway:8000",
  "inference_objective": "batch",
  "resumable": true, "render_url": "http://vllm-render:8000"
}]}
```

`render_url` is any vLLM serving the queue's model with `--enable-scale-out`
(render and derender use no GPU). Run vLLM with
`--enable-prompt-tokens-details` to report cached tokens.

Praxis AI: see `examples/configs/openai/responses/background-llm-d-async.yaml`
from patch 0002; `tests/e2e/praxis.rs` generates the same configuration with
test ports. The points that matter:

- Foreground: `responses_to_chat_completions` → `path_rewrite` to
  `/v1/chat/completions` → a `headers` filter **inside the IRR step** setting
  `x-llm-d-inference-objective` and `x-llm-d-async-timeout` (headers set before
  the IRR do not reach its sub-requests) → the llm-d-async cluster.
- Timeouts must outlast the deadline, because llm-d-async holds the
  connection while the request waits: IRR `timeout_ms`,
  `openai_stream_events.timeout_secs`, and
  `responses_to_chat_completions.stream_timeout_secs: 0`.
- Keep the response store's database outside the config file's directory on
  Praxis builds without patch 0001.

## Evidence

Everything below runs locally with no GPU: the vLLM 0.30 frontend over
`vllm-vcr`, which replays scripted output tokens
(`tests/fixtures/vllm/v0.30.0/resume/trace.jsonl`), behind a gateway that
cuts, stalls or sheds generate streams the way flow control evicts them.

- **Fixtures** (`tests/fixtures/vllm`, recorded from Python vLLM 0.30): for
  every case, the render → generate → derender response equals vLLM's native
  non-streamed response, except per-run IDs and timestamps, `system_fingerprint`,
  `stop_reason`, reasoning-token counts and per-request metrics. Every cut
  point of every case resumes into the identical token sequence.
- **`tests/e2e/resumable.rs`**, the processor against real vLLM:
  - fresh chat, completion and tool-call requests match vLLM's own responses;
  - evicted mid-stream, the same requests resume into identical responses;
  - a shed continuation is sent again;
  - a failed derender re-derenders without regenerating;
  - a render error surfaces as vLLM's 400;
  - ineligible requests pass through as submitted;
  - a draining replica hands its progress to the next.
- **`tests/e2e/facade.rs`**, the official OpenAI Python SDK against the
  OpenAI-compatible routes, on the embedded store and Postgres:
  - streamed and non-streamed responses match vLLM's, including evicted runs;
  - an objective header reaches the gateway;
  - a missed deadline gives 504;
  - a caller that disconnects has its queued request cancelled, never sent.
- **`tests/e2e/praxis.rs`**, the OpenAI SDK's Responses API through Praxis AI:
  - foreground, and foreground evicted after 24 tokens (the continuation
    carries 20 prompt + 24 saved tokens), with the same answer and reasoning;
  - background `queued → in_progress → completed`, evicted on the way;
  - idempotent cancel;
  - function calls in both modes;
  - a background turn continuing a background tool call, then a foreground
    turn continuing that;
  - agent-style `reasoning.summary: "auto"` and `truncation: "auto"`;
  - background + stream refused.
- **Prefix reuse across agent turns**: turn 2's rendered prompt shares 195 of
  turn 1's 207 prompt-plus-output tokens (about 90% of turn 2's prompt). The
  prior reasoning replays intact; the difference is the chat template
  re-serializing the tool call, as native chat serving does too.

Earlier cluster runs (llm-d-router v0.10.0, vLLM 0.30.0, the text-continuation
path this replaces) showed the value of resuming under flow-control eviction:
token-identical output at 1.5B; interactive TTFT held at the no-load floor
(p95 0.44 s vs 6.5 s) with eviction; restarting instead of resuming dropped
deadline hit rates to 1%; precise prefix routing cut prefill 32% on 4 × 14B.
Details: [`../resume-demo/eviction/README.md`](../resume-demo/eviction/README.md).

## Running it

```sh
S=$(mktemp -d)
# vLLM 0.30 CPU frontend (macOS arm64 wheel; use the matching one elsewhere)
uv venv --python 3.12 $S/vllm-venv
VIRTUAL_ENV=$S/vllm-venv uv pip install \
  'https://github.com/vllm-project/vllm/releases/download/v0.30.0/vllm-0.30.0+cpu-cp312-cp312-macosx_11_0_arm64.whl'
# vllm-vcr for the 0.30 engine protocol: in a vllm-vcr checkout, add a compat.toml
# line (tag v0.30.0, protocol_rev ced6857afa0ea7b2e3f0846a62e1394e90f15607,
# default = true), point vllm-engine-core-client at that rev, then
cargo build --release --bin vllm-vcr
# Praxis AI with both patches
git -C praxis-ai apply 0001-*.patch 0002-*.patch
cargo build -p praxis-ai-proxy --features full,store-sqlite   # in praxis-ai

VLLM_BIN=$S/vllm-venv/bin/vllm VCR_BIN=<vllm-vcr> REQUIRE_VLLM=1 \
PRAXIS_AI_BIN=<praxis-ai>/target/debug/praxis-ai REQUIRE_PRAXIS=1 \
TEST_DATABASE_URL=postgres://… REQUIRE_POSTGRES=1 \
cargo test --test e2e
```

The frontend loads `zai-org/GLM-4.7`'s tokenizer from the Hugging Face cache
offline; fetch it once. The SDK clients run through `uv`.

## Known limits

- **Streaming is buffered.** A streamed call waits in the queue and gets its
  events at the end, so time to first token is the whole generation. That is
  fine for background agents, and it is what makes resume possible.
  Separately, Praxis's `responses_to_chat_completions` rejects streaming
  Responses when a reasoning dialect is configured.
- **Derender does not report** reasoning-token counts (Praxis reports `0`),
  `stop_reason`, or `system_fingerprint`. Stop strings, stop token IDs, prompt
  truncation, logprobs, seeds, structured output, forced tool choice,
  penalties, `n > 1` and caller streams on the processor's queue API are sent as
  submitted and restart on eviction.
- **Background mode** runs without streaming, `conversation`, or server-side
  tools (those need Praxis's agentic loop to run detached from the client).
  Cancel is pre-dispatch at the processor; an in-flight generation finishes
  and its result is discarded. DELETE does not cancel at the processor.
- `in_progress` means claimed by a processor, including its dispatch buffer.
- Praxis's llm-d `ext_proc` mode (`integrations/llmd/ext-proc`) cannot carry
  EPP eviction yet: the EPP evicts with an unsolicited `ImmediateResponse`
  during the response (llm-d-router v0.10.0 `pkg/epp/handlers/server.go:392`),
  but the filter only reads the stream in the request phase and supports
  `response_body_mode: none`. The inner gateway stays Envoy-based until it
  does.
- The Go and Python clients do not expose `result_delivery` or request status.

## Next

1. The 14B cluster run: a multi-turn agent workload through Praxis AI under
   interactive bursts. Measure resumes, tail latency, and prefix hits with
   precise routing.
2. Upstream: vLLM derender reasoning-token counts (`count_reasoning_tokens`);
   Praxis PRs for patches 0001 and 0002; Praxis ext_proc response-phase
   eviction.
3. Detached background agentic loops in Praxis (server-side tools).
