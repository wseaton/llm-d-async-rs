# vLLM response corpus

Real Python vLLM 0.30.0 HTTP responses for GLM-4.7, recorded streamed and non-streamed for
the same request. The engine is `vllm-vcr play`, which serves scripted output token ids over
the engine-core ZMQ protocol, so no GPU and no model weights are involved.

Layout: `v0.30.0/<server-config>/<case>/` holds `request.json` (the caller's body),
`sent.json` (the body rewritten with `stream`, `stream_options.include_usage` and
`return_token_ids`), `response.json` and `stream.sse` (raw response bytes) and `meta.json`
(status, content type, server args, the scripted output text).

## Regenerating

```sh
# vLLM 0.30.0 frontend (macOS arm64 CPU wheel; use the matching wheel on other platforms)
uv venv --python 3.12 venv
VIRTUAL_ENV=$PWD/venv uv pip install \
  'https://github.com/vllm-project/vllm/releases/download/v0.30.0/vllm-0.30.0+cpu-cp312-cp312-macosx_11_0_arm64.whl'

# vllm-vcr pinned to the vLLM v0.30.0 commit: in a checkout of vllm-vcr, add a compat.toml
# line with protocol_rev = ced6857afa0ea7b2e3f0846a62e1394e90f15607 and default = true, point
# workspace.dependencies.vllm-engine-core-client at the same rev, then
cargo build --release --bin vllm-vcr

VCR_BIN=/path/to/vllm-vcr VLLM_BIN=$PWD/venv/bin/vllm ./record.py
```

`--config default` or `--config extras` records one server config only; `--out`, `--port`,
`--handshake-port`, `--work` and `--model` override the rest. The recorder starts and stops
both processes itself: one pass to learn prompt token ids and tokenize the scripted outputs,
then a non-streamed pass and a streamed pass per config.

## Known non-determinism

`system_fingerprint` under the `default` config, tool-call ids, request ids, `created`, and
`metrics` timings change every run. The `extras` config pins the fingerprint.

Chat cases also hold `rendered.json` and `derendered.json`, vLLM's
`/v1/chat/completions/render` of the request and `/v1/chat/completions/derender`
of the recorded tokens, written by `record_derender.py` against a vLLM started
with the corpus flags plus `--enable-scale-out`.
