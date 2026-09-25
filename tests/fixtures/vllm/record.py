#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Record real vLLM 0.30.0 responses (streamed and non-streamed) against a GPU-free engine.

The Python vLLM frontend runs API-server-only (`--data-parallel-size-local 0`) and talks
the engine-core ZMQ protocol to `vllm-vcr play`, which serves scripted output token ids
from a trace matched by prompt block-hash prefix. Two passes: one to learn each case's
prompt token ids and to tokenize its scripted output, one to replay and capture bytes.
"""

from __future__ import annotations

import argparse
import copy
import http.client
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

VLLM_VERSION = "0.30.0"
BLOCK_SIZE = 16
VOCAB_SIZE = 151552
EOS_TOKEN_ID = 151329  # <|endoftext|>
MAX_MODEL_LEN = 8192

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_weather",
            "description": "Look up the current weather for a city.",
            "parameters": {
                "type": "object",
                "properties": {"city": {"type": "string"}},
                "required": ["city"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_time",
            "description": "Look up the current local time for a city.",
            "parameters": {
                "type": "object",
                "properties": {"city": {"type": "string"}},
                "required": ["city"],
            },
        },
    },
]


def sysmsg(case: str) -> str:
    return (
        f"Case {case}. You are a careful assistant that answers in one short sentence "
        "and never adds extra commentary beyond what was asked of you."
    )


def chat(case: str, user: str, **extra) -> dict:
    body = {
        "model": None,
        "messages": [
            {"role": "system", "content": sysmsg(case)},
            {"role": "user", "content": user},
        ],
        "max_tokens": 64,
        "temperature": 0,
        "n": 1,
    }
    body.update(extra)
    return body


def completion(prompt: str, **extra) -> dict:
    body = {
        "model": None,
        "prompt": prompt,
        "max_tokens": 64,
        "temperature": 0,
        "n": 1,
    }
    body.update(extra)
    return body


CASES: list[dict] = [
    {
        "name": "c-stop",
        "endpoint": "/v1/completions",
        "request": completion(
            "Case c-stop. Finish the following arithmetic sentence with a single short "
            "phrase and nothing else. The sum of two and two is",
            max_tokens=32,
        ),
        "output": " four.",
        "finish": "stop",
    },
    {
        "name": "c-length",
        "endpoint": "/v1/completions",
        "request": completion(
            "Case c-length. Count upward in English words, separated by commas, and keep "
            "going for as long as you are allowed to:",
            max_tokens=8,
        ),
        "output": " one, two, three, four, five, six, seven, eight, nine, ten",
        "finish": "length",
    },
    {
        "name": "c-stop-string",
        "endpoint": "/v1/completions",
        "request": completion(
            "Case c-stop-string. Emit a short phrase, then the marker word, then some more "
            "text that the caller is expected to never see:",
            max_tokens=48,
            stop=["END"],
        ),
        "output": " first part END second part nobody should ever read.",
        "finish": "stop",
    },
    {
        "name": "c-caller-token-ids",
        "endpoint": "/v1/completions",
        "request": completion(
            "Case c-caller-token-ids. Answer with one short phrase: on a clear day the "
            "colour of the sky above an open field is",
            max_tokens=32,
            return_token_ids=True,
        ),
        "output": " blue.",
        "finish": "stop",
    },
    {
        "name": "c-token-prompt",
        "endpoint": "/v1/completions",
        "request": completion("", max_tokens=32),
        "prompt_from_text": (
            "Case c-token-prompt. The caller passed this prompt as a list of token ids "
            "rather than as text, so the frontend must skip tokenization here:"
        ),
        "output": " understood.",
        "finish": "stop",
    },
    {
        "name": "c-unicode",
        "endpoint": "/v1/completions",
        "request": completion(
            "Case c-unicode. Reply with a greeting that mixes several scripts and at least "
            "one emoji built out of more than one code point:",
            max_tokens=48,
        ),
        "output": " Grüße, 世界! \U0001f468‍\U0001f469‍\U0001f467‍\U0001f466 \U0001fae0 café naïve — fertig.",
        "finish": "stop",
    },
    {
        "name": "c-think",
        "endpoint": "/v1/completions",
        "request": completion(
            "Case c-think. Think inside think tags first and then give the answer, all as "
            "one raw completion with no chat template involved:",
            max_tokens=64,
        ),
        "output": "<think>The caller wants a short answer. Keep it to one line.</think>Hi there!",
        "finish": "stop",
    },
    {
        "name": "c-error",
        "endpoint": "/v1/completions",
        "request": completion(
            "Case c-error. The engine is going to fail partway through this request, so the "
            "frontend has to surface a generation error:",
            max_tokens=32,
        ),
        "output": " partial output before the engine",
        "finish": "error",
    },
    {
        "name": "h-reasoning",
        "endpoint": "/v1/chat/completions",
        "request": chat("h-reasoning", "What is the capital of France?"),
        "output": "The user asks for the capital of France. That is Paris.</think>The capital of France is Paris.",
        "finish": "stop",
    },
    {
        "name": "h-no-thinking",
        "endpoint": "/v1/chat/completions",
        "request": chat(
            "h-no-thinking",
            "What is the capital of France?",
            chat_template_kwargs={"enable_thinking": False},
        ),
        "output": "Paris is the capital of France.",
        "finish": "stop",
    },
    {
        "name": "h-tool",
        "endpoint": "/v1/chat/completions",
        "request": chat(
            "h-tool",
            "What is the weather in Paris right now?",
            tools=[TOOLS[0]],
        ),
        "output": "The user wants live weather, so the weather tool is needed.</think>\n<tool_call>get_weather<arg_key>city</arg_key><arg_value>Paris</arg_value></tool_call>",
        "finish": "stop",
    },
    {
        "name": "h-two-tools",
        "endpoint": "/v1/chat/completions",
        "request": chat(
            "h-two-tools",
            "What is the weather and the local time in Paris right now?",
            tools=TOOLS,
            max_tokens=96,
        ),
        "output": "Two separate facts are needed, so both tools get called.</think>\n<tool_call>get_weather<arg_key>city</arg_key><arg_value>Paris</arg_value></tool_call>\n<tool_call>get_time<arg_key>city</arg_key><arg_value>Paris</arg_value></tool_call>",
        "finish": "stop",
    },
    {
        "name": "h-content-and-tool",
        "endpoint": "/v1/chat/completions",
        "request": chat(
            "h-content-and-tool",
            "Tell me what you are about to do, then check the weather in Paris.",
            tools=[TOOLS[0]],
            max_tokens=96,
        ),
        "output": "Announce the action first, then call the tool.</think>Let me look that up for you.\n<tool_call>get_weather<arg_key>city</arg_key><arg_value>Paris</arg_value></tool_call>",
        "finish": "stop",
    },
    {
        "name": "h-length-in-reasoning",
        "endpoint": "/v1/chat/completions",
        "request": chat(
            "h-length-in-reasoning",
            "Explain, at length, why the sky looks blue.",
            max_tokens=12,
        ),
        "output": "Rayleigh scattering is the reason, and the explanation needs several careful steps before the answer.</think>The sky looks blue because of Rayleigh scattering.",
        "finish": "length",
    },
    {
        "name": "h-caller-token-ids",
        "endpoint": "/v1/chat/completions",
        "request": chat(
            "h-caller-token-ids",
            "What is the capital of Germany?",
            return_token_ids=True,
        ),
        "output": "Straightforward geography question.</think>The capital of Germany is Berlin.",
        "finish": "stop",
    },
    {
        "name": "h-stop-string",
        "endpoint": "/v1/chat/completions",
        "request": chat(
            "h-stop-string",
            "Answer, then write the marker word, then keep writing.",
            stop=["END"],
        ),
        "output": "The caller wants the marker mid-answer.</think>Here is the answer END and here is text nobody should see.",
        "finish": "stop",
    },
    {
        "name": "h-multi-turn",
        "endpoint": "/v1/chat/completions",
        "request": {
            "model": None,
            "messages": [
                {"role": "system", "content": sysmsg("h-multi-turn")},
                {"role": "user", "content": "What is the weather in Paris?"},
                {
                    "role": "assistant",
                    "reasoning_content": "The user wants live weather, so the tool is needed.",
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "call_0",
                            "type": "function",
                            "function": {
                                "name": "get_weather",
                                "arguments": '{"city": "Paris"}',
                            },
                        }
                    ],
                },
                {"role": "tool", "tool_call_id": "call_0", "content": '{"temp_c": 17}'},
                {"role": "user", "content": "And what about Berlin?"},
            ],
            "tools": [TOOLS[0]],
            "max_tokens": 96,
            "temperature": 0,
            "n": 1,
        },
        "output": "Berlin needs the same lookup as Paris did.</think>\n<tool_call>get_weather<arg_key>city</arg_key><arg_value>Berlin</arg_value></tool_call>",
        "finish": "stop",
    },
]

SERVER_CONFIGS = {
    "default": [],
    "extras": [
        "--enable-prompt-tokens-details",
        "--enable-per-request-metrics",
        "--fingerprint-mode=custom",
        "--fingerprint-value=vllm-fixture-0.30.0",
    ],
}


def block_hashes(tokens: list[int], block_size: int = BLOCK_SIZE) -> list[int]:
    """Chained FNV-1a per full token block, matching sim_trace::prompt_block_hashes."""
    offset = 0xCBF29CE484222325
    prime = 0x00000100000001B3
    mask = (1 << 64) - 1
    out: list[int] = []
    prev = offset
    for start in range(0, len(tokens) - len(tokens) % block_size, block_size):
        h = offset
        payload = prev.to_bytes(8, "little")
        for t in tokens[start : start + block_size]:
            payload += t.to_bytes(4, "little")
        for byte in payload:
            h = ((h ^ byte) * prime) & mask
        prev = h
        out.append(h)
    return out


def rewrite_for_stream(body: dict) -> dict:
    sent = copy.deepcopy(body)
    sent["stream"] = True
    sent["stream_options"] = {"include_usage": True}
    sent["return_token_ids"] = True
    return sent


class Server:
    def __init__(self, args: argparse.Namespace, work: Path):
        self.args = args
        self.work = work
        self.vcr: subprocess.Popen | None = None
        self.frontend: subprocess.Popen | None = None
        self.frontend_args: list[str] = []

    def start(self, tag: str, extra_args: list[str], trace: Path | None) -> None:
        vcr_cmd = [
            self.args.vcr_bin,
            "play",
            "--handshake-address",
            f"tcp://127.0.0.1:{self.args.handshake_port}",
            "--vocab-size",
            str(VOCAB_SIZE),
            "--tokens-per-block",
            str(BLOCK_SIZE),
            "--output-token-chunk-size",
            "1",
            "--time-to-first-token",
            "50",
            "--inter-token-latency",
            "10",
            "--log-requests",
        ]
        if trace is not None:
            vcr_cmd += ["--replay-tokens", str(trace), "--replay-match", "prefix"]
        self.vcr_log = open(self.work / f"vcr-{tag}.log", "wb")
        self.vcr = subprocess.Popen(vcr_cmd, stdout=self.vcr_log, stderr=subprocess.STDOUT)

        self.frontend_args = [
            self.args.model,
            "--data-parallel-size=1",
            "--data-parallel-size-local=0",
            "--data-parallel-address=127.0.0.1",
            f"--data-parallel-rpc-port={self.args.handshake_port}",
            "--host=127.0.0.1",
            f"--port={self.args.port}",
            f"--max-model-len={MAX_MODEL_LEN}",
            "--reasoning-parser=glm47",
            "--tool-call-parser=glm47",
            "--enable-auto-tool-choice",
        ] + extra_args
        env = dict(os.environ, HF_HUB_DISABLE_XET="1")
        self.fe_log = open(self.work / f"frontend-{tag}.log", "wb")
        self.frontend = subprocess.Popen(
            [self.args.vllm_bin, "serve", *self.frontend_args],
            stdout=self.fe_log,
            stderr=subprocess.STDOUT,
            env=env,
        )
        self.wait_ready()

    def wait_ready(self, timeout: float = 420.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.frontend is not None and self.frontend.poll() is not None:
                raise SystemExit("frontend exited during startup; see the log in the work dir")
            try:
                status, _, _ = self.request("GET", "/v1/models", None, timeout=5)
                if status == 200:
                    return
            except (OSError, http.client.HTTPException):
                pass
            time.sleep(2)
        raise SystemExit("frontend never became ready")

    def stop(self) -> None:
        for proc in (self.frontend, self.vcr):
            if proc is None:
                continue
            proc.send_signal(signal.SIGTERM)
        for proc in (self.frontend, self.vcr):
            if proc is None:
                continue
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()
        self.frontend = None
        self.vcr = None
        wait_port_free(self.args.port)
        wait_port_free(self.args.handshake_port)

    def request(
        self, method: str, path: str, body: dict | None, timeout: float = 300.0
    ) -> tuple[int, str, bytes]:
        conn = http.client.HTTPConnection("127.0.0.1", self.args.port, timeout=timeout)
        try:
            payload = None if body is None else json.dumps(body).encode()
            headers = {"Content-Type": "application/json"} if payload else {}
            conn.request(method, path, payload, headers)
            resp = conn.getresponse()
            return resp.status, resp.getheader("Content-Type") or "", resp.read()
        finally:
            conn.close()

    def json_post(self, path: str, body: dict) -> dict:
        status, _, raw = self.request("POST", path, body)
        if status != 200:
            raise SystemExit(f"{path} returned {status}: {raw[:400]!r}")
        return json.loads(raw)


def wait_port_free(port: int, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        with socket.socket() as s:
            s.settimeout(1)
            if s.connect_ex(("127.0.0.1", port)) != 0:
                return
        time.sleep(1)


def probe(server: Server, model: str) -> dict[str, dict]:
    """Learn each case's prompt token ids and its scripted output token ids."""
    plans: dict[str, dict] = {}
    for case in CASES:
        body = copy.deepcopy(case["request"])
        body["model"] = model
        if "prompt_from_text" in case:
            tok = server.json_post(
                "/tokenize",
                {
                    "model": model,
                    "prompt": case["prompt_from_text"],
                    "add_special_tokens": False,
                },
            )
            body["prompt"] = tok["tokens"]

        probe_body = copy.deepcopy(body)
        probe_body["max_tokens"] = 1
        probe_body["return_token_ids"] = True
        probe_body.pop("stop", None)
        got = server.json_post(case["endpoint"], probe_body)
        if case["endpoint"] == "/v1/completions":
            prompt_ids = got["choices"][0]["prompt_token_ids"]
        else:
            prompt_ids = got["prompt_token_ids"]

        out = server.json_post(
            "/tokenize",
            {"model": model, "prompt": case["output"], "add_special_tokens": False},
        )
        out_ids = list(out["tokens"])
        if case["finish"] == "length":
            limit = body["max_tokens"]
            if len(out_ids) < limit:
                raise SystemExit(f"{case['name']}: scripted output shorter than max_tokens")
            out_ids = out_ids[:limit]
        elif case["finish"] == "stop":
            out_ids.append(EOS_TOKEN_ID)

        chain = block_hashes(prompt_ids)
        if not chain:
            raise SystemExit(f"{case['name']}: prompt is shorter than one {BLOCK_SIZE}-token block")
        plans[case["name"]] = {
            "request": body,
            "prompt_token_ids": prompt_ids,
            "block_hashes": chain,
            "output_token_ids": out_ids,
        }

    chains = {name: tuple(p["block_hashes"]) for name, p in plans.items()}
    if len(set(chains.values())) != len(chains):
        raise SystemExit(f"two cases share an identical prompt block-hash chain: {chains}")
    return plans


def write_trace(path: Path, plans: dict[str, dict], model: str) -> None:
    lines = [json.dumps({"meta": {"model": model, "block_size": BLOCK_SIZE, "source": "scripted"}})]
    arrival = 0.0
    for case in CASES:
        plan = plans[case["name"]]
        ids = plan["output_token_ids"]
        lines.append(
            json.dumps(
                {
                    "prompt_tokens": len(plan["prompt_token_ids"]),
                    "output_tokens": len(ids),
                    "ttft_ms": 50.0,
                    "itl_ms": [10.0] * max(len(ids) - 1, 0),
                    "concurrency": 1,
                    "arrival_ms": arrival,
                    "block_hashes": plan["block_hashes"],
                    "output_token_ids": ids,
                    "finish_reason": case["finish"],
                }
            )
        )
        arrival += 1.0
    path.write_text("\n".join(lines) + "\n")


def send_all(server: Server, plans: dict[str, dict], stream: bool) -> dict[str, tuple[int, str, bytes]]:
    """Send every case's request once, in case order, against a freshly started stack.

    The two variants run in separate passes so each sees the same prefix-cache state;
    sending them back to back would let the second warm on the first and skew
    `usage.prompt_tokens_details.cached_tokens`.
    """
    out: dict[str, tuple[int, str, bytes]] = {}
    for case in CASES:
        body = plans[case["name"]]["request"]
        if stream:
            body = rewrite_for_stream(body)
        out[case["name"]] = server.request("POST", case["endpoint"], body)
    return out


def write_case_files(
    plans: dict[str, dict],
    plain: dict[str, tuple[int, str, bytes]],
    streamed: dict[str, tuple[int, str, bytes]],
    server_args: list[str],
    out_root: Path,
    config: str,
    model: str,
) -> None:
    for case in CASES:
        name = case["name"]
        body = plans[name]["request"]
        sent = rewrite_for_stream(body)
        status, ctype, response = plain[name]
        s_status, s_ctype, stream = streamed[name]

        case_dir = out_root / config / name
        case_dir.mkdir(parents=True, exist_ok=True)
        (case_dir / "request.json").write_text(json.dumps(body, indent=2, ensure_ascii=False) + "\n")
        (case_dir / "sent.json").write_text(json.dumps(sent, indent=2, ensure_ascii=False) + "\n")
        (case_dir / "response.json").write_bytes(response)
        (case_dir / "stream.sse").write_bytes(stream)
        (case_dir / "meta.json").write_text(
            json.dumps(
                {
                    "endpoint": case["endpoint"],
                    "response_status": status,
                    "response_content_type": ctype,
                    "stream_status": s_status,
                    "stream_content_type": s_ctype,
                    "vllm_version": VLLM_VERSION,
                    "model": model,
                    "server_args": server_args,
                    "scripted_output": case["output"],
                },
                indent=2,
                ensure_ascii=False,
            )
            + "\n"
        )
        print(f"  {config}/{name}: {status} / {s_status} ({len(stream)} sse bytes)")


def main() -> None:
    here = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--vcr-bin", default=os.environ.get("VCR_BIN"), help="path to the vllm-vcr binary")
    ap.add_argument(
        "--vllm-bin",
        default=os.environ.get("VLLM_BIN", shutil.which("vllm")),
        help="path to the vllm 0.30.0 CLI",
    )
    ap.add_argument("--model", default=os.environ.get("MODEL", "zai-org/GLM-4.7"))
    ap.add_argument("--out", default=str(here / f"v{VLLM_VERSION}"))
    ap.add_argument("--work", default=os.environ.get("WORK_DIR"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8100")))
    ap.add_argument(
        "--handshake-port", type=int, default=int(os.environ.get("HANDSHAKE_PORT", "29551"))
    )
    ap.add_argument("--config", action="append", choices=sorted(SERVER_CONFIGS), default=None)
    args = ap.parse_args()

    if not args.vcr_bin or not Path(args.vcr_bin).is_file():
        ap.error("--vcr-bin / VCR_BIN must point at a vllm-vcr binary")
    if not args.vllm_bin or not Path(args.vllm_bin).is_file():
        ap.error("--vllm-bin / VLLM_BIN must point at the vllm CLI")

    work = Path(args.work) if args.work else Path(tempfile.mkdtemp(prefix="vllm-record-"))
    work.mkdir(parents=True, exist_ok=True)
    out_root = Path(args.out)
    configs = args.config or list(SERVER_CONFIGS)
    print(f"work dir: {work}", file=sys.stderr)

    server = Server(args, work)
    print("pass 1: probing prompt and output token ids")
    server.start("probe", SERVER_CONFIGS["default"], None)
    try:
        plans = probe(server, args.model)
    finally:
        server.stop()

    trace = work / "trace.jsonl"
    write_trace(trace, plans, args.model)
    print(f"wrote {trace}")

    for config in configs:
        print(f"pass 2: recording config {config}, non-streamed")
        server.start(f"{config}-plain", SERVER_CONFIGS[config], trace)
        try:
            plain = send_all(server, plans, stream=False)
            server_args = server.frontend_args
        finally:
            server.stop()

        print(f"pass 3: recording config {config}, streamed")
        server.start(f"{config}-stream", SERVER_CONFIGS[config], trace)
        try:
            streamed = send_all(server, plans, stream=True)
        finally:
            server.stop()

        write_case_files(plans, plain, streamed, server_args, out_root, config, args.model)


if __name__ == "__main__":
    main()
