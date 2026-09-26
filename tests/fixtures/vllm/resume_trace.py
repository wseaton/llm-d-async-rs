#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Write the vllm-vcr trace the resumable end-to-end tests serve.

Each case's scripted output is served whole to a fresh attempt and, from its
cut onward, to the continuation of an attempt cut there: vcr matches a prompt
to the record with the longest block-hash prefix, and a continuation's prompt
(the rendered prompt plus the saved tokens) is at least one block longer than
the fresh prompt. Fresh copies come first in each case, so a fresh attempt
never takes the continuation record, whose chain also holds the fresh prompt's.

Writes `v0.30.0/resume/trace.jsonl` and `v0.30.0/resume/cases.json`.
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

from record import BLOCK_SIZE, EOS_TOKEN_ID, TOOLS, Server, block_hashes

FRESH_COPIES = 16
CONTINUATION_COPIES = 4

CASES: list[dict] = [
    {
        "name": "chat",
        "endpoint": "/v1/chat/completions",
        "request": {
            "messages": [
                {"role": "system", "content": "Resume case chat. Answer in two sentences."},
                {"role": "user", "content": "Tell me about Paris."},
            ],
            "max_tokens": 128,
            "temperature": 0,
        },
        "output": (
            "The user wants a short description of Paris, so two plain facts will do.</think>"
            "Paris is the capital of France and sits on the Seine. "
            "It is known for the Eiffel Tower and the Louvre."
        ),
        "cut": 24,
    },
    {
        "name": "completion",
        "endpoint": "/v1/completions",
        "request": {
            "prompt": (
                "Resume case completion. Finish this sentence about optics with one clause. "
                "The three primary colors of light are"
            ),
            "max_tokens": 64,
            "temperature": 0,
        },
        "output": " red, green and blue, and mixing all three at full strength gives white light.",
        "cut": 17,
    },
    {
        "name": "tool",
        "endpoint": "/v1/chat/completions",
        "request": {
            "messages": [
                {"role": "system", "content": "Resume case tool. Use tools for live data."},
                {"role": "user", "content": "What is the weather in Paris right now?"},
            ],
            "tools": [TOOLS[0]],
            "max_tokens": 128,
            "temperature": 0,
        },
        "output": (
            "The user wants live weather, which needs the weather tool for Paris.</think>\n"
            "<tool_call>get_weather<arg_key>city</arg_key><arg_value>Paris</arg_value></tool_call>"
        ),
        "cut": 20,
    },
]


def record(prompt: list[int], output: list[int]) -> dict:
    return {
        "prompt_tokens": len(prompt),
        "output_tokens": len(output),
        "ttft_ms": 20.0,
        "itl_ms": [5.0] * max(len(output) - 1, 0),
        "concurrency": 1,
        "arrival_ms": 0.0,
        "block_hashes": block_hashes(prompt),
        "output_token_ids": output,
        "finish_reason": "stop",
    }


def main() -> None:
    here = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--vcr-bin", default=os.environ.get("VCR_BIN"))
    ap.add_argument("--vllm-bin", default=os.environ.get("VLLM_BIN", shutil.which("vllm")))
    ap.add_argument("--model", default=os.environ.get("MODEL", "zai-org/GLM-4.7"))
    ap.add_argument("--out", default=str(here / "v0.30.0" / "resume"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8100")))
    ap.add_argument(
        "--handshake-port", type=int, default=int(os.environ.get("HANDSHAKE_PORT", "29551"))
    )
    args = ap.parse_args()
    if not args.vcr_bin or not args.vllm_bin:
        ap.error("--vcr-bin / VCR_BIN and --vllm-bin / VLLM_BIN are required")

    work = Path(tempfile.mkdtemp(prefix="vllm-resume-trace-"))
    print(f"work dir: {work}", file=sys.stderr)
    server = Server(args, work)
    server.start("probe", ["--enable-scale-out"], None)
    records: list[dict] = []
    cases: list[dict] = []
    try:
        for case in CASES:
            request = dict(copy.deepcopy(case["request"]), model=args.model)
            rendered = server.json_post(case["endpoint"] + "/render", request)
            prompt = (rendered[0] if isinstance(rendered, list) else rendered)["token_ids"]
            tokens = server.json_post(
                "/tokenize",
                {"model": args.model, "prompt": case["output"], "add_special_tokens": False},
            )["tokens"]
            output = list(tokens) + [EOS_TOKEN_ID]
            cut = case["cut"]
            if len(block_hashes(prompt + output[:cut])) <= len(block_hashes(prompt)):
                raise SystemExit(f"{case['name']}: the cut does not reach a new block")
            if cut >= len(output):
                raise SystemExit(f"{case['name']}: the cut is past the output")
            records += [record(prompt, output)] * FRESH_COPIES
            records += [record(prompt + output[:cut], output[cut:])] * CONTINUATION_COPIES
            cases.append(
                {
                    "name": case["name"],
                    "endpoint": case["endpoint"],
                    "request": request,
                    "cut": cut,
                    "prompt_token_ids": prompt,
                    "output_token_ids": output,
                }
            )
            print(f"{case['name']}: {len(prompt)} prompt tokens, {len(output)} output, cut at {cut}")
    finally:
        server.stop()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    meta = {"meta": {"model": args.model, "block_size": BLOCK_SIZE, "source": "scripted"}}
    lines = [json.dumps(meta)] + [json.dumps(r) for r in records]
    (out / "trace.jsonl").write_text("\n".join(lines) + "\n")
    (out / "cases.json").write_text(json.dumps(cases, indent=2, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
