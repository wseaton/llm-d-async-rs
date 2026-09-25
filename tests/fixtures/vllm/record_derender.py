#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Adds vLLM's render and derender responses to every recorded chat case.

Needs a vLLM serving the corpus model with the corpus flags plus
`--enable-scale-out` (any engine; vllm-vcr's random tokens do). For each
case it writes `rendered.json`, `/v1/chat/completions/render` of the case's
request, and `derendered.json`, `/v1/chat/completions/derender` of the
case's recorded prompt and output token IDs.
"""

import argparse
import json
import sys
import urllib.request
from pathlib import Path


def post(base: str, path: str, body: object) -> bytes:
    req = urllib.request.Request(
        base + path, json.dumps(body).encode(), {"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req) as r:
        return r.read()


def recorded_tokens(sse: str) -> tuple[list[int], list[int], str]:
    prompt: list[int] = []
    output: list[int] = []
    finish = ""
    for line in sse.splitlines():
        if not line.startswith("data: ") or line == "data: [DONE]":
            continue
        chunk = json.loads(line[6:])
        prompt = chunk.get("prompt_token_ids") or prompt
        for choice in chunk.get("choices", []):
            output += choice.get("token_ids") or []
            finish = choice.get("finish_reason") or finish
    return prompt, output, finish


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://127.0.0.1:8110")
    ap.add_argument("corpus", nargs="+", type=Path, help="config directories, e.g. v0.30.0/default")
    args = ap.parse_args()
    for config in args.corpus:
        for case in sorted(config.iterdir()):
            meta = json.loads((case / "meta.json").read_text())
            if meta["endpoint"] != "/v1/chat/completions" or meta["response_status"] != 200:
                continue
            request = json.loads((case / "request.json").read_text())
            prompt, output, finish = recorded_tokens((case / "stream.sse").read_text())
            rendered = post(args.url, "/v1/chat/completions/render", request)
            derendered = post(
                args.url,
                "/v1/chat/completions/derender",
                {
                    "generate_response": {
                        "request_id": f"chatcmpl-{case.name}",
                        "choices": [{"index": 0, "token_ids": output, "finish_reason": finish}],
                        "prompt_token_ids": prompt,
                    },
                    "prompt_tokens": len(prompt),
                    "chat_request": request,
                },
            )
            (case / "rendered.json").write_bytes(rendered)
            (case / "derendered.json").write_bytes(derendered)
            print(f"{case}: rendered {len(json.loads(rendered)['token_ids'])} prompt tokens")
    return 0


if __name__ == "__main__":
    sys.exit(main())
