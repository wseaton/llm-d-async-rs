"""Renders and derenders every recorded chat case and diffs the result with
the recorded non-streamed response."""

import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

BASE = "http://127.0.0.1:8110"
CORPUS = Path("/Users/weaton/git/llm-d-async-rs/tests/fixtures/vllm/v0.30.0") / sys.argv[1]


def post(path: str, body: dict) -> tuple[int, dict]:
    req = urllib.request.Request(BASE + path, json.dumps(body).encode(), {"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


def stream_tokens(sse: str) -> tuple[list[int], list[int], str | None]:
    prompt, out, finish = [], [], None
    for line in sse.splitlines():
        if not line.startswith("data: ") or line == "data: [DONE]":
            continue
        chunk = json.loads(line[6:])
        if "error" in chunk:
            continue
        prompt = chunk.get("prompt_token_ids") or prompt
        for c in chunk.get("choices", []):
            out += c.get("token_ids") or []
            finish = c.get("finish_reason") or finish
    return prompt, out, finish


def diff(a: object, b: object, path: str = "") -> list[str]:
    if isinstance(a, dict) and isinstance(b, dict):
        return [d for k in sorted(set(a) | set(b)) for d in diff(a.get(k, "<absent>"), b.get(k, "<absent>"), f"{path}.{k}")]
    if isinstance(a, list) and isinstance(b, list) and len(a) == len(b):
        return [d for i, (x, y) in enumerate(zip(a, b)) for d in diff(x, y, f"{path}[{i}]")]
    return [] if a == b else [f"{path}: derender={json.dumps(a)[:90]} direct={json.dumps(b)[:90]}"]


for case in sorted(CORPUS.iterdir()):
    meta = json.loads((case / "meta.json").read_text())
    if meta["endpoint"] != "/v1/chat/completions" or meta["response_status"] != 200:
        continue
    request = json.loads((case / "request.json").read_text())
    direct = json.loads((case / "response.json").read_text())
    prompt, out, finish = stream_tokens((case / "stream.sse").read_text())

    status, rendered = post("/v1/chat/completions/render", request)
    render_ok = status == 200 and rendered.get("token_ids") == prompt

    body = {
        "generate_response": {
            "request_id": "derender-" + case.name,
            "choices": [{"index": 0, "token_ids": out, "finish_reason": finish}],
            "prompt_token_ids": prompt,
        },
        "prompt_tokens": len(prompt),
        "chat_request": request,
    }
    status, derendered = post("/v1/chat/completions/derender", body)
    if status != 200:
        print(f"{case.name}: derender {status} {derendered}")
        continue
    got, want = derendered["choices"][0], direct["choices"][0]
    for call in got["message"].get("tool_calls", []) + want["message"].get("tool_calls", []):
        call["id"] = "<id>"
    message = diff(got["message"], want["message"], "message")
    other = diff(
        {k: v for k, v in got.items() if k != "message"} | {"usage": derendered["usage"]},
        {k: v for k, v in want.items() if k != "message"} | {"usage": direct["usage"]},
    )
    print(f"{case.name}: render {'ok' if render_ok else 'MISMATCH'}, message {'identical' if not message else 'DIFFERS'}")
    for d in message + other:
        print(f"    {d}")
