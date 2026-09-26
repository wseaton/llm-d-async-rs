#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Compares one greedy completion sent straight to vLLM with the same request
sent through the processor, whose upstream breaks streams mid-generation."""

import argparse
import json
import sys
import time
import urllib.request

PROMPT = (
    "Write a detailed, multi-section essay on the history of the printing press, "
    "from Gutenberg to the digital age. Include dates, people and consequences."
)


def post(url: str, body: dict | None, timeout: float = 600) -> tuple[int, dict | None]:
    data = json.dumps(body).encode() if body is not None else b""
    req = urllib.request.Request(url, data=data, method="POST", headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
        return r.status, (json.loads(raw) if raw else None)


def metric(base: str, name: str) -> float:
    with urllib.request.urlopen(f"{base}/metrics") as r:
        for line in r.read().decode().splitlines():
            if line.startswith(name + "{"):
                return float(line.rsplit(" ", 1)[1])
    return 0.0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--vllm", default="http://localhost:18000")
    ap.add_argument("--processor", default="http://localhost:18080")
    ap.add_argument("--metrics", default="http://localhost:19090")
    ap.add_argument("--model", default="Qwen/Qwen2.5-1.5B-Instruct")
    ap.add_argument("--max-tokens", type=int, default=600)
    ap.add_argument("--id", default=f"poc-{int(time.time())}")
    ap.add_argument("--skip-baseline", action="store_true")
    ap.add_argument("--submit-only", action="store_true")
    ap.add_argument("--ignore-eos", action="store_true")
    ap.add_argument("--chat", action="store_true", help="render PROMPT as a chat turn into token IDs")
    ap.add_argument("--api", choices=["completions", "chat"], default="completions")
    args = ap.parse_args()

    endpoint = "/v1/completions"
    prompt: str | list[int] = PROMPT
    if args.api == "chat":
        endpoint = "/v1/chat/completions"
    elif args.chat:
        _, rendered = post(
            f"{args.vllm}/tokenize",
            {"model": args.model, "messages": [{"role": "user", "content": PROMPT}], "add_generation_prompt": True},
        )
        prompt = rendered["tokens"]
    payload = {
        "model": args.model,
        ("messages" if args.api == "chat" else "prompt"): (
            [{"role": "user", "content": PROMPT}] if args.api == "chat" else prompt
        ),
        "max_tokens": args.max_tokens,
        "temperature": 0,
        "return_token_ids": True,
    }
    if args.ignore_eos:
        payload["ignore_eos"] = True

    if not args.skip_baseline:
        _, direct = post(f"{args.vllm}{endpoint}", payload)
        json.dump(direct, open("baseline.json", "w"), indent=1)
    direct = json.load(open("baseline.json"))

    before = metric(args.metrics, "llm_d_async_async_request_resumes_total")
    status, submitted = post(
        f"{args.processor}/v1/requests",
        {"id": args.id, "created": int(time.time()), "deadline": int(time.time()) + 900,
         "payload": payload, "endpoint": endpoint},
    )
    print(f"submitted {args.id}: {status} {submitted}")
    if args.submit_only:
        return 0

    while True:
        try:
            status, claim = post(f"{args.processor}/v1/results/result-list/claims?wait_ms=20000&lease_ms=60000", None)
        except OSError as e:
            print(f"processor unreachable ({e}), retrying")
            time.sleep(2)
            continue
        if status == 200 and claim:
            post(f"{args.processor}/v1/results/result-list/claims/{claim['claim_id']}/ack", {"owner_token": claim["owner_token"]})
            if claim["result"]["id"] == args.id:
                break
    result = claim["result"]
    if result.get("status_code") != 200:
        print("request failed:", json.dumps(result, indent=1))
        return 1
    resumed = json.loads(result["payload"])
    json.dump(resumed, open(f"{args.id}.json", "w"), indent=1)

    d, r = direct["choices"][0], resumed["choices"][0]
    try:
        print(f"resumes counted by this processor: {metric(args.metrics, 'llm_d_async_async_request_resumes_total') - before:.0f}")
    except OSError:
        pass
    print(f"resumed tokens (all requests): {metric(args.metrics, 'llm_d_async_async_resumed_tokens_total'):.0f}")
    print(f"direct : {len(d['token_ids'])} tokens, finish={d['finish_reason']}, usage={direct['usage']}")
    print(f"resumed: {len(r['token_ids'])} tokens, finish={r['finish_reason']}, usage={resumed['usage']}")
    if args.api == "chat":
        prompts = (direct["prompt_token_ids"], resumed["prompt_token_ids"])
        texts = {f: (d["message"].get(f), r["message"].get(f)) for f in ("reasoning", "content", "tool_calls")}
    else:
        prompts = (d["prompt_token_ids"], r["prompt_token_ids"])
        texts = {"text": (d["text"], r["text"])}
    print(f"prompt_token_ids equal: {prompts[0] == prompts[1]}")
    same_tokens = d["token_ids"] == r["token_ids"]
    print(f"token_ids equal: {same_tokens}")
    same_text = True
    for name, (a, b) in texts.items():
        print(f"{name} equal: {a == b}")
        same_text &= a == b
    print(f"finish_reason equal: {d['finish_reason'] == r['finish_reason']}")
    gaps = {"id", "created", "system_fingerprint", "metrics", "usage"}
    other = sorted(k for k in set(direct) | set(resumed) if k not in gaps and k != "choices" and direct.get(k) != resumed.get(k))
    print(f"other differing top-level fields: {other}")
    usage_diff = {k: (direct["usage"].get(k), resumed["usage"].get(k)) for k in set(direct["usage"]) | set(resumed["usage"]) if direct["usage"].get(k) != resumed["usage"].get(k)}
    print(f"usage differences: {usage_diff}")
    if args.chat or args.api == "chat":
        _, marker = post(
            f"{args.vllm}/tokenize",
            {"model": args.model, "prompt": "</think>", "add_special_tokens": False},
        )
        [think_end] = marker["tokens"]
        at = r["token_ids"].index(think_end) if think_end in r["token_ids"] else None
        print(f"</think> is output token {at} of {len(r['token_ids'])}")
    if not same_tokens:
        i = next((i for i, (a, b) in enumerate(zip(d["token_ids"], r["token_ids"])) if a != b), None)
        print(f"first differing token index: {i}")
    return 0 if same_tokens and same_text else 2


if __name__ == "__main__":
    sys.exit(main())
