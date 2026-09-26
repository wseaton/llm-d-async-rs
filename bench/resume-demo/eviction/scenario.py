#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Flow-control eviction scenario: long greedy batch completions go through the
processor (objective `batch`, priority -1) while bursts of interactive requests
(objective `interactive`) hit the gateway directly and evict them. Compares every
batch output against an uninterrupted direct run and reports how much decode
work vLLM did for the batch.

Batch requests can be split into jobs with their own deadlines
(`--job NAME:COUNT:DEADLINE_S`, seconds after submit). With `--tiered`, jobs
are ranked by deadline and the looser ones name lower-priority objectives, so
eviction takes them first."""

import argparse
import json
import random
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

DEFAULT_MODEL = "Qwen/Qwen2.5-1.5B-Instruct"
WORDS = (
    "archive ledger harbor merchant river council charter guild census market bridge tariff canal "
    "treaty mill quarry granary festival scribe courier beacon orchard vessel foundry monastery "
    "caravan observatory aqueduct parish estate warehouse lighthouse printing inventory bishop "
    "cartographer surveyor apprentice magistrate ferry toll almanac pilgrimage garrison seal "
    "weaver tanner mason clerk steward navigator dock rope timber salt wool grain copper silver "
    "ledger entry recorded season winter spring summer autumn north south east west delivered "
    "shipment disputed repaired flooded expanded taxed granted petitioned surveyed rebuilt"
).split()
TOPICS = [
    "the printing press", "the steam engine", "the telegraph", "the transistor",
    "the Roman aqueducts", "double-entry bookkeeping", "the Silk Road", "vaccination",
    "the Hanseatic League", "the Panama Canal", "radio astronomy", "the Green Revolution",
    "container shipping", "the Rosetta Stone", "the Apollo guidance computer",
    "the Dutch tulip mania", "the Gutenberg Bible", "public libraries", "the metric system",
    "undersea telegraph cables", "the Jacquard loom", "the Suez Canal", "penicillin",
    "the Library of Alexandria",
]
TIERS = ["batch", "batch-2", "batch-3"]


@dataclass
class Job:
    name: str
    count: int
    deadline_s: int
    objective: str | None = None


def parse_job(spec: str) -> Job:
    name, count, deadline = spec.split(":")
    return Job(name, int(count), int(deadline))


def post(url: str, body: dict | list | None, headers: dict | None = None, timeout: float = 900) -> tuple[int, dict | None]:
    data = json.dumps(body).encode() if body is not None else b""
    req = urllib.request.Request(url, data=data, method="POST",
                                 headers={"Content-Type": "application/json", **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        return e.code, {"error": e.read().decode(errors="replace"), "headers": dict(e.headers)}


def scrape(base: str) -> dict[str, float]:
    """Sums every sample of each metric family across its labels."""
    out: dict[str, float] = {}
    with urllib.request.urlopen(f"{base}/metrics", timeout=10) as r:
        for line in r.read().decode().splitlines():
            if not line or line.startswith("#"):
                continue
            name_labels, _, value = line.rpartition(" ")
            name = name_labels.split("{", 1)[0]
            try:
                out[name] = out.get(name, 0.0) + float(value)
            except ValueError:
                pass
            if "{" in name_labels:
                out[name_labels] = float(value)
    return out


def scrape_pods(context: str, namespace: str, selector: str) -> dict[str, dict[str, float]]:
    """Scrapes every vLLM pod through the API server proxy, since a
    port-forward to the Service reaches only one pod."""
    pods = subprocess.run(
        ["kubectl", "--context", context, "-n", namespace, "get", "pods", "-l", selector,
         "-o", "jsonpath={.items[*].metadata.name}"],
        check=True, capture_output=True, text=True,
    ).stdout.split()
    out: dict[str, dict[str, float]] = {}
    for pod in pods:
        raw = subprocess.run(
            ["kubectl", "--context", context, "get", "--raw",
             f"/api/v1/namespaces/{namespace}/pods/{pod}:8000/proxy/metrics"],
            check=True, capture_output=True, text=True,
        ).stdout
        metrics: dict[str, float] = {}
        for line in raw.splitlines():
            if line and not line.startswith("#"):
                name_labels, _, value = line.rpartition(" ")
                name = name_labels.split("{", 1)[0]
                try:
                    metrics[name] = metrics.get(name, 0.0) + float(value)
                except ValueError:
                    pass
        out[pod] = metrics
    return out


def scrape_dns(name: str, port: int = 8000) -> dict[str, dict[str, float]]:
    """Scrapes every address a headless Service name resolves to, for runs
    inside the cluster."""
    ips = sorted({info[4][0] for info in socket.getaddrinfo(name, port, proto=socket.IPPROTO_TCP)})
    return {ip: scrape(f"http://{ip}:{port}") for ip in ips}


def total(pods: dict[str, dict[str, float]]) -> dict[str, float]:
    out: dict[str, float] = {}
    for metrics in pods.values():
        for name, value in metrics.items():
            out[name] = out.get(name, 0.0) + value
    return out


def delta(after: dict[str, float], before: dict[str, float], name: str) -> float:
    return after.get(name, 0.0) - before.get(name, 0.0)


@dataclass
class Workload:
    model: str = DEFAULT_MODEL
    context_tokens: int = 0
    interactive_context_tokens: int = 0

    def key(self, n: int, max_tokens: int) -> str:
        if self.model == DEFAULT_MODEL and not self.context_tokens:
            return f"{n}x{max_tokens}"
        return f"{self.model}:{self.context_tokens}ctx:{n}x{max_tokens}"


def context(tag: str, seed: int, words: int) -> str:
    """A distinct pseudo-document of about `words` tokens that starts with
    its own tag, so no two requests share a prefix."""
    if not words:
        return ""
    rng = random.Random(f"{tag}-{seed}")
    body = " ".join(rng.choice(WORDS) for _ in range(words))
    return f"{tag} {seed:06d}. {body}\n\n"


def batch_payload(i: int, max_tokens: int, work: Workload = Workload()) -> dict:
    prompt = (f"Write a detailed, multi-section essay on the history of {TOPICS[i % len(TOPICS)]}. "
              "Include dates, people and consequences.")
    if i >= len(TOPICS):
        prompt += f" Focus on angle number {i // len(TOPICS) + 1}."
    if work.context_tokens:
        prompt = context("Record", i, work.context_tokens) + "Using the record above as background. " + prompt
    return {
        "model": work.model,
        "prompt": prompt,
        "max_tokens": max_tokens,
        "temperature": 0,
        "ignore_eos": True,
        "return_token_ids": True,
    }


def baselines(vllm: str, n: int, max_tokens: int, path: Path, concurrency: int, work: Workload) -> list[dict]:
    key = work.key(n, max_tokens)
    cached = json.loads(path.read_text()) if path.exists() else {}
    if key in cached:
        return cached[key]
    print(f"baseline: {n} direct requests to vLLM")
    with ThreadPoolExecutor(concurrency) as pool:
        results = list(pool.map(lambda i: post(f"{vllm}/v1/completions", batch_payload(i, max_tokens, work)), range(n)))
    bad = [r for r in results if r[0] != 200 or r[1] is None]
    if bad:
        raise SystemExit(f"baseline failed: {bad[0]}")
    cached[key] = [{"token_ids": b["choices"][0]["token_ids"], "text": b["choices"][0]["text"]}
                   for _, b in results if b is not None]
    path.write_text(json.dumps(cached))
    return cached[key]


def interactive(gateway: str, i: int, max_tokens: int, log: list, lock: threading.Lock,
                salt: str | None = None, work: Workload = Workload()) -> None:
    body: dict = {
        "model": work.model,
        "prompt": context("Conversation", i, work.interactive_context_tokens)
                  + f"In two short paragraphs, explain why {TOPICS[(i * 7) % len(TOPICS)]} mattered.",
        "max_tokens": max_tokens,
        "temperature": 0,
        "stream": True,
        "stream_options": {"include_usage": True},
    }
    if salt:
        body["cache_salt"] = salt
    req = urllib.request.Request(
        f"{gateway}/v1/completions", data=json.dumps(body).encode(), method="POST",
        headers={"Content-Type": "application/json", "x-llm-d-inference-objective": "interactive"},
    )
    start = time.monotonic()
    ttft = None
    tokens = 0
    status = 0
    try:
        with urllib.request.urlopen(req, timeout=600) as r:
            status = r.status
            for raw in r:
                line = raw.decode().strip()
                if not line.startswith("data: ") or line == "data: [DONE]":
                    continue
                event = json.loads(line[6:])
                if event.get("choices") and ttft is None:
                    ttft = time.monotonic() - start
                if event.get("usage"):
                    tokens = event["usage"]["completion_tokens"]
    except urllib.error.HTTPError as e:
        status = e.code
    except OSError as e:
        status = -1
        print(f"interactive {i}: {e}")
    with lock:
        log.append({"i": i, "status": status, "ttft": ttft, "total": time.monotonic() - start, "tokens": tokens})


def claim_all(processor: str, ids: set[str], t0: float, claimers: int = 32) -> tuple[dict[str, dict], int]:
    """Claims results with parallel leases so claim latency does not delay the
    recorded completion times. Returns the results and the deepest result
    backlog seen, which should stay near zero."""
    done: dict[str, dict] = {}
    lock = threading.Lock()
    backlog = 0

    def claimer() -> None:
        nonlocal backlog
        while True:
            with lock:
                if not ids - done.keys():
                    return
            try:
                status, claim = post(f"{processor}/v1/results/result-list/claims?wait_ms=2000&lease_ms=60000", None)
                if status != 200 or not claim:
                    continue
                at = time.monotonic() - t0
                post(f"{processor}/v1/results/result-list/claims/{claim['claim_id']}/ack",
                     {"owner_token": claim["owner_token"]})
                rid = claim["result"]["id"]
                with urllib.request.urlopen(f"{processor}/v1/results/result-list/depth", timeout=10) as r:
                    depth = int(json.loads(r.read())["depth"])
            except OSError:
                time.sleep(1)
                continue
            with lock:
                backlog = max(backlog, depth)
                if rid in ids:
                    done[rid] = {"result": claim["result"], "at": at}

    threads = [threading.Thread(target=claimer) for _ in range(claimers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return done, backlog


def pct(xs: list[float], p: float) -> float:
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p * len(xs)))] if xs else float("nan")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", required=True, help="run name, e.g. resumable or restart")
    ap.add_argument("--vllm", default="http://localhost:18000", help="where baselines are computed")
    ap.add_argument("--baseline-concurrency", type=int, default=32)
    ap.add_argument("--kube-context", default="coreweave-waldorf")
    ap.add_argument("--namespace", default="weaton-dev")
    ap.add_argument("--vllm-selector", default="app=evict-vllm")
    ap.add_argument("--vllm-pods-dns", help="headless Service name to scrape each replica by IP, instead of kubectl")
    ap.add_argument("--gateway", default="http://localhost:18081")
    ap.add_argument("--processor", default="http://localhost:18080")
    ap.add_argument("--processor-metrics", default="http://localhost:19090")
    ap.add_argument("--epp-metrics", default="http://localhost:19091")
    ap.add_argument("--batch", type=int, default=20, help="request count when no --job is given")
    ap.add_argument("--job", action="append", type=parse_job, default=[],
                    help="NAME:COUNT:DEADLINE_S, repeatable; deadline is seconds after submit")
    ap.add_argument("--session-id", action="store_true",
                    help="send x-session-id set to the request id, for session-affinity routing")
    ap.add_argument("--cache-salt", action="store_true",
                    help="salt this run's requests so vLLM prefix caches start cold for every run")
    ap.add_argument("--tiered", action="store_true",
                    help="looser-deadline jobs name lower-priority objectives (batch, batch-2, batch-3)")
    ap.add_argument("--max-tokens", type=int, default=2000)
    ap.add_argument("--warmup", type=float, default=8.0, help="seconds between batch submit and first burst")
    ap.add_argument("--bursts", type=int, default=3)
    ap.add_argument("--burst-size", type=int, default=6)
    ap.add_argument("--burst-gap", type=float, default=15.0)
    ap.add_argument("--interactive-max-tokens", type=int, default=256)
    ap.add_argument("--out", default="runs")
    ap.add_argument("--model", default=DEFAULT_MODEL)
    ap.add_argument("--context-tokens", type=int, default=0,
                    help="prepend a distinct pseudo-document of about this many tokens to each batch prompt")
    ap.add_argument("--interactive-context-tokens", type=int, default=0,
                    help="the same for interactive prompts")
    args = ap.parse_args()

    work = Workload(args.model, args.context_tokens, args.interactive_context_tokens)
    jobs: list[Job] = args.job or [Job("batch", args.batch, 1800)]
    if args.tiered:
        for rank, job in enumerate(sorted(jobs, key=lambda j: j.deadline_s)):
            job.objective = TIERS[min(rank, len(TIERS) - 1)]
    total_requests = sum(j.count for j in jobs)

    out = Path(args.out)
    out.mkdir(exist_ok=True)
    expected = baselines(args.vllm, total_requests, args.max_tokens, out / "baselines.json", args.baseline_concurrency, work)

    def vllm_pods() -> dict[str, dict[str, float]]:
        if args.vllm_pods_dns:
            return scrape_dns(args.vllm_pods_dns)
        return scrape_pods(args.kube_context, args.namespace, args.vllm_selector)

    pods_before = vllm_pods()
    before = {"vllm": total(pods_before), "proc": scrape(args.processor_metrics), "epp": scrape(args.epp_metrics)}
    run = f"{args.label}-{int(time.time())}"
    owner: list[Job] = [job for job in jobs for _ in range(job.count)]
    ids = [f"{run}-{owner[i].name}-{i}" for i in range(total_requests)]
    now = int(time.time())
    requests = []
    for i, rid in enumerate(ids):
        request = {"id": rid, "created": now, "deadline": now + owner[i].deadline_s,
                   "endpoint": "/v1/completions", "payload": batch_payload(i, args.max_tokens, work)}
        if args.cache_salt:
            request["payload"]["cache_salt"] = run
        headers = {}
        if owner[i].objective:
            headers["x-llm-d-inference-objective"] = owner[i].objective
        if args.session_id:
            headers["x-session-id"] = rid
        if headers:
            request["headers"] = headers
        requests.append(request)
    t0 = time.monotonic()
    status, resp = post(f"{args.processor}/v1/requests/batch", requests)
    if status >= 300:
        print("submit failed:", status, resp)
        return 1
    submit_s = time.monotonic() - t0
    print(f"{run}: submitted {total_requests} batch requests x {args.max_tokens} tokens: "
          + ", ".join(f"{j.name} {j.count} due {j.deadline_s}s ({j.objective or 'queue objective'})" for j in jobs))

    log: list[dict] = []
    lock = threading.Lock()

    def bursts() -> None:
        time.sleep(args.warmup)
        threads: list[threading.Thread] = []
        for b in range(args.bursts):
            print(f"  [{time.monotonic() - t0:6.1f}s] interactive burst {b + 1}/{args.bursts} x {args.burst_size}")
            burst = [threading.Thread(target=interactive, args=(args.gateway, b * args.burst_size + j,
                                                                 args.interactive_max_tokens, log, lock,
                                                                 run if args.cache_salt else None, work))
                     for j in range(args.burst_size)]
            for t in burst:
                t.start()
            threads += burst
            if b + 1 < args.bursts:
                time.sleep(args.burst_gap)
        for t in threads:
            t.join()

    burster = threading.Thread(target=bursts)
    burster.start()
    done, backlog = claim_all(args.processor, set(ids), t0)
    burster.join()
    makespan = max((d["at"] for d in done.values()), default=0.0)
    pods_after = vllm_pods()
    after = {"vllm": total(pods_after), "proc": scrape(args.processor_metrics), "epp": scrape(args.epp_metrics)}

    identical = failed = 0
    mismatches = []
    per_job = {j.name: {"requests": j.count, "deadline_s": j.deadline_s, "objective": j.objective or "batch",
                        "identical": 0, "failed": {}, "done_s": []} for j in jobs}
    for i, rid in enumerate(ids):
        res = done[rid]["result"]
        job = per_job[owner[i].name]
        if res.get("status_code") != 200:
            failed += 1
            code = str(res.get("status_code"))
            job["failed"][code] = job["failed"].get(code, 0) + 1
            mismatches.append({"id": rid, "status": res.get("status_code"), "payload": str(res.get("payload"))[:300]})
            continue
        job["done_s"].append(done[rid]["at"])
        choice = json.loads(res["payload"])["choices"][0]
        if choice["token_ids"] == expected[i]["token_ids"] and choice["text"] == expected[i]["text"]:
            identical += 1
            job["identical"] += 1
        else:
            first = next((k for k, (a, b) in enumerate(zip(choice["token_ids"], expected[i]["token_ids"])) if a != b), None)
            mismatches.append({"id": rid, "first_diff": first, "len": len(choice["token_ids"])})
    for job in per_job.values():
        times = job.pop("done_s")
        job["met_deadline"] = sum(1 for t in times if t <= job["deadline_s"])
        job["p50_done_s"] = round(pct(times, 0.5), 1)
        job["last_done_s"] = round(max(times), 1) if times else None

    ok = [r for r in log if r["status"] == 200]
    interactive_tokens = sum(r["tokens"] for r in ok)
    gen = delta(after["vllm"], before["vllm"], "vllm:generation_tokens_total")
    ideal = total_requests * args.max_tokens
    batch_decoded = gen - interactive_tokens
    report = {
        "run": run,
        "batch": {"requests": total_requests, "max_tokens": args.max_tokens, "identical": identical, "failed": failed,
                  "makespan_s": round(makespan, 1),
                  "p50_done_s": round(pct([d["at"] for d in done.values()], 0.5), 1),
                  "max_result_backlog": backlog,
                  "submit_s": round(submit_s, 3),
                  "mismatches": mismatches},
        "jobs": per_job,
        "interactive": {"sent": len(log), "ok": len(ok),
                        "statuses": sorted({r["status"] for r in log}),
                        "ttft_p50_s": round(pct([r["ttft"] for r in ok if r["ttft"]], 0.5), 3),
                        "ttft_p95_s": round(pct([r["ttft"] for r in ok if r["ttft"]], 0.95), 3),
                        "tokens": interactive_tokens},
        "decode": {"vllm_generation_tokens": gen, "batch_decoded": batch_decoded, "batch_ideal": ideal,
                   "batch_redone": batch_decoded - ideal,
                   "vllm_prompt_tokens": delta(after["vllm"], before["vllm"], "vllm:prompt_tokens_total"),
                   "prefix_cache_hits": delta(after["vllm"], before["vllm"], "vllm:prefix_cache_hits_total"),
                   "prefix_cache_queries": delta(after["vllm"], before["vllm"], "vllm:prefix_cache_queries_total")},
        "pods": {pod: {name.removeprefix("vllm:").removesuffix("_total"): delta(pods_after[pod], pods_before.get(pod, {}), name)
                       for name in ("vllm:generation_tokens_total", "vllm:prompt_tokens_total",
                                    "vllm:prefix_cache_hits_total", "vllm:prefix_cache_queries_total")}
                 for pod in pods_after},
        "processor": {k.removeprefix("llm_d_async_async_"): delta(after["proc"], before["proc"], k) for k in (
            "llm_d_async_async_request_resumes_total", "llm_d_async_async_resumed_tokens_total",
            "llm_d_async_async_request_retries_total")},
        "epp": {k.removeprefix("llm_d_epp_flow_control_"): delta(after["epp"], before["epp"], k) for k in (
            "llm_d_epp_flow_control_revocations_issued_total", "llm_d_epp_flow_control_revocations_total")},
        "epp_outcomes": {k: delta(after["epp"], before["epp"], k) for k in after["epp"]
                         if k.startswith("llm_d_epp_flow_control_requests_total{") and delta(after["epp"], before["epp"], k)},
    }
    (out / f"{run}.json").write_text(json.dumps(report, indent=1))
    print(json.dumps(report, indent=1))
    return 0 if identical == total_requests else 2


if __name__ == "__main__":
    sys.exit(main())
