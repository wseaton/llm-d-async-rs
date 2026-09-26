#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Tabulates <dir>/*.json (default runs) by arm: mean and range per metric,
then per-job deadline results for runs that split the batch into jobs.
Decode redone only counts runs where every batch request completed, since
the ideal it is measured against assumes they all did."""

import json
import sys
from collections import defaultdict
from pathlib import Path

ARMS = ["floor", "noevict", "holdback-50", "holdback-75", "restart", "resumable", "tiered",
        "affinity-none", "affinity-session", "affinity-precise"]
COLUMNS = [
    ("TTFT p50 s", lambda r: r["interactive"]["ttft_p50_s"]),
    ("TTFT p95 s", lambda r: r["interactive"]["ttft_p95_s"]),
    ("batch makespan s", lambda r: r["batch"]["makespan_s"]),
    ("batch p50 s", lambda r: r["batch"]["p50_done_s"]),
    ("decode redone", lambda r: r["decode"]["batch_redone"] if not r["batch"]["failed"] else None),
    ("evictions", lambda r: r["epp"]["revocations_issued_total"]),
    ("prefill computed", lambda r: r["decode"]["vllm_prompt_tokens"] - r["decode"]["prefix_cache_hits"]),
    ("prefix hit %", lambda r: 100 * r["decode"]["prefix_cache_hits"] / max(1, r["decode"]["prefix_cache_queries"])),
    ("identical", lambda r: f"{r['batch']['identical']}/{r['batch']['requests']}"),
]

runs = defaultdict(list)
for path in sorted(Path(sys.argv[1] if len(sys.argv) > 1 else "runs").glob("*-*.json")):
    report = json.loads(path.read_text())
    arm = report["run"].rsplit("-", 1)[0]
    if arm in ARMS:
        runs[arm].append(report)


def cell(values: list) -> str:
    values = [v for v in values if v is not None]
    if not values:
        return "n/a"
    if isinstance(values[0], str):
        return ", ".join(sorted(set(values)))
    if len(values) == 1:
        return f"{values[0]:g}"
    mean = sum(values) / len(values)
    return f"{mean:.3g} ({min(values):g}-{max(values):g})"


print("| arm | n | " + " | ".join(name for name, _ in COLUMNS) + " |")
print("|---|---|" + "---|" * len(COLUMNS))
for arm in ARMS:
    if runs[arm]:
        cells = [cell([f(r) for r in runs[arm]]) if arm != "floor" or i < 2 else "" for i, (_, f) in enumerate(COLUMNS)]
        print(f"| {arm} | {len(runs[arm])} | " + " | ".join(cells) + " |")

job_rows = [(arm, name) for arm in ARMS for name in dict.fromkeys(
    name for r in runs[arm] for name in r.get("jobs", {}) if len(r.get("jobs", {})) > 1)]
if job_rows:
    print()
    print("| arm | job | deadline s | met deadline | p50 done s | last done s | failed |")
    print("|---|---|---|---|---|---|---|")
    for arm, name in job_rows:
        jobs = [r["jobs"][name] for r in runs[arm] if name in r.get("jobs", {})]
        met = cell([j["met_deadline"] / j["requests"] * 100 for j in jobs])
        failed: dict[str, int] = defaultdict(int)
        for j in jobs:
            for code, n in j["failed"].items():
                failed[code] += n
        print(f"| {arm} | {name} | {jobs[0]['deadline_s']} | {met}% | "
              f"{cell([j['p50_done_s'] for j in jobs])} | {cell([j['last_done_s'] or 0 for j in jobs])} | "
              f"{', '.join(f'{c}: {n}' for c, n in sorted(failed.items())) or 0} |")
