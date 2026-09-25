# /// script
# requires-python = ">=3.12"
# dependencies = ["matplotlib>=3.9"]
# ///
"""Charts of a benchmark matrix run: uv run charts.py runs/matrix-*/results.jsonl."""

import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.ticker import FuncFormatter

IMPLS = ("rust", "go")
COLORS = {"rust": "#E8590C", "go": "#1971C2"}
LABELS = {"rust": "Rust (this repo)", "go": "Go sql transport"}
INK = "#212529"
MUTED = "#868E96"


def style() -> None:
    for path in (Path.home() / "Library/Fonts").glob("TX-02*"):
        font_manager.fontManager.addfont(str(path))
    plt.rcParams.update(
        {
            "font.family": "TX-02",
            "font.size": 10,
            "axes.titlesize": 12,
            "axes.titlelocation": "left",
            "axes.labelcolor": INK,
            "axes.edgecolor": MUTED,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.grid": True,
            "axes.grid.axis": "y",
            "axes.axisbelow": True,
            "grid.color": "#E9ECEF",
            "grid.linewidth": 0.8,
            "xtick.color": INK,
            "ytick.color": INK,
            "legend.frameon": False,
            "figure.facecolor": "white",
            "savefig.dpi": 200,
            "savefig.bbox": "tight",
        }
    )


def load(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def spread(values: list[float]) -> tuple[float, float, float]:
    """Median, and the distance down to the minimum and up to the maximum."""
    m = statistics.median(values)
    return m, m - min(values), max(values) - m


def thousands(x: float, _pos: int) -> str:
    return f"{x / 1000:.0f}k" if x >= 1000 else f"{x:.0f}"


def legend(fig, ax, ncol: int = 2) -> None:
    fig.legend(*ax.get_legend_handles_labels(), loc="upper center", ncol=ncol,
               bbox_to_anchor=(0.5, 0.0), fontsize=9)


def grouped_bars(ax, groups: list[str], series: dict[str, list[list[float]]], fmt: str) -> None:
    width = 0.38
    for i, impl in enumerate(IMPLS):
        xs = [g + (i - 0.5) * width for g in range(len(groups))]
        stats = [spread(v) if v else (0.0, 0.0, 0.0) for v in series[impl]]
        meds = [s[0] for s in stats]
        ax.bar(xs, meds, width, color=COLORS[impl], label=LABELS[impl],
               yerr=[[s[1] for s in stats], [s[2] for s in stats]],
               error_kw={"ecolor": INK, "elinewidth": 0.8, "capsize": 2})
        for x, m in zip(xs, meds):
            if m:
                ax.annotate(fmt.format(m), (x, m), xytext=(0, 3), textcoords="offset points",
                            ha="center", va="bottom", fontsize=6.5, color=INK)
    ax.set_xticks(range(len(groups)), groups)


def scaling(rows: list[dict], out: Path) -> None:
    drains = [r for r in rows if r["mode"] == "drain" and r.get("isl", 256) == 256]
    configs = sorted({(r["queues"], r["replicas"]) for r in drains})
    groups = [f"{q}q\n{rp}r" for q, rp in configs]

    def series(key):
        return {i: [[key(r) for r in drains if r["impl"] == i and (r["queues"], r["replicas"]) == c]
                    for c in configs] for i in IMPLS}

    fig, (a, b) = plt.subplots(1, 2, figsize=(12, 4.2))
    grouped_bars(a, groups, series(lambda r: r["dispatch_per_s"]), "{:,.0f}")
    a.set_title("Dispatch rate")
    a.set_ylabel("requests dispatched / s")
    a.yaxis.set_major_formatter(FuncFormatter(thousands))
    grouped_bars(b, groups, series(lambda r: r["total_s"]), "{:.1f}")
    b.set_title("Cold start to last result")
    b.set_ylabel("seconds (lower is better)")
    n = drains[0]["requests"] if drains else 0
    legend(fig, a)
    fig.suptitle(f"Draining {n:,} queued requests · q = queues, r = replicas · median of runs, whiskers min–max",
                 x=0.01, ha="left", color=MUTED, fontsize=9, y=1.0)
    fig.savefig(out / "scaling.png")
    plt.close(fig)


def database(rows: list[dict], out: Path) -> None:
    drains = [r for r in rows if r["mode"] == "drain" and r.get("isl", 256) == 256]
    configs = sorted({(r["queues"], r["replicas"]) for r in drains})
    groups = [f"{q}q\n{rp}r" for q, rp in configs]

    def series(key):
        return {i: [[key(r) for r in drains if r["impl"] == i and (r["queues"], r["replicas"]) == c]
                    for c in configs] for i in IMPLS}

    fig, axes = plt.subplots(1, 3, figsize=(17, 4.4))
    for ax, (title, key, fmt) in zip(axes, [
        ("Transactions per request", lambda r: r["db"]["xacts"], "{:.2f}"),
        ("Postgres execution ms per request", lambda r: r["db"].get("exec_ms", 0), "{:.2f}"),
        ("Processor CPU ms per request", lambda r: r["cpu_ms_per_request"], "{:.3f}"),
    ]):
        grouped_bars(ax, groups, series(key), fmt)
        ax.set_title(title)
    legend(fig, axes[0])
    fig.suptitle("Cost of each dispatched request · q = queues, r = replicas · lower is better", x=0.01, ha="left",
                 color=MUTED, fontsize=9, y=1.0)
    fig.savefig(out / "cost.png")
    plt.close(fig)


def sizes(rows: list[dict], out: Path) -> None:
    drains = [r for r in rows if r["mode"] == "drain" and r["queues"] == 1 and r["replicas"] == 1
              and "isl" in r]
    combos = sorted({(r["isl"], r["osl"], r["requests"]) for r in drains})
    groups = [f"{isl:,}/{osl:,}\n{n:,} req" for isl, osl, n in combos]

    def series(key):
        return {i: [[key(r) for r in drains if r["impl"] == i and (r["isl"], r["osl"], r["requests"]) == c]
                    for c in combos] for i in IMPLS}

    fig, (a, b) = plt.subplots(1, 2, figsize=(13, 4.4))
    grouped_bars(a, groups, series(lambda r: r["total_s"]), "{:.1f}")
    a.set_title("Cold start to last result")
    a.set_ylabel("seconds (lower is better)")
    grouped_bars(b, groups, series(lambda r: r["max_rss_mib"]), "{:.0f}")
    b.set_title("Processor peak memory")
    b.set_ylabel("max RSS, MiB (lower is better)")
    for ax in (a, b):
        ax.set_xlabel("ISL / OSL tokens (4 bytes a token)")
    legend(fig, a)
    fig.suptitle("Request and response sizes · 1 queue, 1 replica", x=0.01, ha="left",
                 color=MUTED, fontsize=9, y=1.0)
    fig.savefig(out / "sizes.png")
    plt.close(fig)


def latency(rows: list[dict], out: Path) -> None:
    rates = [r for r in rows if r["mode"] == "rate" and r.get("result_latency_ms")]
    if not rates:
        return
    fig, (a, b) = plt.subplots(1, 2, figsize=(12, 4.2), sharey=True)
    for ax, field, title in [(a, "dispatch_lag_ms", "Submit to gateway"),
                             (b, "result_latency_ms", "Submit to result read")]:
        for impl in IMPLS:
            by_rate = defaultdict(list)
            for r in rates:
                if r["impl"] == impl and r.get(field):
                    by_rate[round(r["offered_per_s"], -1) or 10].append(r[field])
            xs = sorted(by_rate)
            for q, style_ in (("p50", "-"), ("p99", "--")):
                ys = [statistics.median(x[q] for x in by_rate[k]) for k in xs]
                ax.plot(xs, ys, style_, marker="o", ms=4, color=COLORS[impl],
                        label=f"{LABELS[impl]} {q}")
        ax.set_xscale("log")
        ax.set_title(title)
        ax.set_xlabel("offered requests / s")
        ax.xaxis.set_major_formatter(FuncFormatter(thousands))
    a.set_ylabel("ms (lower is better)")
    legend(fig, a, ncol=4)
    fig.suptitle("Latency by load · 1 queue, 1 replica, 10 ms poll · Rust submits over HTTP, "
                 "Go's producer inserts directly", x=0.01, ha="left", color=MUTED, fontsize=9, y=1.0)
    fig.savefig(out / "latency.png")
    plt.close(fig)


def main() -> None:
    if len(sys.argv) != 3:
        sys.exit("usage: uv run charts.py RESULTS.jsonl OUT_DIR")
    rows = load(Path(sys.argv[1]))
    out = Path(sys.argv[2])
    out.mkdir(parents=True, exist_ok=True)
    style()
    scaling(rows, out)
    database(rows, out)
    sizes(rows, out)
    latency(rows, out)
    print(f"wrote {', '.join(sorted(p.name for p in out.glob('*.png')))} to {out}")


if __name__ == "__main__":
    main()
