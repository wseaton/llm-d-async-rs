#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["openai>=1.0", "rich>=13"]
# ///
"""Research agents that run every model turn as an OpenAI Responses
background response through Praxis AI, so each turn is `batch` work in
llm-d-async that flow control may evict.

Each agent investigates one question about a workspace (this repository by
default) with read-only tools it runs locally, continuing with
`previous_response_id` until it writes its report. A board shows every
agent's turn and status, and tags the agent whose generation the processor
resumed, from the processor's `resuming interrupted generation` log lines
read from `--processor-logs`.

Reports land in `--out`, with `summary.json` describing the run.
"""

import argparse
import json
import os
import re
import shlex
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

import openai
from rich.console import Console, Group
from rich.live import Live
from rich.table import Table
from rich.text import Text

TASKS = [
    ("resume", "How does a resumable queue resume a generation that flow control evicted? Trace it from the worker in src/worker through render, generate and derender."),
    ("redis", "How does the Redis store in src/store/redis claim, renew and reclaim requests, and what keeps it compatible with the Go processor?"),
    ("gates", "How do admission gates and quota counters in src/gate decide whether a request is dispatched now or waits?"),
    ("results", "How are results delivered by request (the @request- routes), and how do long polls get woken when a result is written?"),
    ("metrics", "Which Prometheus metrics does the processor export (src/telemetry), and which code paths change each one?"),
    ("dispatch", "How does the dispatch consumer in src/dispatch pull requests from queues and stamp their routing before a worker sends them?"),
    ("drain", "What happens to in-flight requests when a processor replica drains or is killed, and how does the next replica pick up saved progress?"),
    ("go-client", "What does the Go client in clients/go offer, and how does it talk to the processor and to Redis?"),
]

INSTRUCTIONS = """You are a research agent investigating a Rust codebase. Use the tools to
read the code; do not guess. Read the files that matter, follow the calls, and
cite file paths. When you have enough, stop calling tools and write a detailed
report in Markdown (at least 800 words): an overview, the control flow step by
step with file references, the invariants the code relies on, and anything
surprising."""

TOOLS = [
    {
        "type": "function",
        "name": "list_dir",
        "description": "List a directory of the workspace. Directories end with '/'.",
        "parameters": {
            "type": "object",
            "properties": {"path": {"type": "string", "description": "Relative path, '.' for the root."}},
            "required": ["path"],
        },
    },
    {
        "type": "function",
        "name": "read_file",
        "description": "Read lines of a workspace file, numbered.",
        "parameters": {
            "type": "object",
            "properties": {
                "path": {"type": "string"},
                "start": {"type": "integer", "description": "First line, 1-based. Default 1."},
                "lines": {"type": "integer", "description": "How many lines, at most 400. Default 200."},
            },
            "required": ["path"],
        },
    },
    {
        "type": "function",
        "name": "grep",
        "description": "Search workspace files for a regular expression. Returns path:line: text.",
        "parameters": {
            "type": "object",
            "properties": {
                "pattern": {"type": "string"},
                "path": {"type": "string", "description": "Directory or file to search. Default '.'."},
            },
            "required": ["pattern"],
        },
    },
]

SKIP_DIRS = {".git", "target", "node_modules", "__pycache__", "fixtures"}
MAX_TOOL_OUTPUT = 12_000
ANSI = re.compile(r"\x1b\[[0-9;]*m")
RESUME_LINE = re.compile(r"request\.id=(\S+?)[\s}].*resuming interrupted generation.*saved_tokens=(\d+)")
RETRY_LINE = re.compile(r"request\.id=(\S+?)[\s}].*retrying request")
ESCALATE_LINE = re.compile(r"escalating request.*request\.id=(\S+?)(?:\s|$)|request\.id=(\S+?)[\s}].*escalating request")


class Workspace:
    def __init__(self, root: Path) -> None:
        self.root = root.resolve()

    def resolve(self, path: str) -> Path:
        p = (self.root / path).resolve()
        if not p.is_relative_to(self.root):
            raise ValueError(f"{path} is outside the workspace")
        return p

    def list_dir(self, path: str) -> str:
        p = self.resolve(path)
        entries = sorted(e for e in p.iterdir() if e.name not in SKIP_DIRS)
        return "\n".join(f"{e.name}/" if e.is_dir() else e.name for e in entries)

    def read_file(self, path: str, start: int = 1, lines: int = 200) -> str:
        text = self.resolve(path).read_text(errors="replace").splitlines()
        start = max(start, 1)
        chunk = text[start - 1 : start - 1 + min(max(lines, 1), 400)]
        body = "\n".join(f"{start + i}: {line}" for i, line in enumerate(chunk))
        return f"{body}\n({len(text)} lines in {path})"

    def files(self, base: Path):
        for d, dirs, names in os.walk(base):
            dirs[:] = sorted(n for n in dirs if n not in SKIP_DIRS)
            for n in sorted(names):
                yield Path(d) / n

    def grep(self, pattern: str, path: str = ".") -> str:
        regex = re.compile(pattern)
        base = self.resolve(path)
        hits: list[str] = []
        for f in [base] if base.is_file() else self.files(base):
            try:
                for n, line in enumerate(f.read_text().splitlines(), 1):
                    if regex.search(line):
                        hits.append(f"{f.relative_to(self.root)}:{n}: {line.strip()}")
            except (UnicodeDecodeError, OSError):
                continue
            if len(hits) >= 200:
                break
        return "\n".join(hits) or "no matches"

    def run(self, name: str, arguments: str) -> str:
        try:
            args = json.loads(arguments or "{}")
            out = getattr(self, name)(**args) if name in {t["name"] for t in TOOLS} else f"unknown tool {name}"
        except Exception as e:  # the model sees tool errors and can correct itself
            out = f"error: {e}"
        return out[:MAX_TOOL_OUTPUT]


@dataclass
class Agent:
    name: str
    question: str
    turn: int = 0
    status: str = "starting"
    tool_calls: int = 0
    last_tool: str = ""
    response_ids: list[str] = field(default_factory=list)
    resumes: list[int] = field(default_factory=list)
    requeues: int = 0
    escalations: int = 0
    flash: str = ""
    flash_until: float = 0.0
    started: float = field(default_factory=time.monotonic)
    finished: float | None = None
    output_tokens: int = 0
    error: str = ""


class Run:
    """Every agent's state, and `responses.tsv`, which maps each response ID
    the agents submit to the agent's name, for the ticker."""

    def __init__(self, agents: list[Agent], out: Path) -> None:
        self.agents = agents
        self.by_response: dict[str, Agent] = {}
        self.lock = threading.Lock()
        self.unmatched_resumes = 0
        self.index = (out / "responses.tsv").open("w")

    def issued(self, agent: Agent, response_id: str) -> None:
        with self.lock:
            agent.response_ids.append(response_id)
            self.by_response[response_id] = agent
            self.index.write(f"{response_id}\t{agent.name}\n")
            self.index.flush()

    def resumed(self, response_id: str, saved_tokens: int) -> None:
        with self.lock:
            agent = self.by_response.get(response_id)
            if agent is None:
                self.unmatched_resumes += 1
                return
            agent.resumes.append(saved_tokens)
            agent.flash = f"⚡ evicted → resumed @{saved_tokens}"
            agent.flash_until = time.monotonic() + 4

    def escalated(self, response_id: str) -> None:
        with self.lock:
            agent = self.by_response.get(response_id)
            if agent is None:
                return
            agent.escalations += 1
            agent.flash = "⬆ starved → escalated to interactive"
            agent.flash_until = time.monotonic() + 4

    def requeued(self, response_id: str) -> None:
        with self.lock:
            agent = self.by_response.get(response_id)
            if agent is None:
                return
            agent.requeues += 1
            agent.flash = "↻ interrupted → requeued"
            agent.flash_until = time.monotonic() + 4


def wait(client: openai.OpenAI, agent: Agent, response):
    while response.status in ("queued", "in_progress"):
        agent.status = response.status
        time.sleep(1)
        response = client.responses.retrieve(response.id)
    return response


def research(client: openai.OpenAI, run: Run, ws: Workspace, agent: Agent, args: argparse.Namespace) -> None:
    agent.started = time.monotonic()
    create = dict(model=args.model, instructions=INSTRUCTIONS, background=True, max_output_tokens=args.max_output_tokens)
    request = dict(input=[{"role": "user", "content": agent.question}], tools=TOOLS)
    try:
        while True:
            agent.turn += 1
            response = client.responses.create(**create, **request)
            run.issued(agent, response.id)
            response = wait(client, agent, response)
            if response.usage:
                agent.output_tokens += response.usage.output_tokens
            if response.status != "completed":
                agent.status = response.status
                agent.error = str(response.error or response.incomplete_details or "")
                break
            calls = [o for o in response.output if o.type == "function_call"]
            if not calls:
                (Path(args.out) / f"{agent.name}.md").write_text(f"# {agent.question}\n\n{response.output_text}\n")
                agent.status = "done"
                break
            outputs = []
            for c in calls:
                agent.tool_calls += 1
                agent.last_tool = f"{c.name}({_brief(c.arguments)})"
                agent.status = "tools"
                outputs.append({"type": "function_call_output", "call_id": c.call_id, "output": ws.run(c.name, c.arguments)})
            request = dict(previous_response_id=response.id, input=outputs, tools=TOOLS)
            if agent.turn >= args.max_turns:
                outputs.append({"role": "user", "content": "Stop investigating and write the report now."})
                request.pop("tools")
    except Exception as e:
        agent.status = "error"
        agent.error = str(e)
    agent.finished = time.monotonic()


def _brief(arguments: str) -> str:
    try:
        args = json.loads(arguments)
    except json.JSONDecodeError:
        return ""
    s = ", ".join(str(v) for v in args.values())
    return s if len(s) <= 28 else s[:27] + "…"


def follow_logs(run: Run, command: str) -> None:
    proc = subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    assert proc.stdout is not None
    for line in proc.stdout:
        line = ANSI.sub("", line)
        if m := RESUME_LINE.search(line):
            run.resumed(m.group(1), int(m.group(2)))
        elif m := RETRY_LINE.search(line):
            run.requeued(m.group(1))
        elif m := ESCALATE_LINE.search(line):
            run.escalated(m.group(1) or m.group(2))


STYLE = {"queued": "dim", "in_progress": "yellow", "tools": "cyan", "done": "green", "starting": "dim"}


def board(run: Run, stack: str) -> Group:
    now = time.monotonic()
    table = Table(expand=True, border_style="grey50")
    for col, kw in [
        ("agent", {"style": "bold", "no_wrap": True}),
        ("turn", {"justify": "right"}),
        ("status", {"no_wrap": True}),
        ("tools", {"overflow": "ellipsis", "no_wrap": True, "ratio": 2}),
        ("resumed after eviction", {"no_wrap": True, "ratio": 2}),
        ("time", {"justify": "right"}),
    ]:
        table.add_column(col, **kw)
    for a in run.agents:
        status = Text(a.status.replace("_", " "), style=STYLE.get(a.status, "bold red"))
        if a.status == "done":
            status = Text("done ✓", style="bold green")
        if a.flash_until > now:
            status = Text(a.flash, style="bold black on yellow")
        resumes = Text("")
        if a.resumes:
            resumes += Text(f"⚡×{len(a.resumes)} ", style="bold yellow") + Text(
                " ".join(str(n) for n in a.resumes[-4:]) + " tokens kept ", style="yellow"
            )
        if a.requeues:
            resumes += Text(f"↻×{a.requeues} requeued ", style="magenta")
        if a.escalations:
            resumes += Text("⬆ escalated", style="bold cyan")
        elapsed = (a.finished or now) - a.started
        tools = f"{a.tool_calls} · {a.last_tool}" if a.tool_calls else ""
        table.add_row(a.name, str(a.turn), status, tools, resumes, f"{elapsed:5.0f}s")
    done = sum(a.status == "done" for a in run.agents)
    evictions = sum(len(a.resumes) for a in run.agents)
    requeues = sum(a.requeues for a in run.agents)
    escalations = sum(a.escalations for a in run.agents)
    kept = sum(sum(a.resumes) for a in run.agents)
    turns = sum(a.turn for a in run.agents)
    footer = Text.assemble(
        ("  agents done ", "dim"), (f"{done}/{len(run.agents)}", "bold green"),
        ("   background turns ", "dim"), (str(turns), "bold"),
        ("   evictions resumed ", "dim"), (str(evictions), "bold yellow"),
        ("   tokens kept ", "dim"), (f"{kept:,}", "bold yellow"),
        ("   requeued ", "dim"), (str(requeues), "bold magenta"),
        ("   escalated ", "dim"), (str(escalations), "bold cyan"),
        ("   tokens out ", "dim"), (f"{sum(a.output_tokens for a in run.agents):,}", "bold"),
    )
    if not stack:
        return Group(table, footer)
    return Group(Text(stack, style="bold cyan", justify="center"), table, footer)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base-url", default="http://127.0.0.1:18125/v1", help="Praxis AI")
    ap.add_argument("--model", default="zai-org/GLM-4.7-Flash")
    ap.add_argument("--agents", type=int, default=len(TASKS))
    ap.add_argument("--workspace", default=str(Path(__file__).resolve().parents[3]))
    ap.add_argument("--out", default="agent-reports")
    ap.add_argument("--max-turns", type=int, default=10)
    ap.add_argument("--max-output-tokens", type=int, default=8192)
    ap.add_argument("--stagger", type=float, default=1.5, help="Seconds between agent starts.")
    ap.add_argument(
        "--stack",
        default=os.environ.get("AGENTS_STACK", ""),
        help="Model and hardware line shown above the board (default: $AGENTS_STACK).",
    )
    ap.add_argument(
        "--processor-logs",
        default="kubectl --context coreweave-waldorf -n weaton-dev logs -f deploy/agents-processor --since=1s",
        help="Command whose output is the processor's log; empty to skip.",
    )
    args = ap.parse_args()

    Path(args.out).mkdir(parents=True, exist_ok=True)
    client = openai.OpenAI(base_url=args.base_url, api_key="x", max_retries=5)
    ws = Workspace(Path(args.workspace))
    agents = [Agent(name, q) for name, q in TASKS[: args.agents]]
    run = Run(agents, Path(args.out))
    if args.processor_logs:
        threading.Thread(target=follow_logs, args=(run, args.processor_logs), daemon=True).start()

    threads = []
    console = Console()
    with Live(board(run, args.stack), console=console, refresh_per_second=4) as live:
        for a in agents:
            t = threading.Thread(target=research, args=(client, run, ws, a, args), daemon=True)
            t.start()
            threads.append(t)
            deadline = time.monotonic() + args.stagger
            while time.monotonic() < deadline:
                live.update(board(run, args.stack))
                time.sleep(0.25)
        while any(t.is_alive() for t in threads):
            live.update(board(run, args.stack))
            time.sleep(0.25)
        time.sleep(1)
        live.update(board(run, args.stack))

    summary = {
        "agents": [
            {
                "name": a.name,
                "status": a.status,
                "turns": a.turn,
                "tool_calls": a.tool_calls,
                "resumed_at_tokens": a.resumes,
                "requeues": a.requeues,
                "escalations": a.escalations,
                "output_tokens": a.output_tokens,
                "seconds": round((a.finished or time.monotonic()) - a.started, 1),
                "responses": a.response_ids,
                "error": a.error,
            }
            for a in agents
        ],
        "unmatched_resumes": run.unmatched_resumes,
    }
    (Path(args.out) / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return 0 if all(a.status == "done" for a in agents) else 1


if __name__ == "__main__":
    sys.exit(main())
