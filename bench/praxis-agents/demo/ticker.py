#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Formats `stern --output json` lines from the EPP and the processor into
one line per eviction, resume and requeue. Processor lines name the agent
when `$AGENTS_RESPONSES` points at the `responses.tsv` agents.py writes."""

import json
import os
import re
import sys
import time

ANSI = re.compile(r"\x1b\[[0-9;]*m")
FIELD = re.compile(r"([\w.]+)=(\"[^\"]*\"|\S+)")
RESET, DIM, BOLD, RED, YELLOW, MAGENTA, CYAN = "\x1b[0m", "\x1b[2m", "\x1b[1m", "\x1b[31m", "\x1b[33m", "\x1b[35m", "\x1b[36m"


def fields(message: str) -> dict[str, str]:
    try:
        parsed = json.loads(message)
        if isinstance(parsed, dict):
            return {k: str(v) for k, v in parsed.items()}
    except json.JSONDecodeError:
        pass
    return {k: v.strip('"') for k, v in FIELD.findall(message)}


class Agents:
    def __init__(self, path: str) -> None:
        self.path = path
        self.names: dict[str, str] = {}

    def name(self, response_id: str) -> str:
        if response_id not in self.names and self.path:
            try:
                with open(self.path) as f:
                    self.names = dict(line.rstrip("\n").split("\t", 1) for line in f if "\t" in line)
            except OSError:
                pass
        return self.names.get(response_id, response_id[-8:])


def first(f: dict[str, str], *keys: str) -> str:
    return next((f[k] for k in keys if f.get(k)), "?")


def main() -> None:
    agents = Agents(os.environ.get("AGENTS_RESPONSES", ""))
    for line in sys.stdin:
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        pod = entry.get("podName", "")
        message = ANSI.sub("", entry.get("message", ""))
        f = fields(message)
        stamp = f"{DIM}{time.strftime('%H:%M:%S')}{RESET}"
        if "resuming interrupted generation" in message:
            agent = agents.name(first(f, "request.id", "gen_ai.request.id"))
            print(f"{stamp} {CYAN}processor{RESET} {BOLD}{YELLOW}⚡ resume {RESET}  {BOLD}{agent:<10}{RESET} from {BOLD}{YELLOW}{first(f, 'saved_tokens'):>5}{RESET} saved tokens {DIM}(resume #{first(f, 'resumes')}){RESET}")
        elif "retrying request" in message:
            agent = agents.name(first(f, "request.id", "gen_ai.request.id"))
            reason = first(f, "error").rstrip(":")
            print(f"{stamp} {CYAN}processor{RESET} {BOLD}{MAGENTA}↻ requeue{RESET}  {BOLD}{agent:<10}{RESET} with {first(f, 'saved_tokens'):>5} saved tokens {DIM}(retry #{first(f, 'retry_count')}, {reason}){RESET}")
        elif "escalating request" in message:
            agent = agents.name(first(f, "request.id", "gen_ai.request.id"))
            print(f"{stamp} {CYAN}processor{RESET} {BOLD}{CYAN}⬆ escalate{RESET} {BOLD}{agent:<10}{RESET} after {first(f, 'retry_count')} retries without output → objective {BOLD}{first(f, 'objective')}{RESET}")
        elif "evicted by flow control" in message and "epp" in pod:
            rid = first(f, "x-request-id", "requestID", "request_id", "requestId")[:8]
            print(f"{stamp} {RED}epp      {RESET} {BOLD}{RED}✂ evicted{RESET}  {DIM}{rid:<10}{RESET} a batch generation, to make room for interactive work")
        else:
            continue
        sys.stdout.flush()


if __name__ == "__main__":
    main()
