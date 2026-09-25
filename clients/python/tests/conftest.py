"""The real llm-d-async binary on fresh ports, in front of a stand-in
gateway that is a real HTTP server. Nothing inside the processor is
replaced."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import threading
import time
from collections.abc import Callable, Generator, Sequence
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Protocol

import httpx2
import pytest
from anyio.from_thread import BlockingPortal, start_blocking_portal

from llm_d_async import AsyncClient, Client, Delivery, Result, Submission, Submitted

ROOT = Path(__file__).resolve().parents[3]


def pattern(n: int, mod: int) -> bytes:
    return bytes(i % mod for i in range(n))


@dataclass
class Upstream:
    """Echoes a JSON request's prompt; `/v1/audio/speech` answers with
    binary audio."""

    url: str = ""
    audio: bytes = field(default_factory=lambda: pattern(3 << 20, 251))
    seen: int = 0
    body: bytes = b""


def start_upstream() -> tuple[Upstream, ThreadingHTTPServer]:
    up = Upstream()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_POST(self) -> None:
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            up.seen += 1
            up.body = body
            if self.path == "/v1/audio/speech":
                reply, content_type = up.audio, "audio/mpeg"
            else:
                reply, content_type = (
                    json.dumps({"echo": prompt(body)}).encode(),
                    "application/json",
                )
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(reply)))
            self.end_headers()
            self.wfile.write(reply)

        def log_message(self, format: str, *args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    up.url = f"http://127.0.0.1:{server.server_address[1]}"
    return up, server


def prompt(body: bytes) -> str:
    """The `prompt` of a JSON object body, empty for any other body."""
    try:
        value: object = json.loads(body)["prompt"]
    except (ValueError, TypeError, KeyError, IndexError):
        return ""
    return value if isinstance(value, str) else ""


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def binary() -> str:
    path = os.environ.get("LDA_BIN") or str(ROOT / "target/debug/llm-d-async")
    if not Path(path).exists():
        pytest.fail(f"processor binary not found at {path}: run `cargo build` or set LDA_BIN")
    return path


@dataclass
class Processor:
    api: str
    queue: str
    gated: str
    results: str
    budget: str
    postgres: bool
    upstream: Upstream

    def set_budget(self, value: str) -> None:
        r = httpx2.put(
            f"{self.api}/v1/admin/budgets/{self.budget}",
            content=value,
            headers={"content-type": "application/json"},
        )
        assert r.status_code < 300, r.text


@pytest.fixture(params=["embedded", "postgres"])
def processor(request: pytest.FixtureRequest, tmp_path: Path) -> Generator[Processor]:
    postgres_url = ""
    if request.param == "postgres":
        postgres_url = os.environ.get("TEST_DATABASE_URL", "")
        if not postgres_url:
            if os.environ.get("REQUIRE_POSTGRES"):
                pytest.fail("REQUIRE_POSTGRES is set but TEST_DATABASE_URL is not")
            pytest.skip("TEST_DATABASE_URL not set")
    up, server = start_upstream()
    suffix = str(time.time_ns())
    names = {k: f"{k}-{suffix}" for k in ("q", "gated", "results", "budget")}
    transport = {
        "poll_interval_ms": 50,
        "result_queue_name": names["results"],
        "queues": [
            {"queue_name": names["q"], "igw_base_url": up.url},
            {
                "queue_name": names["gated"],
                "igw_base_url": up.url,
                "gate_type": "budget-key",
                "gate_params": {"budget_key": names["budget"]},
            },
        ],
    }
    config = tmp_path / "transport.json"
    config.write_text(json.dumps(transport))
    log_path = tmp_path / "processor.log"
    proc: subprocess.Popen[bytes] | None = None
    api = ""
    for _ in range(5):
        api_port, health, metrics = free_port(), free_port(), free_port()
        args = [
            binary(),
            "--data-dir",
            str(tmp_path / "data"),
            "--transport-config-file",
            str(config),
            "--api-addr",
            f"127.0.0.1:{api_port}",
            "--health-port",
            str(health),
            "--metrics-port",
            str(metrics),
        ]
        if postgres_url:
            args += [
                "--store=postgres",
                f"--database-url={postgres_url}",
                "--database-max-connections=4",
                "--partition-lease-ttl=3s",
                f"--blob-store=file://{tmp_path / 'blobs'}",
            ]
        env = {**os.environ, "RUST_LOG": "warn", "OTEL_EXPORTER_OTLP_ENDPOINT": ""}
        with log_path.open("ab") as log:
            proc = subprocess.Popen(args, stdout=log, stderr=log, env=env)
        until = time.monotonic() + 30
        while time.monotonic() < until and proc.poll() is None:
            try:
                if httpx2.get(f"http://127.0.0.1:{health}/readyz").status_code == 200:
                    api = f"http://127.0.0.1:{api_port}"
                    break
            except httpx2.HTTPError:
                pass
            time.sleep(0.05)
        if api:
            break
        proc.kill()
        proc.wait()
    if not api or proc is None:
        pytest.fail(f"processor never became ready:\n{log_path.read_text()}")
    yield Processor(
        api=api,
        queue=names["q"],
        gated=names["gated"],
        results=names["results"],
        budget=names["budget"],
        postgres=bool(postgres_url),
        upstream=up,
    )
    proc.kill()
    proc.wait()
    server.shutdown()
    print(f"processor log:\n{log_path.read_text()}")


class Api(Protocol):
    """What the scenarios need from a client, sync or not."""

    @property
    def result_queue(self) -> str: ...
    def submit(self, submission: Submission) -> Submitted: ...
    def submit_batch(self, submissions: Sequence[Submission]) -> list[Submitted]: ...
    def cancel(self, ids: Sequence[str]) -> int: ...
    def pop_result(self, timeout: float | None = None) -> Result: ...
    def receive_result(self, timeout: float | None = None) -> Delivery: ...
    def renew(self, delivery: Delivery) -> None: ...
    def ack(self, delivery: Delivery) -> None: ...
    def queue_depth(self, name: str) -> int: ...
    def result_queue_depth(self) -> int: ...


class BlockingAsync:
    """Runs an `AsyncClient` on a portal's event loop, blocking for each
    call, so one scenario covers both clients."""

    def __init__(self, portal: BlockingPortal, client: AsyncClient) -> None:
        self._portal = portal
        self._client = client

    @property
    def result_queue(self) -> str:
        return self._client.result_queue

    def submit(self, submission: Submission) -> Submitted:
        return self._portal.call(self._client.submit, submission)

    def submit_batch(self, submissions: Sequence[Submission]) -> list[Submitted]:
        return self._portal.call(self._client.submit_batch, submissions)

    def cancel(self, ids: Sequence[str]) -> int:
        return self._portal.call(self._client.cancel, ids)

    def pop_result(self, timeout: float | None = None) -> Result:
        return self._portal.call(self._client.pop_result, timeout)

    def receive_result(self, timeout: float | None = None) -> Delivery:
        return self._portal.call(self._client.receive_result, timeout)

    def renew(self, delivery: Delivery) -> None:
        self._portal.call(self._client.renew, delivery)

    def ack(self, delivery: Delivery) -> None:
        self._portal.call(self._client.ack, delivery)

    def queue_depth(self, name: str) -> int:
        return self._portal.call(self._client.queue_depth, name)

    def result_queue_depth(self) -> int:
        return self._portal.call(self._client.result_queue_depth)


Clients = Callable[..., Api]


@pytest.fixture(params=["sync", "async"])
def clients(request: pytest.FixtureRequest, processor: Processor) -> Generator[Clients]:
    """Builds clients of the parameter's kind for `processor`, with its
    queues and a short poll wait."""
    closers: list[Callable[[], object]] = []
    with start_blocking_portal() as portal:

        def make(result_queue: str = "", lease: float = 300.0) -> Api:
            results = result_queue or processor.results
            if request.param == "sync":
                client = Client(
                    processor.api,
                    request_queue=processor.queue,
                    result_queue=results,
                    poll_wait=2.0,
                    lease=lease,
                )
                closers.append(client.close)
                return client
            async_client = AsyncClient(
                processor.api,
                request_queue=processor.queue,
                result_queue=results,
                poll_wait=2.0,
                lease=lease,
            )
            closers.append(lambda: portal.call(async_client.aclose))
            return BlockingAsync(portal, async_client)

        yield make
        for close in closers:
            close()
