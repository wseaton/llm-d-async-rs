"""Request building and response parsing shared by the sync and async
clients, which only differ in how they do I/O."""

from __future__ import annotations

import json
import secrets
import time
from collections.abc import AsyncIterable, AsyncIterator, Iterable, Iterator
from dataclasses import dataclass
from typing import Any, Protocol, TypeAlias, runtime_checkable
from urllib.parse import quote, urlsplit

import httpx2

from llm_d_async._errors import LlmDAsyncError, NotFoundError, OwnershipLostError, StatusError
from llm_d_async._types import Delivery, ErrorCode, Result, Submission, Submitted

DEFAULT_RESULT_QUEUE = "result-list"
DEFAULT_POLL_WAIT = 30.0
DEFAULT_LEASE = 300.0
MAX_POLL_WAIT = 60.0
RESULT_REF_PREFIX = "blob://results/"
CHUNK = 64 << 10


@runtime_checkable
class Readable(Protocol):
    """A binary file or anything else read in chunks."""

    def read(self, n: int = -1, /) -> bytes: ...


Body: TypeAlias = "bytes | Readable | Iterable[bytes]"
AsyncBody: TypeAlias = "bytes | Readable | Iterable[bytes] | AsyncIterable[bytes]"


@dataclass(frozen=True)
class Settings:
    base_url: str
    request_queue: str
    result_queue: str
    poll_wait: float
    lease: float

    def __post_init__(self) -> None:
        url = urlsplit(self.base_url)
        if url.scheme not in ("http", "https") or not url.netloc:
            raise ValueError(f"processor URL {self.base_url!r}: want http(s)://host[:port]")
        if not self.result_queue:
            raise ValueError("result queue name must not be empty")
        if not 0 < self.poll_wait <= MAX_POLL_WAIT:
            raise ValueError(f"poll wait must be in (0, {MAX_POLL_WAIT}] seconds")
        if self.lease <= 0:
            raise ValueError("result lease must be positive")

    @property
    def base(self) -> str:
        return self.base_url.rstrip("/")

    def timeout(self) -> httpx2.Timeout:
        return httpx2.Timeout(connect=10.0, read=self.poll_wait + 15.0, write=60.0, pool=10.0)

    def result_path(self, suffix: str) -> str:
        return f"/v1/results/{quote(self.result_queue, safe='')}{suffix}"

    def claim_path(self, delivery: Delivery, action: str) -> str:
        return self.result_path(f"/claims/{delivery.claim_id}/{action}")

    def submission(self, s: Submission) -> dict[str, Any]:
        out: dict[str, Any] = {"id": s.id, "deadline": s.deadline}
        if s.created:
            out["created"] = s.created
        if s.payload is not None:
            out["payload"] = s.payload
        for key, value in (
            ("metadata", dict(s.metadata)),
            ("headers", dict(s.headers)),
            ("endpoint", s.endpoint),
            ("model", s.model),
            ("request_queue_name", s.request_queue or self.request_queue),
            ("result_queue_name", s.result_queue or self.result_queue),
        ):
            if value:
                out[key] = value
        return out

    def claim_params(self, deadline: float | None) -> dict[str, int]:
        return {"wait_ms": wait_ms(self.poll_wait, deadline), "lease_ms": int(self.lease * 1000)}

    def renewal(self, delivery: Delivery) -> dict[str, Any]:
        return {"owner_token": delivery.owner_token, "lease_ms": int(self.lease * 1000)}


def deadline_after(timeout: float | None) -> float | None:
    return None if timeout is None else time.monotonic() + timeout


def expired(deadline: float | None) -> bool:
    return deadline is not None and time.monotonic() >= deadline


def wait_ms(poll_wait: float, deadline: float | None) -> int:
    wait = poll_wait if deadline is None else min(poll_wait, deadline - time.monotonic())
    return max(int(wait * 1000), 0)


def submitted(raw: Any) -> Submitted:
    return Submitted(id=raw["id"], request_token=raw["request_token"])


def submitted_batch(raw: Any) -> list[Submitted]:
    if raw is None:
        return []
    items: list[Any] = list(raw)
    return [submitted(s) for s in items]


def field_int(raw: Any, name: str) -> int:
    """An integer field of a JSON object reply, 0 when absent."""
    if raw is None:
        return 0
    value: Any = raw.get(name, 0)
    return int(value)


def result(raw: Any) -> Result:
    code = raw.get("error_code")
    return Result(
        id=raw["id"],
        status_code=raw.get("status_code", 0),
        payload=raw.get("payload", ""),
        error_code=ErrorCode(code) if code else None,
        error_message=raw.get("error_message", ""),
        payload_ref=raw.get("payload_ref", ""),
        payload_location=raw.get("payload_location", ""),
        content_type=raw.get("content_type", ""),
        payload_size=raw.get("payload_size", 0),
        payload_sha256=raw.get("payload_sha256", ""),
        request_token=raw.get("request_token", ""),
    )


def delivery(raw: Any) -> Delivery:
    return Delivery(
        result=result(raw["result"]),
        claim_id=raw["claim_id"],
        owner_token=raw["owner_token"],
    )


def queue_depth(raw: Any, name: str) -> int:
    for queue in raw:
        if queue["queue_name"] == name:
            return int(queue["depth"])
    raise NotFoundError(404, f"queue {name!r}")


def blob_path(r: Result) -> str:
    name = r.payload_ref.removeprefix(RESULT_REF_PREFIX)
    if not r.payload_ref.startswith(RESULT_REF_PREFIX) or not name:
        raise LlmDAsyncError(f"result {r.id!r} has no body by reference")
    return f"/v1/blobs/results/{quote(name, safe='')}"


def status_error(status: int, body: bytes) -> StatusError:
    message = body[: 64 << 10].decode("utf-8", "replace").strip()
    try:
        error: Any = json.loads(message)["error"]
    except (ValueError, TypeError, KeyError, IndexError):
        error = None
    if isinstance(error, str) and error:
        message = error
    if status == 409:
        return OwnershipLostError(status, message)
    if status == 404:
        return NotFoundError(status, message)
    return StatusError(status, message)


def decode(response: httpx2.Response) -> Any | None:
    """The JSON body of a successful response; `None` for 204."""
    if response.status_code == 204:
        return None
    if response.status_code >= 300:
        raise status_error(response.status_code, response.content)
    if not response.content:
        return {}
    return response.json()


def stream_content_type(content_type: str) -> str:
    if not content_type or "\r" in content_type or "\n" in content_type:
        raise ValueError(f"invalid payload content type {content_type!r}")
    return content_type


class Multipart:
    """The streamed `multipart/form-data` body of a submission: a `request`
    part with the JSON envelope, then a `payload` part with the body."""

    def __init__(self, envelope: dict[str, Any], content_type: str) -> None:
        if "payload" in envelope:
            raise ValueError("submit_stream takes the payload as body, not Submission.payload")
        self.boundary = secrets.token_hex(16)
        self.head = (
            f"--{self.boundary}\r\n"
            'Content-Disposition: form-data; name="request"\r\n\r\n'
            f"{json.dumps(envelope)}\r\n"
            f"--{self.boundary}\r\n"
            'Content-Disposition: form-data; name="payload"; filename="payload"\r\n'
            f"Content-Type: {stream_content_type(content_type)}\r\n\r\n"
        ).encode()
        self.tail = f"\r\n--{self.boundary}--\r\n".encode()

    @property
    def content_type(self) -> str:
        return f"multipart/form-data; boundary={self.boundary}"

    def chunks(self, body: Body) -> Iterator[bytes]:
        yield self.head
        yield from _sync_chunks(body)
        yield self.tail

    async def async_chunks(self, body: AsyncBody) -> AsyncIterator[bytes]:
        yield self.head
        if isinstance(body, AsyncIterable):
            async for chunk in body:
                yield chunk
        else:
            for chunk in _sync_chunks(body):
                yield chunk
        yield self.tail


def _sync_chunks(body: Body) -> Iterator[bytes]:
    if isinstance(body, bytes):
        yield body
    elif isinstance(body, Readable):
        while chunk := body.read(CHUNK):
            yield chunk
    else:
        yield from body
