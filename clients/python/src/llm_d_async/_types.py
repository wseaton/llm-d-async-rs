from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ErrorCode(str, Enum):
    """Why a request finished without an HTTP response."""

    DEADLINE_EXCEEDED = "DEADLINE_EXCEEDED"
    CANCELLED = "CANCELLED"
    GATE_DROPPED = "GATE_DROPPED"
    GATE_ERROR = "GATE_ERROR"
    INFERENCE_ERROR = "INFERENCE_ERROR"
    INVALID_REQUEST = "INVALID_REQUEST"
    PAYLOAD_UNAVAILABLE = "PAYLOAD_UNAVAILABLE"


@dataclass(frozen=True)
class Submission:
    """One request. `payload` is JSON-encoded and sent upstream as the
    request body (application/json); use `submit_stream` for other content
    types or large bodies. `deadline` and `created` are Unix seconds."""

    id: str
    deadline: int
    payload: Any = None
    created: int = 0
    metadata: Mapping[str, str] = field(default_factory=dict[str, str])
    headers: Mapping[str, str] = field(default_factory=dict[str, str])
    endpoint: str = ""
    model: str = ""
    request_queue: str = ""
    """Defaults to the client's request queue."""
    result_queue: str = ""
    """Defaults to the client's result queue."""


@dataclass(frozen=True)
class Submitted:
    """An accepted submission. `request_token` tells apart submissions that
    reuse an ID."""

    id: str
    request_token: str


@dataclass(frozen=True)
class Result:
    """A result as the processor sends it.

    `status_code > 0` means the gateway answered and `payload` is its body,
    unless the body was binary: then `payload_ref` is set, `payload` is
    empty, and `open_result_body` reads it. `payload_location` is the body's
    URL in the object store holding it, if it is in one. `status_code == 0`
    means no response, and `error_code` says why."""

    id: str
    status_code: int = 0
    payload: str = ""
    error_code: ErrorCode | None = None
    error_message: str = ""
    payload_ref: str = ""
    payload_location: str = ""
    content_type: str = ""
    payload_size: int = 0
    payload_sha256: str = ""
    request_token: str = ""

    @property
    def by_reference(self) -> bool:
        return bool(self.payload_ref)

    def json(self) -> Any:
        """The inline payload, decoded."""
        return json.loads(self.payload)


@dataclass(frozen=True)
class Delivery:
    """A leased result. It stays stored until acknowledged; if the lease
    lapses first it is delivered again."""

    result: Result
    claim_id: int
    owner_token: str = field(repr=False)
