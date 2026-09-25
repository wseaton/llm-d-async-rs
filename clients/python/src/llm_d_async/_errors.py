from __future__ import annotations


class LlmDAsyncError(Exception):
    """Any failure talking to the processor."""


class StatusError(LlmDAsyncError):
    """A response the processor refused."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(f"llm-d-async: {status}: {message}")
        self.status = status
        self.message = message


class OwnershipLostError(StatusError):
    """A leased result is no longer held by this consumer: its lease lapsed
    and it may have been delivered again."""


class NotFoundError(StatusError):
    """The processor has no such result body or queue."""
