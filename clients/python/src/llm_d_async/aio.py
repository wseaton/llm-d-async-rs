from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from types import TracebackType
from typing import Any

import httpx2

from llm_d_async import _core
from llm_d_async._core import AsyncBody, Multipart, Settings
from llm_d_async._errors import LlmDAsyncError
from llm_d_async._types import Delivery, Result, Submission, Submitted


class AsyncResultBody:
    """A result body stored by reference, streamed from the processor.
    Close it, or use it as an async context manager."""

    def __init__(self, response: httpx2.Response) -> None:
        self._response = response
        self.content_type: str = response.headers.get("content-type", "")
        length = response.headers.get("content-length")
        self.size: int | None = int(length) if length is not None else None

    def aiter_bytes(self, chunk_size: int = _core.CHUNK) -> AsyncIterator[bytes]:
        return self._response.aiter_bytes(chunk_size)

    async def read(self) -> bytes:
        return await self._response.aread()

    async def aclose(self) -> None:
        await self._response.aclose()

    async def __aenter__(self) -> AsyncResultBody:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()


class AsyncClient:
    """The asyncio form of `Client`. Talks to one processor URL. With the Postgres store any replica
    serves any call, so the URL can front all of them.

    `result_queue` is the route this client reads results from, and the
    route its submissions ask results to go to when they name none. Give
    each consumer its own route: a result read by one is gone for the
    others. Blocking waits long-poll for up to `poll_wait` seconds at a
    time, and `receive_result` leases for `lease` seconds."""

    def __init__(
        self,
        base_url: str,
        *,
        request_queue: str = "",
        result_queue: str = _core.DEFAULT_RESULT_QUEUE,
        poll_wait: float = _core.DEFAULT_POLL_WAIT,
        lease: float = _core.DEFAULT_LEASE,
        http: httpx2.AsyncClient | None = None,
    ) -> None:
        self._settings = Settings(base_url, request_queue, result_queue, poll_wait, lease)
        self._owns_http = http is None
        self._http = http or httpx2.AsyncClient()
        self._timeout = self._settings.timeout()

    @property
    def result_queue(self) -> str:
        return self._settings.result_queue

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def __aenter__(self) -> AsyncClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        params: dict[str, int] | None = None,
        content: AsyncIterator[bytes] | None = None,
        content_type: str | None = None,
    ) -> Any | None:
        headers = {"content-type": content_type} if content_type else None
        try:
            response = await self._http.request(
                method,
                self._settings.base + path,
                json=json,
                params=params,
                content=content,
                headers=headers,
                timeout=self._timeout,
            )
        except httpx2.HTTPError as e:
            raise LlmDAsyncError(f"{method} {path}: {e}") from e
        return _core.decode(response)

    async def submit(self, submission: Submission) -> Submitted:
        raw = await self._request(
            "POST", "/v1/requests", json=self._settings.submission(submission)
        )
        return _core.submitted(raw)

    async def submit_batch(self, submissions: Sequence[Submission]) -> list[Submitted]:
        """Enqueues all of `submissions` in one transaction, or none."""
        body = [self._settings.submission(s) for s in submissions]
        raw = await self._request("POST", "/v1/requests/batch", json=body)
        return _core.submitted_batch(raw)

    async def submit_stream(
        self, submission: Submission, content_type: str, body: AsyncBody
    ) -> Submitted:
        """Enqueues `submission` with `body` as its payload, streamed without
        buffering and sent upstream with `content_type`."""
        form = Multipart(self._settings.submission(submission), content_type)
        raw = await self._request(
            "POST",
            "/v1/requests",
            content=form.async_chunks(body),
            content_type=form.content_type,
        )
        return _core.submitted(raw)

    async def cancel(self, ids: Sequence[str]) -> int:
        """Marks requests cancelled before dispatch, best effort. Returns how
        many IDs had a live request to cancel."""
        raw = await self._request("POST", "/v1/requests/cancel", json={"ids": list(ids)})
        return _core.field_int(raw, "cancelled")

    async def pop_result(self, timeout: float | None = None) -> Result:
        """Takes the next result for good, waiting up to `timeout` seconds
        (forever when `None`). Raises `TimeoutError` when none arrives."""
        deadline = _core.deadline_after(timeout)
        while True:
            params = {"wait_ms": _core.wait_ms(self._settings.poll_wait, deadline)}
            raw = await self._request("POST", self._settings.result_path("/pop"), params=params)
            if raw is not None:
                return _core.result(raw)
            if _core.expired(deadline):
                raise TimeoutError(f"no result on {self.result_queue!r} within {timeout}s")

    async def receive_result(self, timeout: float | None = None) -> Delivery:
        """Leases the next result, waiting up to `timeout` seconds (forever
        when `None`). Checkpoint it, then `ack` it."""
        deadline = _core.deadline_after(timeout)
        while True:
            params = self._settings.claim_params(deadline)
            raw = await self._request("POST", self._settings.result_path("/claims"), params=params)
            if raw is not None:
                return _core.delivery(raw)
            if _core.expired(deadline):
                raise TimeoutError(f"no result on {self.result_queue!r} within {timeout}s")

    async def renew(self, delivery: Delivery) -> None:
        """Extends the lease. Raises `OwnershipLostError` once it lapsed."""
        path = self._settings.claim_path(delivery, "renew")
        await self._request("POST", path, json=self._settings.renewal(delivery))

    async def ack(self, delivery: Delivery) -> None:
        """Deletes a leased result and its body. Repeating it is safe."""
        path = self._settings.claim_path(delivery, "ack")
        await self._request("POST", path, json={"owner_token": delivery.owner_token})

    async def open_result_body(self, result: Result) -> AsyncResultBody:
        """Streams the body of a result stored by reference. Raises
        `NotFoundError` once the result is acknowledged or expired."""
        request = self._http.build_request(
            "GET", self._settings.base + _core.blob_path(result), timeout=self._timeout
        )
        try:
            response = await self._http.send(request, stream=True)
        except httpx2.HTTPError as e:
            raise LlmDAsyncError(f"open body of {result.id!r}: {e}") from e
        if response.status_code != 200:
            body = await response.aread()
            await response.aclose()
            raise _core.status_error(response.status_code, body)
        return AsyncResultBody(response)

    async def queue_depth(self, name: str) -> int:
        """Requests waiting in queue `name`."""
        return _core.queue_depth(await self._request("GET", "/v1/queues"), name)

    async def result_queue_depth(self) -> int:
        """Unleased results waiting on this client's result queue."""
        raw = await self._request("GET", self._settings.result_path("/depth"))
        return _core.field_int(raw, "depth")
