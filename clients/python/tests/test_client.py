from __future__ import annotations

import hashlib
import time
from collections.abc import AsyncIterator, Iterator

import pytest
from anyio.from_thread import start_blocking_portal

from llm_d_async import (
    AsyncClient,
    Client,
    ErrorCode,
    LlmDAsyncError,
    NotFoundError,
    OwnershipLostError,
    Result,
    StatusError,
    Submission,
)
from tests.conftest import Clients, Processor, pattern


def deadline(seconds: float = 60) -> int:
    return int(time.time() + seconds)


@pytest.mark.parametrize("make", [Client, AsyncClient])
def test_rejects_bad_config(make: type[Client] | type[AsyncClient]) -> None:
    for url in ["", "ftp://x", "::", "http://"]:
        with pytest.raises(ValueError):
            make(url)
    for bad in [
        {"result_queue": ""},
        {"poll_wait": 0.0},
        {"poll_wait": 61.0},
        {"lease": 0.0},
    ]:
        with pytest.raises(ValueError):
            make("http://x", **bad)  # pyright: ignore[reportArgumentType]


def test_submit_and_pop(clients: Clients, processor: Processor) -> None:
    c = clients()
    s = c.submit(
        Submission(
            id="a",
            created=int(time.time()),
            deadline=deadline(),
            payload={"model": "m", "prompt": "hello"},
            metadata={"k": "v"},
        )
    )
    assert s.id == "a" and s.request_token
    r = c.pop_result(timeout=30)
    assert (r.id, r.status_code, r.error_code) == ("a", 200, None)
    assert r.json() == {"echo": "hello"}
    assert r.request_token == s.request_token and not r.by_reference


def test_batch_and_leased_delivery(clients: Clients, processor: Processor) -> None:
    c = clients()
    done = c.submit_batch(
        [Submission(id=i, deadline=deadline(), payload={"prompt": i}) for i in "xyz"]
    )
    assert [d.id for d in done] == ["x", "y", "z"] and all(d.request_token for d in done)
    seen: set[str] = set()
    for _ in range(3):
        d = c.receive_result(timeout=30)
        seen.add(d.result.id)
        assert d.result.json() == {"echo": d.result.id}
        c.renew(d)
        c.ack(d)
        c.ack(d)
    assert seen == {"x", "y", "z"}
    assert c.result_queue_depth() == 0


def test_batch_is_all_or_nothing(clients: Clients, processor: Processor) -> None:
    c = clients()
    with pytest.raises(StatusError) as e:
        c.submit_batch(
            [
                Submission(id="ok", deadline=deadline(), payload={"prompt": "p"}),
                Submission(id="old", deadline=1, payload={"prompt": "p"}),
            ]
        )
    assert e.value.status == 400
    with pytest.raises(TimeoutError):
        c.pop_result(timeout=1)


def test_lapsed_lease_is_redelivered(clients: Clients, processor: Processor) -> None:
    short = clients(lease=0.3)
    c = clients()
    c.submit(Submission(id="l", deadline=deadline()))
    lost = short.receive_result(timeout=30)
    time.sleep(0.6)
    again = c.receive_result(timeout=30)
    assert again.claim_id == lost.claim_id and again.result.id == "l"
    with pytest.raises(OwnershipLostError):
        short.renew(lost)
    with pytest.raises(OwnershipLostError):
        short.ack(lost)
    c.ack(again)


def test_cancel_before_dispatch(clients: Clients, processor: Processor) -> None:
    c = clients()
    processor.set_budget("0")
    c.submit(Submission(id="c", deadline=deadline(), request_queue=processor.gated))
    until = time.monotonic() + 30
    while c.queue_depth(processor.gated) != 1:
        assert time.monotonic() < until, "request never queued"
        time.sleep(0.05)
    assert c.cancel(["c", "unknown"]) == 1
    c.cancel(["c"])
    processor.set_budget("1")
    r = c.pop_result(timeout=30)
    assert (r.id, r.status_code, r.error_code) == ("c", 0, ErrorCode.CANCELLED)
    assert processor.upstream.seen == 0
    with pytest.raises(NotFoundError):
        c.queue_depth("no-such-queue")


def test_queues_come_from_the_client(clients: Clients, processor: Processor) -> None:
    mine = clients()
    theirs = clients(result_queue=processor.results + "-other")
    for c in (mine, theirs):
        c.submit(Submission(id=c.result_queue, deadline=deadline(), payload={"prompt": "p"}))
    for c in (mine, theirs):
        assert c.pop_result(timeout=30).id == c.result_queue
    assert mine.queue_depth(processor.gated) == 0


def test_waiting_honors_the_timeout(clients: Clients, processor: Processor) -> None:
    c = clients()
    for wait in (c.pop_result, c.receive_result):
        started = time.monotonic()
        with pytest.raises(TimeoutError):
            wait(0.3)
        assert time.monotonic() - started < 2


def test_refusals_carry_the_status(clients: Clients, processor: Processor) -> None:
    with pytest.raises(StatusError) as e:
        clients().submit(Submission(id="old", deadline=1))
    assert e.value.status == 400 and e.value.message


PAYLOAD = pattern(5 << 20, 241)
SPEECH = Submission(id="tts", deadline=deadline(600), endpoint="/v1/audio/speech")


def pieces() -> Iterator[bytes]:
    for off in range(0, len(PAYLOAD), 64 << 10):
        yield PAYLOAD[off : off + (64 << 10)]


def check_by_reference(r: Result, processor: Processor) -> None:
    audio = processor.upstream.audio
    assert r.request_token
    assert processor.postgres == r.payload_location.startswith("file://")
    assert r.by_reference and r.payload == ""
    assert (r.content_type, r.payload_size) == ("audio/mpeg", len(audio))
    assert r.payload_sha256 == hashlib.sha256(audio).hexdigest()
    assert processor.upstream.body == PAYLOAD, "payload changed on its way upstream"


def test_streamed_body_and_result_by_reference(processor: Processor) -> None:
    with Client(processor.api, request_queue=processor.queue, result_queue=processor.results) as c:
        c.submit_stream(SPEECH, "audio/wav", pieces())
        with pytest.raises(ValueError):
            c.submit_stream(Submission(id="bad", deadline=deadline(), payload=1), "x/y", b"")
        with pytest.raises(ValueError):
            c.submit_stream(Submission(id="bad", deadline=deadline()), "x/y\r\nEvil: 1", b"")
        d = c.receive_result(timeout=60)
        check_by_reference(d.result, processor)
        with c.open_result_body(d.result) as body:
            assert (body.content_type, body.size) == ("audio/mpeg", len(processor.upstream.audio))
            assert b"".join(body.iter_bytes()) == processor.upstream.audio
        c.ack(d)
        with pytest.raises(NotFoundError):
            c.open_result_body(d.result)
        with pytest.raises(LlmDAsyncError):
            c.open_result_body(Result(id="none"))


def test_async_streamed_body_and_result_by_reference(processor: Processor) -> None:
    async def apieces() -> AsyncIterator[bytes]:
        for piece in pieces():
            yield piece

    async def scenario() -> None:
        async with AsyncClient(
            processor.api, request_queue=processor.queue, result_queue=processor.results
        ) as c:
            await c.submit_stream(SPEECH, "audio/wav", apieces())
            d = await c.receive_result(timeout=60)
            check_by_reference(d.result, processor)
            async with await c.open_result_body(d.result) as body:
                assert body.content_type == "audio/mpeg"
                got = b"".join([chunk async for chunk in body.aiter_bytes()])
                assert got == processor.upstream.audio
            await c.ack(d)
            with pytest.raises(NotFoundError):
                await c.open_result_body(d.result)
            await c.submit_stream(
                Submission(id="file", deadline=deadline()), "text/plain", b"from bytes"
            )
            r = await c.pop_result(timeout=30)
            assert r.id == "file" and processor.upstream.body == b"from bytes"

    with start_blocking_portal() as portal:
        portal.call(scenario)
