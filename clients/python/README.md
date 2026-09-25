# llm-d-async-client

Python client for the llm-d-async processor's HTTP API, sync and asyncio, on
[httpx2](https://github.com/pydantic/httpx2). It mirrors the Go client in
[`../go`](../go).

```python
import time
from llm_d_async import Client, Submission

with Client("http://processor:8080", request_queue="batch", result_queue="my-results") as c:
    c.submit(
        Submission(
            id="r1",
            deadline=int(time.time()) + 3600,
            payload={"model": "m", "prompt": "hello", "max_tokens": 64},
        )
    )
    delivery = c.receive_result(timeout=600)  # leased; redelivered if the lease lapses
    print(delivery.result.status_code, delivery.result.json())
    c.ack(delivery)
```

`AsyncClient` has the same methods as coroutines. `submit_stream` sends a
body of any content type from bytes, a file or an (async) iterator without
buffering it; `open_result_body` streams a binary result stored by reference.
`pop_result` takes a result without a lease. Blocking waits take `timeout=`
and raise `TimeoutError`.

## Development

```sh
uv sync
uv run ruff format --check . && uv run ruff check . && uv run pyright
cargo build --manifest-path ../../Cargo.toml && uv run pytest
```

The tests run the real processor binary (`../../target/debug/llm-d-async`,
or `LDA_BIN`) against a stand-in gateway, on the embedded store and, with
`TEST_DATABASE_URL` set, on Postgres.
