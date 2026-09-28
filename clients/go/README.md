# llm-d-async Go client

A Go client for the [llm-d-async (Rust)](../../README.md) processor's HTTP API:
submit inference requests to its queues and read their results back.

```sh
go get github.com/wseaton/llm-d-async-rs/clients/go
```

```go
import asyncclient "github.com/wseaton/llm-d-async-rs/clients/go"
```

Like the processor it talks to, this client is experimental; see the notice in
the [main README](../../README.md).

## Quick start

```go
c, err := asyncclient.New("http://processor:8080",
	asyncclient.WithRequestQueue("batch"),
	asyncclient.WithResultQueue("my-results"),
)
if err != nil {
	return err
}
defer c.Close()

_, err = c.Submit(ctx, asyncclient.Submission{
	ID:       "r1",
	Deadline: time.Now().Add(time.Hour).Unix(),
	Endpoint: "/v1/completions",
	Payload:  json.RawMessage(`{"model":"m","prompt":"hello","max_tokens":64}`),
})
if err != nil {
	return err
}

d, err := c.ReceiveResult(ctx) // leased: redelivered if the lease lapses
if err != nil {
	return err
}
fmt.Println(d.Result.ID, d.Result.StatusCode, d.Result.Payload)
if err := c.AckResult(ctx, d); err != nil {
	return err
}
```

`Deadline` is Unix seconds; a request still queued at its deadline finishes
with `DEADLINE_EXCEEDED` instead of being sent. `Endpoint` is the gateway path
the payload goes to (default: the queue's `request_path_url`).

## Drop-in for the Redis producer

`*Client` satisfies `producer.Producer` from
[github.com/llm-d/llm-d-async](https://github.com/llm-d/llm-d-async), so code
written against the Go processor's Redis producer switches by construction
alone:

```go
var p producer.Producer
p, err = asyncclient.New("http://processor:8080",
	asyncclient.WithRequestQueue("batch"),
	asyncclient.WithResultQueue("my-results"),
)
```

| `producer.Producer` | here |
|---|---|
| `SubmitRequest(ctx, api.Request)` | converts with `FromRequest`; an `*api.RedisRequest`'s queue names carry over |
| `CancelRequests(ctx, ids)` | `Cancel`, which also reports how many were live |
| `GetResult(ctx)` | `PopResult`, which also returns by-reference fields |
| `Close()` | releases idle connections |

The processor's own queues replace Redis, so there is no Redis address to
configure; the URL is the processor's API port. With the Postgres store any
replica serves any call, so it can front all of them.

## Options

| option | default | |
|---|---|---|
| `WithRequestQueue(name)` | the processor's first queue | queue for submissions that name none |
| `WithResultQueue(name)` | `result-list` | route this client reads results from, and the route its submissions ask results to go to when they name none |
| `WithPollWait(d)` | 30s | each long poll for results (the server caps it at 60s) |
| `WithResultLease(d)` | 5m | how long `ReceiveResult` holds a result |
| `WithHTTPClient(hc)` | `http.DefaultClient` | its timeout must not cut long polls shorter than the poll wait |

A `Submission` can override both queues per request with `RequestQueue` and
`ResultQueue`.

## Reading results

Results go to a named route. Each result is delivered to one reader, so give
every consumer its own route.

- **Leased** (`ReceiveResult`, `RenewResult`, `AckResult`): the result stays
  stored until acknowledged. If the lease lapses first it is delivered again,
  and the late holder's `RenewResult` or `AckResult` fails with
  `ErrResultDeliveryOwnershipLost`. `AckResult` is idempotent. Use this when a
  lost result matters: checkpoint, then acknowledge.
- **Destructive** (`PopResult`, `GetResult`): the result is gone once read.

Both wait until a result arrives or `ctx` ends, polling for at most the poll
wait at a time, and return `ctx.Err()` wrapped when it ends.

`ResultQueueDepth` counts unleased results waiting on the client's route;
`QueueDepth(ctx, name)` counts requests waiting in a queue.

## Large and binary bodies

`Submit` sends `Payload` as JSON inline. `SubmitStream` sends a body of any
content type from an `io.Reader`, streamed to the processor without buffering:

```go
f, err := os.Open("speech-request.json")
if err != nil {
	return err
}
defer f.Close()
_, err = c.SubmitStream(ctx, asyncclient.Submission{
	ID:       "tts-1",
	Deadline: time.Now().Add(time.Hour).Unix(),
	Endpoint: "/v1/audio/speech",
}, "application/json", f)
```

A successful response with a binary media type (audio, images, PDFs,
archives and the like), or any successful body that is not UTF-8, is stored by
reference rather than inline: the result has `PayloadRef`,
`ContentType`, `PayloadSize` and `PayloadSHA256` set and an empty `Payload`.
Stream it with `OpenResultBody` before acknowledging the result, which deletes
the body:

```go
if d.Result.PayloadRef != "" {
	body, err := c.OpenResultBody(ctx, d.Result)
	if err != nil {
		return err
	}
	defer body.Close()
	_, err = io.Copy(out, body)
}
```

`PayloadLocation` is set when the body sits in an object store, for readers
that can copy it from there directly.

## Batches and cancellation

`SubmitBatch` enqueues a slice of submissions in one transaction: all or none.
`Cancel` marks requests cancelled; it only stops requests not yet dispatched,
and is idempotent.

## Errors

| error | when |
|---|---|
| `ErrResultDeliveryOwnershipLost` | a lease lapsed before `RenewResult` or `AckResult` |
| `ErrNotFound` | a result body was acknowledged or expired, or `QueueDepth` names no queue |
| `*StatusError` | any other refusal, with the HTTP status and the processor's message |

Check them with `errors.Is` and `errors.As`.

## Not covered yet

Results by request (`"result_delivery": "request"`, a result route per
submission) and `GET /v1/requests/{id}` status are processor features this
client does not expose; the admin endpoints (budgets, dispatch rates) are not
wrapped either. Use the [HTTP API](../../README.md#api) directly for those.

## Development

```sh
cargo build --manifest-path ../../Cargo.toml
test -z "$(gofmt -l .)" && go vet ./... && go test -race ./...
```

The tests run the real processor binary (`../../target/debug/llm-d-async`, or
`LDA_BIN`) against a stand-in gateway, on the embedded store and, with
`TEST_DATABASE_URL` set, on Postgres (`REQUIRE_POSTGRES=1` makes skipping a
failure).
