package asyncclient

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/producer"
)

var _ producer.Producer = (*Client)(nil)

func TestNewRejectsBadConfig(t *testing.T) {
	for _, url := range []string{"", "ftp://x", "::"} {
		if _, err := New(url); err == nil {
			t.Errorf("New(%q) accepted", url)
		}
	}
	if _, err := New("http://x", WithResultQueue("")); err == nil {
		t.Error("empty result queue accepted")
	}
	if _, err := New("http://x", WithPollWait(0)); err == nil {
		t.Error("zero poll wait accepted")
	}
}

func TestSubmitRequestAndGetResult(t *testing.T) {
	stores(t, func(t *testing.T, up *upstream, p *processor) {
		c := p.client(t)
		ctx := ctxFor(t, 30*time.Second)
		req := &api.RedisRequest{
			RequestMessage: api.RequestMessage{
				ID:       "a",
				Created:  time.Now().Unix(),
				Deadline: deadline(time.Minute),
				Payload:  map[string]any{"model": "m", "prompt": "hello"},
				Metadata: map[string]string{"k": "v"},
			},
			RequestQueueName: p.queue,
		}
		if err := c.SubmitRequest(ctx, req); err != nil {
			t.Fatal(err)
		}
		r, err := c.GetResult(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if r.ID != "a" || r.StatusCode != 200 || r.Payload != `{"echo":"hello"}`+"\n" {
			t.Fatalf("result %+v", r)
		}
	})
}

func TestSubmitBatchAndLeasedDelivery(t *testing.T) {
	stores(t, func(t *testing.T, up *upstream, p *processor) {
		c := p.client(t)
		ctx := ctxFor(t, 30*time.Second)
		var subs []Submission
		for _, id := range []string{"x", "y", "z"} {
			subs = append(subs, Submission{
				ID: id, Deadline: deadline(time.Minute), RequestQueue: p.queue,
				Payload: json.RawMessage(`{"prompt":"` + id + `"}`),
			})
		}
		done, err := c.SubmitBatch(ctx, subs)
		if err != nil {
			t.Fatal(err)
		}
		if len(done) != 3 || done[0].ID != "x" || done[0].RequestToken == "" {
			t.Fatalf("submitted %+v", done)
		}
		seen := map[string]bool{}
		for range 3 {
			d, err := c.ReceiveResult(ctx)
			if err != nil {
				t.Fatal(err)
			}
			seen[d.Result.ID] = true
			if err := c.RenewResult(ctx, d); err != nil {
				t.Fatal(err)
			}
			if err := c.AckResult(ctx, d); err != nil {
				t.Fatal(err)
			}
			if err := c.AckResult(ctx, d); err != nil {
				t.Fatalf("repeated ack: %v", err)
			}
		}
		if len(seen) != 3 {
			t.Fatalf("results %v", seen)
		}
		if n, err := c.ResultQueueDepth(ctx); err != nil || n != 0 {
			t.Fatalf("result depth %d, %v", n, err)
		}
	})
}

func TestLapsedLeaseIsRedelivered(t *testing.T) {
	stores(t, func(t *testing.T, up *upstream, p *processor) {
		short := p.client(t, WithResultLease(300*time.Millisecond))
		c := p.client(t)
		ctx := ctxFor(t, 30*time.Second)
		if _, err := c.Submit(ctx, Submission{ID: "l", Deadline: deadline(time.Minute), RequestQueue: p.queue}); err != nil {
			t.Fatal(err)
		}
		lost, err := short.ReceiveResult(ctx)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(600 * time.Millisecond)
		again, err := c.ReceiveResult(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if again.ClaimID != lost.ClaimID || again.Result.ID != "l" {
			t.Fatalf("redelivered %+v, first %+v", again, lost)
		}
		for _, err := range []error{short.RenewResult(ctx, lost), short.AckResult(ctx, lost)} {
			if !errors.Is(err, ErrResultDeliveryOwnershipLost) {
				t.Fatalf("stale owner got %v", err)
			}
		}
		if err := c.AckResult(ctx, again); err != nil {
			t.Fatal(err)
		}
	})
}

func TestStreamedBodyAndResultByReference(t *testing.T) {
	stores(t, func(t *testing.T, up *upstream, p *processor) {
		c := p.client(t)
		ctx := ctxFor(t, 60*time.Second)
		payload := pattern(5<<20, 241)
		sub := Submission{ID: "tts", Deadline: deadline(time.Minute), RequestQueue: p.queue, Endpoint: "/v1/audio/speech"}
		pr, pw := io.Pipe()
		go func() {
			for off := 0; off < len(payload); off += 64 << 10 {
				if _, err := pw.Write(payload[off:min(off+64<<10, len(payload))]); err != nil {
					return
				}
			}
			_ = pw.Close()
		}()
		if _, err := c.SubmitStream(ctx, sub, "audio/wav", pr); err != nil {
			t.Fatal(err)
		}
		if _, err := c.SubmitStream(ctx, Submission{ID: "bad", Payload: json.RawMessage(`1`)}, "x/y", nil); err == nil {
			t.Fatal("a streamed submission with an inline payload was accepted")
		}
		d, err := c.ReceiveResult(ctx)
		if err != nil {
			t.Fatal(err)
		}
		r := d.Result
		sum := sha256.Sum256(up.audio)
		if r.RequestToken == "" {
			t.Fatal("result carries no request token")
		}
		if p.objectBlobs != strings.HasPrefix(r.PayloadLocation, "file://") {
			t.Fatalf("location %q: want an object store URL exactly when the processor uses one", r.PayloadLocation)
		}
		if r.PayloadRef == "" || r.ContentType != "audio/mpeg" || r.PayloadSize != int64(len(up.audio)) ||
			r.PayloadSHA256 != hex.EncodeToString(sum[:]) || r.Payload != "" {
			t.Fatalf("result %+v", r)
		}
		if got := up.body.Load(); got == nil || !bytesEqual(*got, payload) {
			t.Fatal("payload changed on its way upstream")
		}
		body, err := c.OpenResultBody(ctx, r)
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(body)
		_ = body.Close()
		if err != nil || !bytesEqual(got, up.audio) || body.ContentType != "audio/mpeg" || body.Size != int64(len(up.audio)) {
			t.Fatalf("body: %d bytes, %q, size %d, %v", len(got), body.ContentType, body.Size, err)
		}
		if err := c.AckResult(ctx, d); err != nil {
			t.Fatal(err)
		}
		if _, err := c.OpenResultBody(ctx, r); !errors.Is(err, ErrNotFound) {
			t.Fatalf("body after ack: %v", err)
		}
		if _, err := c.OpenResultBody(ctx, &Result{}); err == nil {
			t.Fatal("opened a result without a reference")
		}
	})
}

func TestCancelBeforeDispatch(t *testing.T) {
	stores(t, func(t *testing.T, up *upstream, p *processor) {
		c := p.client(t)
		ctx := ctxFor(t, 30*time.Second)
		p.setBudget(t, "0")
		if _, err := c.Submit(ctx, Submission{ID: "c", Deadline: deadline(time.Minute), RequestQueue: p.gated}); err != nil {
			t.Fatal(err)
		}
		for {
			n, err := c.QueueDepth(ctx, p.gated)
			if err != nil {
				t.Fatal(err)
			}
			if n == 1 {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		n, err := c.Cancel(ctx, []string{"c", "unknown"})
		if err != nil || n != 1 {
			t.Fatalf("cancelled %d, %v", n, err)
		}
		if err := c.CancelRequests(ctx, []string{"c"}); err != nil {
			t.Fatal(err)
		}
		p.setBudget(t, "1")
		r, err := c.PopResult(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if r.ID != "c" || r.ErrorCode != api.ErrCodeCancelled {
			t.Fatalf("result %+v", r)
		}
		if up.seen.Load() != 0 {
			t.Fatal("a cancelled request reached the gateway")
		}
		if _, err := c.QueueDepth(ctx, "no-such-queue"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("unknown queue: %v", err)
		}
	})
}

// Mirrors the batch gateway: a plain api.RequestMessage, with queues coming
// from the client, and two consumers that must not see each other's results.
func TestQueuesComeFromTheClient(t *testing.T) {
	stores(t, func(t *testing.T, up *upstream, p *processor) {
		mine := p.client(t)
		theirs := p.client(t, WithResultQueue(p.results+"-other"))
		ctx := ctxFor(t, 30*time.Second)
		for _, c := range []*Client{mine, theirs} {
			req := &api.RequestMessage{ID: c.resultQueue, Deadline: deadline(time.Minute), Payload: map[string]any{"prompt": "p"}}
			if err := c.SubmitRequest(ctx, req); err != nil {
				t.Fatal(err)
			}
		}
		for _, c := range []*Client{mine, theirs} {
			r, err := c.GetResult(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if r.ID != c.resultQueue {
				t.Fatalf("%s read %s's result", c.resultQueue, r.ID)
			}
		}
		if n, err := mine.QueueDepth(ctx, p.gated); err != nil || n != 0 {
			t.Fatalf("gated queue depth %d, %v: requests strayed from the client's queue", n, err)
		}
		stream := p.client(t, WithResultQueue(p.results+"-stream"))
		if _, err := stream.SubmitStream(ctx, Submission{ID: "s", Deadline: deadline(time.Minute)}, "text/plain", strings.NewReader("x")); err != nil {
			t.Fatal(err)
		}
		if r, err := stream.PopResult(ctx); err != nil || r.ID != "s" {
			t.Fatalf("streamed result %+v, %v", r, err)
		}
	})
}

func TestWaitingForResultsHonorsTheContext(t *testing.T) {
	stores(t, func(t *testing.T, up *upstream, p *processor) {
		c := p.client(t)
		for _, wait := range []func(context.Context) error{
			func(ctx context.Context) error { _, err := c.GetResult(ctx); return err },
			func(ctx context.Context) error { _, err := c.ReceiveResult(ctx); return err },
		} {
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			started := time.Now()
			err := wait(ctx)
			cancel()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("got %v", err)
			}
			if took := time.Since(started); took > 2*time.Second {
				t.Fatalf("returned after %v", took)
			}
		}
	})
}

func TestRefusalsCarryTheStatus(t *testing.T) {
	stores(t, func(t *testing.T, up *upstream, p *processor) {
		c := p.client(t)
		_, err := c.Submit(ctxFor(t, 10*time.Second), Submission{ID: "old", Deadline: 1, RequestQueue: p.queue})
		var status *StatusError
		if !errors.As(err, &status) || status.Status != 400 || status.Message == "" {
			t.Fatalf("got %v", err)
		}
	})
}
