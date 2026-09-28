package asyncclient

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/producer"
)

// startRedis runs a redis-server of the test's own (or REDIS_SERVER_BIN).
// Without it the test is skipped, unless REQUIRE_REDIS is set.
func startRedis(t *testing.T) string {
	bin := os.Getenv("REDIS_SERVER_BIN")
	if bin == "" {
		bin = "redis-server"
	}
	if _, err := exec.LookPath(bin); err != nil {
		if os.Getenv("REQUIRE_REDIS") != "" {
			t.Fatalf("REQUIRE_REDIS is set but %s is unavailable: %v", bin, err)
		}
		t.Skipf("%s unavailable: %v", bin, err)
	}
	port := freePort(t)
	cmd := exec.Command(bin, "--port", fmt.Sprint(port), "--bind", "127.0.0.1", "--save", "", "--appendonly", "no")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	for until := time.Now().Add(10 * time.Second); time.Now().Before(until); time.Sleep(20 * time.Millisecond) {
		if conn, err := net.Dial("tcp", addr); err == nil {
			_ = conn.Close()
			return "redis://" + addr
		}
	}
	t.Fatal("redis-server never listened")
	return ""
}

func goRequest(id, prompt string, deadline time.Duration) *api.RedisRequest {
	return &api.RedisRequest{RequestMessage: api.RequestMessage{
		ID:       id,
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(deadline).Unix(),
		Payload:  map[string]any{"prompt": prompt},
		Metadata: map[string]string{"userid": "acme"},
	}}
}

// Upstream's own Go producer, unchanged, drives this processor on the Redis
// store: the queues, results, leases and cancellation markers it writes and
// reads are upstream's.
func TestUpstreamGoProducerOnTheRedisStore(t *testing.T) {
	url := startRedis(t)
	up := newUpstream(t)
	p := startProcessor(t, up, redisStore(url))
	ctx := ctxFor(t, 60*time.Second)

	results := "go-results"
	prod, err := producer.NewRedisSortedSetProducer(producer.RedisSortedSetConfig{
		RedisURL:         url,
		RequestQueueName: p.queue,
		ResultQueueName:  results,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = prod.Close() })

	t.Run("submit and take the result", func(t *testing.T) {
		if err := prod.SubmitRequest(ctx, goRequest("go-1", "hello", time.Minute)); err != nil {
			t.Fatal(err)
		}
		r, err := prod.GetResult(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if r.ID != "go-1" || r.StatusCode != 200 {
			t.Fatalf("result %+v", r)
		}
		var body map[string]string
		if err := json.Unmarshal([]byte(r.Payload), &body); err != nil || body["echo"] != "hello" {
			t.Fatalf("payload %q: %v", r.Payload, err)
		}
	})

	t.Run("leased results are acknowledged once", func(t *testing.T) {
		if err := prod.SubmitRequest(ctx, goRequest("go-2", "leased", time.Minute)); err != nil {
			t.Fatal(err)
		}
		d, err := prod.ReceiveResult(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if d.Result.ID != "go-2" {
			t.Fatalf("result %+v", d.Result)
		}
		if err := prod.RenewResult(ctx, d); err != nil {
			t.Fatal(err)
		}
		if err := prod.AckResult(ctx, d); err != nil {
			t.Fatal(err)
		}
		if err := prod.AckResult(ctx, d); err != nil {
			t.Fatalf("a repeated ack: %v", err)
		}
		depth, err := prod.ResultQueueDepth(ctx)
		if err != nil || depth != 0 {
			t.Fatalf("depth %d: %v", depth, err)
		}
	})

	t.Run("cancelled before dispatch", func(t *testing.T) {
		gated, err := producer.NewRedisSortedSetProducer(producer.RedisSortedSetConfig{
			RedisURL:         url,
			RequestQueueName: p.gated,
			ResultQueueName:  results,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = gated.Close() })
		p.setBudget(t, "0")
		seen := up.seen.Load()
		if err := gated.SubmitRequest(ctx, goRequest("go-3", "never", time.Minute)); err != nil {
			t.Fatal(err)
		}
		if err := gated.CancelRequests(ctx, []string{"go-3"}); err != nil {
			t.Fatal(err)
		}
		p.setBudget(t, "1")
		r, err := gated.GetResult(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if r.ID != "go-3" || r.ErrorCode != "CANCELLED" {
			t.Fatalf("result %+v", r)
		}
		if up.seen.Load() != seen {
			t.Fatal("a cancelled request reached the gateway")
		}
	})

	t.Run("this client reads what the Go producer submitted", func(t *testing.T) {
		c := p.client(t, WithResultQueue(results))
		if err := prod.SubmitRequest(ctx, goRequest("go-4", "shared", time.Minute)); err != nil {
			t.Fatal(err)
		}
		d, err := c.ReceiveResult(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if d.Result.ID != "go-4" || d.Result.StatusCode != 200 {
			t.Fatalf("result %+v", d.Result)
		}
		if err := c.AckResult(ctx, d); err != nil {
			t.Fatal(err)
		}
	})
}
