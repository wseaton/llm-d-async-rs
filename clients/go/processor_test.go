package asyncclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// upstream stands in for the inference gateway: JSON echoes the request's
// prompt; /v1/audio/speech answers with binary audio.
type upstream struct {
	*httptest.Server
	audio []byte
	seen  atomic.Int64
	body  atomic.Pointer[[]byte]
}

func newUpstream(t *testing.T) *upstream {
	u := &upstream{audio: pattern(3<<20, 251)}
	u.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.seen.Add(1)
		body, _ := io.ReadAll(r.Body)
		u.body.Store(&body)
		if r.URL.Path == "/v1/audio/speech" {
			w.Header().Set("Content-Type", "audio/mpeg")
			_, _ = w.Write(u.audio)
			return
		}
		var in struct {
			Prompt string `json:"prompt"`
		}
		_ = json.Unmarshal(body, &in)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"echo": in.Prompt})
	}))
	t.Cleanup(u.Close)
	return u
}

func pattern(n, mod int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % mod)
	}
	return b
}

// processor is the llm-d-async binary on fresh ports with its own queues.
type processor struct {
	api      string
	queue    string
	gated    string
	results  string
	budget   string
	postgres bool
}

func binary(t *testing.T) string {
	if bin := os.Getenv("LDA_BIN"); bin != "" {
		return bin
	}
	bin, err := filepath.Abs("../../target/debug/llm-d-async")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(bin); err != nil {
		t.Fatalf("processor binary not found at %s: run `cargo build` or set LDA_BIN", bin)
	}
	return bin
}

func freePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

func startProcessor(t *testing.T, up *upstream, postgresURL string) *processor {
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	p := &processor{
		queue:    "q-" + suffix,
		gated:    "gated-" + suffix,
		results:  "results-" + suffix,
		budget:   "budget-" + suffix,
		postgres: postgresURL != "",
	}
	transport := map[string]any{
		"poll_interval_ms":  50,
		"result_queue_name": p.results,
		"queues": []map[string]any{
			{"queue_name": p.queue, "igw_base_url": up.URL},
			{
				"queue_name":   p.gated,
				"igw_base_url": up.URL,
				"gate_type":    "budget-key",
				"gate_params":  map[string]any{"budget_key": p.budget},
			},
		},
	}
	dir := t.TempDir()
	raw, _ := json.Marshal(transport)
	cfg := filepath.Join(dir, "transport.json")
	if err := os.WriteFile(cfg, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 5; attempt++ {
		api, health, metrics := freePort(t), freePort(t), freePort(t)
		args := []string{
			"--data-dir", filepath.Join(dir, "data"),
			"--transport-config-file", cfg,
			"--api-addr", fmt.Sprintf("127.0.0.1:%d", api),
			"--health-port", fmt.Sprint(health),
			"--metrics-port", fmt.Sprint(metrics),
		}
		if p.postgres {
			args = append(args, "--store", "postgres", "--database-url", postgresURL,
				"--database-max-connections", "4", "--partition-lease-ttl", "3s")
		}
		log, err := os.Create(filepath.Join(dir, fmt.Sprintf("processor-%d.log", attempt)))
		if err != nil {
			t.Fatal(err)
		}
		cmd := exec.Command(binary(t), args...)
		cmd.Stdout, cmd.Stderr = log, log
		cmd.Env = append(os.Environ(), "RUST_LOG=warn", "OTEL_EXPORTER_OTLP_ENDPOINT=")
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		exited := make(chan struct{})
		go func() { _ = cmd.Wait(); close(exited) }()
		t.Cleanup(func() {
			_ = cmd.Process.Kill()
			<-exited
			if t.Failed() {
				logs, _ := os.ReadFile(log.Name())
				t.Logf("processor log:\n%s", logs)
			}
		})
		ready := fmt.Sprintf("http://127.0.0.1:%d/readyz", health)
		for until := time.Now().Add(30 * time.Second); time.Now().Before(until); {
			select {
			case <-exited:
				until = time.Time{}
				continue
			default:
			}
			if resp, err := http.Get(ready); err == nil {
				_ = resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					p.api = fmt.Sprintf("http://127.0.0.1:%d", api)
					return p
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	t.Fatal("processor never became ready")
	return nil
}

func (p *processor) client(t *testing.T, opts ...Option) *Client {
	c, err := New(p.api, append([]Option{WithRequestQueue(p.queue), WithResultQueue(p.results), WithPollWait(2 * time.Second)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func (p *processor) setBudget(t *testing.T, value string) {
	req, _ := http.NewRequest(http.MethodPut, p.api+"/v1/admin/budgets/"+p.budget, strings.NewReader(value))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode >= 300 {
		t.Fatalf("set budget: %d", resp.StatusCode)
	}
}

// stores runs fn against the embedded store, and against Postgres when
// TEST_DATABASE_URL is set (REQUIRE_POSTGRES makes it mandatory).
func stores(t *testing.T, fn func(t *testing.T, up *upstream, p *processor)) {
	t.Run("embedded", func(t *testing.T) {
		t.Parallel()
		up := newUpstream(t)
		fn(t, up, startProcessor(t, up, ""))
	})
	t.Run("postgres", func(t *testing.T) {
		t.Parallel()
		url := os.Getenv("TEST_DATABASE_URL")
		if url == "" {
			if os.Getenv("REQUIRE_POSTGRES") != "" {
				t.Fatal("REQUIRE_POSTGRES is set but TEST_DATABASE_URL is not")
			}
			t.Skip("TEST_DATABASE_URL not set")
		}
		up := newUpstream(t)
		fn(t, up, startProcessor(t, up, url))
	})
}

func deadline(d time.Duration) int64 { return time.Now().Add(d).Unix() }

func ctxFor(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func bytesEqual(a, b []byte) bool { return bytes.Equal(a, b) }
