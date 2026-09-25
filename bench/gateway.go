package main

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// marker is what the gateway reads from each payload the load generator
// built.
type marker struct {
	BenchID string `json:"bench_id"`
	SentNs  int64  `json:"sent_ns"`
}

// gateway stands in for the inference gateway. It answers every request at
// once (or after delay) and records when each one arrived, so both processors
// are timed by the same clock outside them.
type gateway struct {
	url   string
	delay time.Duration
	srv   *http.Server
	hits  atomic.Int64

	mu         sync.Mutex
	seen       map[string]int
	duplicates int
	lagsMs     []float64
	first      time.Time
	last       time.Time
	perSecond  map[int64]int
}

var reply = []byte(`{"id":"bench","object":"text_completion","choices":[{"index":0,"text":"ok","finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`)

func startGateway(delay time.Duration) (*gateway, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	g := &gateway{
		url:       "http://" + ln.Addr().String(),
		delay:     delay,
		seen:      make(map[string]int),
		perSecond: make(map[int64]int),
	}
	g.srv = &http.Server{Handler: http.HandlerFunc(g.handle)}
	go g.srv.Serve(ln) //nolint:errcheck // Serve returns ErrServerClosed on Close.
	return g, nil
}

func (g *gateway) handle(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var m marker
	if err := json.Unmarshal(body, &m); err != nil || m.BenchID == "" {
		http.Error(w, "payload without bench_id", http.StatusBadRequest)
		return
	}
	g.hits.Add(1)
	g.mu.Lock()
	g.seen[m.BenchID]++
	if g.seen[m.BenchID] > 1 {
		g.duplicates++
	}
	if m.SentNs > 0 {
		g.lagsMs = append(g.lagsMs, float64(now.UnixNano()-m.SentNs)/1e6)
	}
	if g.first.IsZero() {
		g.first = now
	}
	g.last = now
	g.perSecond[now.Unix()]++
	g.mu.Unlock()

	if g.delay > 0 {
		time.Sleep(g.delay)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(reply) //nolint:errcheck // The processor sees a short body as an error.
}

// gatewayStats is a copy of what the gateway recorded.
type gatewayStats struct {
	Hits       int64
	Unique     int
	Duplicates int
	LagsMs     []float64
	First      time.Time
	Last       time.Time
	PerSecond  map[int64]int
}

func (g *gateway) stats() gatewayStats {
	g.mu.Lock()
	defer g.mu.Unlock()
	per := make(map[int64]int, len(g.perSecond))
	for k, v := range g.perSecond {
		per[k] = v
	}
	return gatewayStats{
		Hits:       g.hits.Load(),
		Unique:     len(g.seen),
		Duplicates: g.duplicates,
		LagsMs:     append([]float64(nil), g.lagsMs...),
		First:      g.first,
		Last:       g.last,
		PerSecond:  per,
	}
}

func (g *gateway) close() {
	g.srv.Close() //nolint:errcheck // Shutdown at the end of a run.
}
