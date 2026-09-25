package main

import (
	"encoding/json"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"strings"
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
	reply []byte
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

// text is n tokens of seeded random three-letter words, 4 bytes a token.
// Random letters keep Postgres from compressing bodies to nothing, as it
// would a repeated word.
func text(n int, seed uint64) string {
	r := rand.New(rand.NewPCG(seed, seed))
	var b strings.Builder
	b.Grow(4 * n)
	for range n {
		for range 3 {
			b.WriteByte(byte('a' + r.IntN(26)))
		}
		b.WriteByte(' ')
	}
	return b.String()
}

// completion is a text completion of osl tokens for a prompt of isl.
func completion(isl, osl int) []byte {
	body, _ := json.Marshal(map[string]any{ //nolint:errchkjson // Plain maps of strings and ints.
		"id":     "bench",
		"object": "text_completion",
		"choices": []map[string]any{{
			"index": 0, "text": text(osl, 2), "finish_reason": "stop",
		}},
		"usage": map[string]int{
			"prompt_tokens": isl, "completion_tokens": osl, "total_tokens": isl + osl,
		},
	})
	return body
}

func startGateway(delay time.Duration, reply []byte) (*gateway, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	g := &gateway{
		url:       "http://" + ln.Addr().String(),
		delay:     delay,
		reply:     reply,
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
	w.Write(g.reply) //nolint:errcheck // The processor sees a short body as an error.
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
