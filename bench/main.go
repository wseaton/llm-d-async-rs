// Command bench measures how fast the Rust and Go llm-d-async processors
// dispatch requests from Postgres to an inference gateway.
//
// Both processors run against the same Postgres, send to the same stand-in
// gateway (which times every arrival), and are configured alike. A drain run
// preloads -n requests and times how long cold processors take to dispatch
// them all; a rate run submits at a fixed -rate and records dispatch lag.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
)

type options struct {
	impl           implKind
	mode           string
	databaseURL    string
	n              int
	rate           int
	duration       time.Duration
	queues         int
	replicas       int
	batchSize      int
	pollIntervalMs int
	concurrency    int
	isl            int
	osl            int
	gatewayDelay   time.Duration
	timeout        time.Duration
	binRust        string
	binGo          string
	out            string
	keep           bool
}

func parseOptions() (options, error) {
	var o options
	var impl string
	here := binDir()
	flag.StringVar(&impl, "impl", "rust", "processor under test: rust or go")
	flag.StringVar(&o.mode, "mode", "drain", "drain (preload -n, time dispatching them) or rate (submit at -rate for -duration)")
	flag.StringVar(&o.databaseURL, "database-url", os.Getenv("BENCH_DATABASE_URL"), "maintenance database URL; each run creates its own database next to it")
	flag.IntVar(&o.n, "n", 20000, "drain: requests to preload")
	flag.IntVar(&o.rate, "rate", 1000, "rate: requests submitted per second")
	flag.DurationVar(&o.duration, "duration", 30*time.Second, "rate: how long to submit")
	flag.IntVar(&o.queues, "queues", 1, "request queues")
	flag.IntVar(&o.replicas, "replicas", 1, "processor replicas sharing the database")
	flag.IntVar(&o.batchSize, "batch-size", 100, "requests a queue claims per poll")
	flag.IntVar(&o.pollIntervalMs, "poll-interval-ms", 50, "how often each queue polls")
	flag.IntVar(&o.concurrency, "concurrency", 256, "workers per replica")
	flag.IntVar(&o.isl, "isl", 256, "input sequence length: prompt tokens per request (4 bytes each)")
	flag.IntVar(&o.osl, "osl", 16, "output sequence length: completion tokens per response (4 bytes each)")
	flag.DurationVar(&o.gatewayDelay, "gateway-delay", 0, "how long the gateway takes to answer")
	flag.DurationVar(&o.timeout, "timeout", 10*time.Minute, "give up on a run after this long")
	flag.StringVar(&o.binRust, "bin-rust", filepath.Join(here, "llm-d-async-rs"), "Rust processor binary")
	flag.StringVar(&o.binGo, "bin-go", filepath.Join(here, "llm-d-async-go"), "Go processor binary")
	flag.StringVar(&o.out, "out", "runs/results.jsonl", "append each run's report here as one JSON line")
	flag.BoolVar(&o.keep, "keep", false, "keep the run's database afterwards")
	flag.Parse()

	var err error
	if o.impl, err = parseImpl(impl); err != nil {
		return o, err
	}
	if o.databaseURL == "" {
		return o, errors.New("-database-url or BENCH_DATABASE_URL is required")
	}
	if o.mode != "drain" && o.mode != "rate" {
		return o, fmt.Errorf("unknown -mode %q", o.mode)
	}
	if o.queues < 1 || o.replicas < 1 || o.batchSize < 1 || o.pollIntervalMs < 1 || o.concurrency < 1 {
		return o, errors.New("-queues, -replicas, -batch-size, -poll-interval-ms and -concurrency must be positive")
	}
	return o, nil
}

// binDir is where setup.sh puts the binaries: .bin next to this command.
func binDir() string {
	exe, err := os.Executable()
	if err != nil {
		return ".bin"
	}
	return filepath.Dir(exe)
}

func (o options) bin() string {
	if o.impl == implRust {
		return o.binRust
	}
	return o.binGo
}

func (o options) queueNames() []string {
	names := make([]string, o.queues)
	for i := range names {
		names[i] = fmt.Sprintf("q%d", i)
	}
	return names
}

// report is one run's configuration and results.
type report struct {
	Impl           implKind `json:"impl"`
	Mode           string   `json:"mode"`
	Started        string   `json:"started"`
	Queues         int      `json:"queues"`
	Replicas       int      `json:"replicas"`
	BatchSize      int      `json:"batch_size"`
	PollIntervalMs int      `json:"poll_interval_ms"`
	Concurrency    int      `json:"concurrency"`
	ISL            int      `json:"isl"`
	OSL            int      `json:"osl"`
	GatewayDelayMs float64  `json:"gateway_delay_ms"`
	// ConfigCeiling is the rate the poll settings allow at most:
	// queues x replicas x batch / interval.
	ConfigCeiling float64 `json:"config_ceiling_per_s"`

	Requests   int64 `json:"requests"`
	Dispatched int64 `json:"dispatched"`
	Duplicates int   `json:"duplicates"`
	Results    int64 `json:"results"`

	// Drain: cold start to the last result, and first to last dispatch.
	TotalSeconds    float64 `json:"total_s,omitempty"`
	DispatchSeconds float64 `json:"dispatch_s"`
	DispatchRate    float64 `json:"dispatch_per_s"`
	PeakSecond      int     `json:"peak_second_per_s"`

	// Rate: what was offered and how long requests waited to dispatch.
	OfferedRate float64   `json:"offered_per_s,omitempty"`
	LateTicks   int64     `json:"late_ticks,omitempty"`
	FailedSubs  int64     `json:"failed_submits,omitempty"`
	LagMs       *quantile `json:"dispatch_lag_ms,omitempty"`
	// ResultLatencyMs runs from sending a request to a producer reading
	// its result.
	ResultLatencyMs  *quantile `json:"result_latency_ms,omitempty"`
	ResultDuplicates int64     `json:"result_duplicates,omitempty"`

	DB              perRequestDB `json:"db"`
	CPUSecs         float64      `json:"cpu_s"`
	CPUMsPerRequest float64      `json:"cpu_ms_per_request"`
	MaxRSSMiB       float64      `json:"max_rss_mib"`
	PerSecond       []int        `json:"per_second"`
}

type quantile struct {
	P50 float64 `json:"p50"`
	P90 float64 `json:"p90"`
	P99 float64 `json:"p99"`
	Max float64 `json:"max"`
}

func quantiles(xs []float64) *quantile {
	if len(xs) == 0 {
		return nil
	}
	slices.Sort(xs)
	at := func(q float64) float64 { return xs[min(len(xs)-1, int(q*float64(len(xs))))] }
	return &quantile{P50: at(0.50), P90: at(0.90), P99: at(0.99), Max: xs[len(xs)-1]}
}

// perRequestDB is the database's work divided by requests dispatched.
type perRequestDB struct {
	Transactions float64 `json:"xacts"`
	Rollbacks    float64 `json:"rollbacks"`
	RowsWritten  float64 `json:"rows_written"`
	RowsFetched  float64 `json:"rows_fetched"`
	WALBytes     float64 `json:"wal_bytes"`
	Statements   float64 `json:"statements,omitempty"`
	ExecMs       float64 `json:"exec_ms,omitempty"`
}

func perRequest(d dbStats, n int64) perRequestDB {
	if n == 0 {
		return perRequestDB{}
	}
	f := float64(n)
	p := perRequestDB{
		Transactions: float64(d.XactCommit) / f,
		Rollbacks:    float64(d.XactRollback) / f,
		RowsWritten:  float64(d.TupInserted+d.TupUpdated+d.TupDeleted) / f,
		RowsFetched:  float64(d.TupFetched) / f,
		WALBytes:     d.WALBytes / f,
	}
	if d.StatementOK {
		p.Statements, p.ExecMs = float64(d.Calls)/f, d.ExecMs/f
	}
	return p
}

func main() {
	if len(os.Args) == 3 && os.Args[1] == "summarize" {
		if err := summarize(os.Args[2], os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "bench:", err)
			os.Exit(1)
		}
		return
	}
	o, err := parseOptions()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	r, err := run(ctx, o)
	if err != nil {
		fmt.Fprintln(os.Stderr, "bench:", err)
		os.Exit(1)
	}
	printReport(r)
	if err := appendReport(o.out, r); err != nil {
		fmt.Fprintln(os.Stderr, "bench: write report:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, o options) (report, error) {
	started := time.Now()
	r := report{
		Impl: o.impl, Mode: o.mode, Started: started.UTC().Format(time.RFC3339),
		Queues: o.queues, Replicas: o.replicas, BatchSize: o.batchSize,
		PollIntervalMs: o.pollIntervalMs, Concurrency: o.concurrency,
		ISL: o.isl, OSL: o.osl, GatewayDelayMs: float64(o.gatewayDelay) / 1e6,
		ConfigCeiling: float64(o.queues*o.replicas*o.batchSize) * 1000 / float64(o.pollIntervalMs),
	}
	runDir := filepath.Join(filepath.Dir(o.out), fmt.Sprintf("%s-%s-%d", o.impl, o.mode, started.Unix()))
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return r, err
	}

	adm, err := connectAdmin(ctx, o.databaseURL)
	if err != nil {
		return r, err
	}
	defer adm.conn.Close(context.Background()) //nolint:errcheck // End of run.
	dbName := fmt.Sprintf("bench_%s_%d", o.impl, started.Unix())
	dbURL, err := adm.createDatabase(ctx, dbName)
	if err != nil {
		return r, fmt.Errorf("create database: %w", err)
	}
	if !o.keep {
		defer adm.dropDatabase(context.Background(), dbName) //nolint:errcheck // Best effort.
	}

	gw, err := startGateway(o.gatewayDelay, completion(o.isl, o.osl))
	if err != nil {
		return r, err
	}
	defer gw.close()

	cfg := processorConfig{
		DBURL: dbURL, GatewayURL: gw.url, Queues: o.queueNames(),
		BatchSize: o.batchSize, PollIntervalMs: o.pollIntervalMs, Concurrency: o.concurrency,
	}
	factory := newFactory(o.isl, time.Hour)

	if o.mode == "drain" {
		if err := preloadFor(ctx, o, cfg, factory, runDir); err != nil {
			return r, fmt.Errorf("preload: %w", err)
		}
		if hits := gw.stats().Hits; hits != 0 {
			return r, fmt.Errorf("preload dispatched %d requests; the queues were not held", hits)
		}
		r.Requests = int64(o.n)
	}

	results, err := pgx.Connect(ctx, dbURL)
	if err != nil {
		return r, err
	}
	defer results.Close(context.Background()) //nolint:errcheck // End of run.

	if _, err := adm.conn.Exec(ctx, "CHECKPOINT"); err != nil {
		return r, fmt.Errorf("checkpoint: %w", err)
	}
	before, err := adm.snapshot(ctx, dbName)
	if err != nil {
		return r, err
	}
	coldStart := time.Now()
	procs := make([]*processor, 0, o.replicas)
	stopAll := func() (usage, error) {
		var total usage
		var errs []error
		for _, p := range procs {
			u, err := p.stop()
			errs = append(errs, err)
			total.CPUSeconds += u.CPUSeconds
			total.MaxRSSMiB = max(total.MaxRSSMiB, u.MaxRSSMiB)
		}
		procs = nil
		return total, errors.Join(errs...)
	}
	defer stopAll() //nolint:errcheck // Only reached on an early error.
	for i := range o.replicas {
		p, err := startProcessor(ctx, o.impl, o.bin(), cfg, runDir, fmt.Sprintf("replica-%d", i))
		if err != nil {
			return r, err
		}
		procs = append(procs, p)
	}

	var got consumed
	if o.mode == "rate" {
		sub, reader, workers, err := loadFor(ctx, o.impl, procs[0], dbURL, cfg.Queues)
		if err != nil {
			return r, err
		}
		defer sub.close()
		sent := &sync.Map{}
		readCtx, stopReading := context.WithCancel(ctx)
		readers := consume(readCtx, reader, workers, sent, &got)
		defer func() {
			stopReading()
			readers.Wait()
		}()
		off := openLoop(ctx, sub, factory, cfg.Queues, o.rate, o.duration, 256, sent)
		r.Requests = off.Submitted
		r.OfferedRate = float64(off.Submitted) / off.Elapsed.Seconds()
		r.LateTicks, r.FailedSubs = off.LateTicks, off.Failed
	}

	deadline := time.Now().Add(o.timeout)
	for gw.stats().Unique < int(r.Requests) {
		if time.Now().After(deadline) || ctx.Err() != nil {
			return r, fmt.Errorf("gave up: %d of %d dispatched (logs in %s)", gw.stats().Unique, r.Requests, runDir)
		}
		time.Sleep(10 * time.Millisecond)
	}
	for {
		n := got.count()
		if o.mode == "drain" {
			if n, err = countResults(ctx, results, o.impl); err != nil {
				return r, err
			}
		}
		r.Results = n
		if n >= r.Requests {
			break
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return r, fmt.Errorf("gave up: %d of %d results (logs in %s)", n, r.Requests, runDir)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if o.mode == "drain" {
		r.TotalSeconds = time.Since(coldStart).Seconds()
	}

	u, err := stopAll()
	if err != nil {
		return r, fmt.Errorf("stop processors: %w", err)
	}
	after, err := adm.snapshot(ctx, dbName)
	if err != nil {
		return r, err
	}

	g := gw.stats()
	r.Dispatched, r.Duplicates = g.Hits, g.Duplicates
	r.DispatchSeconds = g.Last.Sub(g.First).Seconds()
	if r.DispatchSeconds > 0 {
		r.DispatchRate = float64(g.Unique) / r.DispatchSeconds
	}
	r.PerSecond = timeline(g.PerSecond)
	for _, c := range r.PerSecond {
		r.PeakSecond = max(r.PeakSecond, c)
	}
	if o.mode == "rate" {
		r.LagMs = quantiles(g.LagsMs)
		got.mu.Lock()
		r.ResultLatencyMs = quantiles(got.latencies)
		r.ResultDuplicates = got.duplicates
		got.mu.Unlock()
	}
	r.DB = perRequest(after.minus(before), r.Requests)
	r.CPUSecs, r.MaxRSSMiB = u.CPUSeconds, u.MaxRSSMiB
	if r.Requests > 0 {
		r.CPUMsPerRequest = u.CPUSeconds * 1000 / float64(r.Requests)
	}
	return r, nil
}

// preloadFor fills the queues without dispatching any of it. The Go
// producer writes the tables directly; the Rust processor only takes
// requests over its API, so one runs with every queue held shut.
func preloadFor(ctx context.Context, o options, cfg processorConfig, f requestFactory, dir string) error {
	switch o.impl {
	case implGo:
		sub, err := newGoSubmitter(ctx, cfg.DBURL, cfg.Queues)
		if err != nil {
			return err
		}
		defer sub.close()
		return preload(ctx, sub, f, cfg.Queues, o.n, 1000, 8)
	case implRust:
		held := cfg
		held.Hold = true
		p, err := startProcessor(ctx, implRust, o.binRust, held, dir, "preload")
		if err != nil {
			return err
		}
		if err := putBudget(ctx, p.api, holdBudgetKey, 0); err != nil {
			p.stop() //nolint:errcheck // Already failing.
			return err
		}
		sub, err := newRustSubmitter(p.api)
		if err != nil {
			p.stop() //nolint:errcheck // Already failing.
			return err
		}
		if err := preload(ctx, sub, f, cfg.Queues, o.n, 1000, 8); err != nil {
			p.stop() //nolint:errcheck // Already failing.
			return err
		}
		_, err = p.stop()
		return err
	}
	return fmt.Errorf("unknown impl %q", o.impl)
}

func putBudget(ctx context.Context, api, key string, value float64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, api+"/v1/admin/budgets/"+key, strings.NewReader(fmt.Sprint(value)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close() //nolint:errcheck // Status is all that matters.
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("PUT budget %s: %s", key, resp.Status)
	}
	return nil
}

// loadFor is how a rate run submits requests and reads results, and how
// many readers it runs: one result per Rust pop, a batch per Go pop.
func loadFor(ctx context.Context, k implKind, p *processor, dbURL string, queues []string) (submitter, resultReader, int, error) {
	if k == implRust {
		sub, err := newRustSubmitter(p.api)
		if err != nil {
			return nil, nil, 0, err
		}
		reader, err := newRustReader(p.api)
		return sub, reader, 16, err
	}
	sub, err := newGoSubmitter(ctx, dbURL, queues)
	if err != nil {
		return nil, nil, 0, err
	}
	return sub, sub.reader(queues[0]), 4, nil
}

func countResults(ctx context.Context, conn *pgx.Conn, k implKind) (int64, error) {
	var n int64
	err := conn.QueryRow(ctx, "SELECT count(*) FROM "+k.resultsTable()).Scan(&n)
	return n, err
}

// timeline turns per-second counts into a dense series from the first busy
// second to the last.
func timeline(per map[int64]int) []int {
	if len(per) == 0 {
		return nil
	}
	secs := make([]int64, 0, len(per))
	for s := range per {
		secs = append(secs, s)
	}
	slices.Sort(secs)
	out := make([]int, secs[len(secs)-1]-secs[0]+1)
	for s, c := range per {
		out[s-secs[0]] = c
	}
	return out
}

func printReport(r report) {
	fmt.Printf("%s %s: %d queues x %d replicas, batch %d every %dms (ceiling %.0f/s), %d workers, ISL %d OSL %d\n",
		r.Impl, r.Mode, r.Queues, r.Replicas, r.BatchSize, r.PollIntervalMs, r.ConfigCeiling, r.Concurrency, r.ISL, r.OSL)
	fmt.Printf("  requests %d, dispatched %d (%d duplicates), results %d\n", r.Requests, r.Dispatched, r.Duplicates, r.Results)
	if r.Mode == "drain" {
		fmt.Printf("  drain: %.2fs cold start to last result; dispatch %.0f/s over %.2fs, peak second %d\n",
			r.TotalSeconds, r.DispatchRate, r.DispatchSeconds, r.PeakSecond)
	} else {
		fmt.Printf("  offered %.0f/s (%d late ticks, %d failed); dispatched %.0f/s\n", r.OfferedRate, r.LateTicks, r.FailedSubs, r.DispatchRate)
		if r.LagMs != nil {
			fmt.Printf("  dispatch lag ms: p50 %.1f  p90 %.1f  p99 %.1f  max %.1f\n", r.LagMs.P50, r.LagMs.P90, r.LagMs.P99, r.LagMs.Max)
		}
		if q := r.ResultLatencyMs; q != nil {
			fmt.Printf("  result latency ms: p50 %.1f  p90 %.1f  p99 %.1f  max %.1f (%d duplicate results)\n", q.P50, q.P90, q.P99, q.Max, r.ResultDuplicates)
		}
	}
	fmt.Printf("  per request: %.2f xacts, %.2f rows written, %.0f WAL bytes", r.DB.Transactions, r.DB.RowsWritten, r.DB.WALBytes)
	if r.DB.Statements > 0 {
		fmt.Printf(", %.2f statements, %.3f ms exec", r.DB.Statements, r.DB.ExecMs)
	}
	fmt.Printf("\n  processor: %.2f CPU s (%.3f ms/request), max RSS %.0f MiB\n", r.CPUSecs, r.CPUMsPerRequest, r.MaxRSSMiB)
}

func appendReport(path string, r report) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close() //nolint:errcheck // Checked by the encoder's write.
	return json.NewEncoder(f).Encode(r)
}
