package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"time"
)

type implKind string

const (
	implRust implKind = "rust"
	implGo   implKind = "go"
)

func parseImpl(s string) (implKind, error) {
	switch implKind(s) {
	case implRust, implGo:
		return implKind(s), nil
	}
	return "", fmt.Errorf("unknown -impl %q: want rust or go", s)
}

func (k implKind) resultsTable() string {
	if k == implRust {
		return "lda_results"
	}
	return "async_results"
}

const resultQueue = "bench-results"

// holdBudgetKey names the budget that holds the Rust processor's queues shut
// while a drain run preloads them.
const holdBudgetKey = "bench-hold"

// processorConfig is everything both processors are configured alike with.
type processorConfig struct {
	DBURL          string
	GatewayURL     string
	Queues         []string
	BatchSize      int
	PollIntervalMs int
	Concurrency    int
	// Hold closes every queue (Rust preload only).
	Hold bool
}

func (c processorConfig) transport(k implKind) map[string]any {
	queues := make([]map[string]any, 0, len(c.Queues))
	for _, q := range c.Queues {
		entry := map[string]any{
			"queue_name":       q,
			"igw_base_url":     c.GatewayURL,
			"request_path_url": "/v1/completions",
		}
		if c.Hold {
			entry["gate_type"] = "budget-key"
			entry["gate_params"] = map[string]any{"budget_key": holdBudgetKey}
		}
		queues = append(queues, entry)
	}
	t := map[string]any{
		"result_queue_name": resultQueue,
		"poll_interval_ms":  c.PollIntervalMs,
		"batch_size":        c.BatchSize,
		"queues":            queues,
	}
	if k == implGo {
		t["url"] = c.DBURL
	}
	return t
}

// processor is one running replica.
type processor struct {
	cmd    *exec.Cmd
	health string
	api    string
	log    *os.File
	// exited closes once the process is gone; waitErr is then set.
	exited  chan struct{}
	waitErr error
}

func freePort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer ln.Close() //nolint:errcheck // Only the port number is wanted.
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		return 0, errors.New("listener has no TCP address")
	}
	return addr.Port, nil
}

func startProcessor(ctx context.Context, k implKind, bin string, cfg processorConfig, dir string, name string) (*processor, error) {
	transport, err := json.Marshal(cfg.transport(k))
	if err != nil {
		return nil, err
	}
	transportFile := filepath.Join(dir, name+"-transport.json")
	if err := os.WriteFile(transportFile, transport, 0o600); err != nil {
		return nil, err
	}
	health, err := freePort()
	if err != nil {
		return nil, err
	}
	metrics, err := freePort()
	if err != nil {
		return nil, err
	}
	args := []string{
		"--transport-config-file", transportFile,
		"--concurrency", strconv.Itoa(cfg.Concurrency),
		"--health-port", strconv.Itoa(health),
		"--metrics-port", strconv.Itoa(metrics),
		"--drain-timeout", "10s",
	}
	p := &processor{health: fmt.Sprintf("http://127.0.0.1:%d", health)}
	switch k {
	case implRust:
		apiPort, err := freePort()
		if err != nil {
			return nil, err
		}
		p.api = fmt.Sprintf("http://127.0.0.1:%d", apiPort)
		args = append(args,
			"--store", "postgres",
			"--database-url", cfg.DBURL,
			"--blob-store", "postgres",
			"--api-addr", fmt.Sprintf("127.0.0.1:%d", apiPort),
		)
	case implGo:
		args = append(args, "--transport", "sql")
	}
	p.log, err = os.Create(filepath.Join(dir, name+".log"))
	if err != nil {
		return nil, err
	}
	p.cmd = exec.Command(bin, args...)
	p.cmd.Stdout, p.cmd.Stderr = p.log, p.log
	if err := p.cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %s: %w", bin, err)
	}
	p.exited = make(chan struct{})
	go func() {
		p.waitErr = p.cmd.Wait()
		close(p.exited)
	}()
	if err := p.waitReady(ctx, 60*time.Second); err != nil {
		p.kill()
		return nil, fmt.Errorf("%s never became ready (log: %s): %w", name, p.log.Name(), err)
	}
	return p, nil
}

func (p *processor) waitReady(ctx context.Context, limit time.Duration) error {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.health+"/readyz", nil)
		if err != nil {
			return err
		}
		if resp, err := http.DefaultClient.Do(req); err == nil {
			resp.Body.Close() //nolint:errcheck // Status is all that matters.
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-p.exited:
			return fmt.Errorf("exited: %v", p.waitErr)
		case <-time.After(100 * time.Millisecond):
		}
	}
	return errors.New("timed out")
}

// usage is a replica's resource use over its whole life.
type usage struct {
	CPUSeconds float64
	MaxRSSMiB  float64
}

// stop sends SIGTERM, waits for a graceful exit, and reports the process's
// resource use.
func (p *processor) stop() (usage, error) {
	defer p.log.Close() //nolint:errcheck // Log is complete once the process exits.
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return usage{}, err
	}
	select {
	case <-p.exited:
		var exit *exec.ExitError
		if p.waitErr != nil && !errors.As(p.waitErr, &exit) {
			return usage{}, p.waitErr
		}
	case <-time.After(30 * time.Second):
		p.kill()
		<-p.exited
		return usage{}, errors.New("did not exit within 30s of SIGTERM")
	}
	ru, ok := p.cmd.ProcessState.SysUsage().(*syscall.Rusage)
	if !ok {
		return usage{}, errors.New("no rusage")
	}
	cpu := time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
	rss := float64(ru.Maxrss) / 1024 // KiB on Linux.
	if runtime.GOOS == "darwin" {
		rss /= 1024 // Bytes on macOS.
	}
	return usage{CPUSeconds: cpu.Seconds(), MaxRSSMiB: rss}, nil
}

func (p *processor) kill() {
	if p.cmd.Process != nil {
		p.cmd.Process.Kill() //nolint:errcheck // Best effort on a failed start.
	}
}
