package main

import (
	"bufio"
	"cmp"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
)

// runKey groups repeated runs of one configuration.
type runKey struct {
	Mode      string
	Queues    int
	Replicas  int
	Impl      implKind
	Size      int64
	BatchSize int
	PollMs    int
	ISL       int
	OSL       int
}

// summarize prints the median of each configuration's runs in file as a
// markdown table.
func summarize(path string, w io.Writer) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close() //nolint:errcheck // Read only.
	groups := map[runKey][]report{}
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<20), 1<<24)
	for scanner.Scan() {
		var r report
		if err := json.Unmarshal(scanner.Bytes(), &r); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		size := r.Requests
		if r.Mode == "rate" {
			size = int64(r.OfferedRate + 0.5)
		}
		k := runKey{r.Mode, r.Queues, r.Replicas, r.Impl, size, r.BatchSize, r.PollIntervalMs, r.ISL, r.OSL}
		groups[k] = append(groups[k], r)
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	keys := make([]runKey, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b runKey) int {
		return cmp.Or(
			cmp.Compare(a.Mode, b.Mode),
			cmp.Compare(a.ISL, b.ISL),
			cmp.Compare(a.OSL, b.OSL),
			cmp.Compare(a.Size, b.Size),
			cmp.Compare(a.Queues, b.Queues),
			cmp.Compare(a.Replicas, b.Replicas),
			cmp.Compare(b.Impl, a.Impl),
		)
	})
	mode := ""
	for _, k := range keys {
		rs := groups[k]
		med := func(f func(report) float64) float64 {
			xs := make([]float64, len(rs))
			for i, r := range rs {
				xs[i] = f(r)
			}
			slices.Sort(xs)
			return xs[len(xs)/2]
		}
		dups := 0
		for _, r := range rs {
			dups += r.Duplicates
		}
		if k.Mode != mode {
			mode = k.Mode
			if mode == "drain" {
				fmt.Fprintln(w, "\n| ISL | OSL | queues | replicas | impl | runs | requests | last result (s) | dispatch/s | xacts/req | DB exec ms/req | WAL B/req | CPU ms/req | max RSS MiB | duplicates |")
				fmt.Fprintln(w, "|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
			} else {
				fmt.Fprintln(w, "\n| ISL | OSL | offered/s | queues | replicas | impl | runs | dispatch lag p50 ms | dispatch lag p99 ms | result latency p50 ms | result latency p99 ms | xacts/req | DB exec ms/req | CPU ms/req | duplicates |")
				fmt.Fprintln(w, "|---:|---:|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
			}
		}
		lag := func(pick func(report) *quantile, f func(*quantile) float64) float64 {
			return med(func(r report) float64 {
				if q := pick(r); q != nil {
					return f(q)
				}
				return 0
			})
		}
		dispatchLag := func(r report) *quantile { return r.LagMs }
		resultLatency := func(r report) *quantile { return r.ResultLatencyMs }
		p50 := func(q *quantile) float64 { return q.P50 }
		p99 := func(q *quantile) float64 { return q.P99 }
		if k.Mode == "drain" {
			fmt.Fprintf(w, "| %d | %d | %d | %d | %s | %d | %d | %.1f | %.0f | %.3f | %.3f | %.0f | %.3f | %.0f | %d |\n",
				k.ISL, k.OSL, k.Queues, k.Replicas, k.Impl, len(rs), k.Size,
				med(func(r report) float64 { return r.TotalSeconds }),
				med(func(r report) float64 { return r.DispatchRate }),
				med(func(r report) float64 { return r.DB.Transactions }),
				med(func(r report) float64 { return r.DB.ExecMs }),
				med(func(r report) float64 { return r.DB.WALBytes }),
				med(func(r report) float64 { return r.CPUMsPerRequest }),
				med(func(r report) float64 { return r.MaxRSSMiB }),
				dups)
		} else {
			fmt.Fprintf(w, "| %d | %d | %d | %d | %d | %s | %d | %.1f | %.1f | %.1f | %.1f | %.3f | %.3f | %.3f | %d |\n",
				k.ISL, k.OSL, k.Size, k.Queues, k.Replicas, k.Impl, len(rs),
				lag(dispatchLag, p50), lag(dispatchLag, p99),
				lag(resultLatency, p50), lag(resultLatency, p99),
				med(func(r report) float64 { return r.DB.Transactions }),
				med(func(r report) float64 { return r.DB.ExecMs }),
				med(func(r report) float64 { return r.CPUMsPerRequest }),
				dups)
		}
	}
	return nil
}
