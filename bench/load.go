package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/llm-d/llm-d-async/api"
	producersql "github.com/llm-d/llm-d-async/producer-sql"
	"github.com/llm-d/llm-d-async/producer-sql/sqlqueue"
	asyncclient "github.com/wseaton/llm-d-async-rs/clients/go"
)

// submitter enqueues a batch of requests on one queue.
type submitter interface {
	submit(ctx context.Context, queue string, reqs []api.Request) error
	close()
}

// rustSubmitter posts batches to the Rust processor's HTTP API.
type rustSubmitter struct {
	client *asyncclient.Client
}

func newRustSubmitter(apiURL string) (*rustSubmitter, error) {
	c, err := asyncclient.New(apiURL, asyncclient.WithResultQueue(resultQueue))
	if err != nil {
		return nil, err
	}
	return &rustSubmitter{client: c}, nil
}

func (s *rustSubmitter) submit(ctx context.Context, queue string, reqs []api.Request) error {
	subs := make([]asyncclient.Submission, len(reqs))
	for i, r := range reqs {
		sub, err := asyncclient.FromRequest(r)
		if err != nil {
			return err
		}
		sub.RequestQueue = queue
		subs[i] = sub
	}
	_, err := s.client.SubmitBatch(ctx, subs)
	return err
}

func (s *rustSubmitter) close() {}

// goSubmitter inserts batches straight into the Go processor's tables, as
// its producer does.
type goSubmitter struct {
	store     *sqlqueue.Store
	producers map[string]*producersql.Producer
}

func newGoSubmitter(ctx context.Context, dbURL string, queues []string) (*goSubmitter, error) {
	store, err := sqlqueue.Open(ctx, dbURL)
	if err != nil {
		return nil, err
	}
	if err := store.Migrate(ctx); err != nil {
		store.Close()
		return nil, err
	}
	s := &goSubmitter{store: store, producers: make(map[string]*producersql.Producer)}
	for _, q := range queues {
		p, err := producersql.New(ctx, producersql.Config{
			RequestQueueName: q,
			ResultQueueName:  resultQueue,
		}, producersql.WithStore(store))
		if err != nil {
			store.Close()
			return nil, err
		}
		s.producers[q] = p
	}
	return s, nil
}

func (s *goSubmitter) submit(ctx context.Context, queue string, reqs []api.Request) error {
	return s.producers[queue].SubmitRequests(ctx, reqs)
}

func (s *goSubmitter) close() { s.store.Close() }

// requestFactory builds requests whose payloads carry their ID and, when
// stamped, the time they were built, for the gateway.
type requestFactory struct {
	prompt   string
	deadline time.Duration
}

// batch builds count requests numbered from from.
func (f requestFactory) batch(from, count int, stamped bool) []api.Request {
	now := time.Now()
	var sent int64
	if stamped {
		sent = now.UnixNano()
	}
	reqs := make([]api.Request, count)
	for i := range reqs {
		id := fmt.Sprintf("b-%08d", from+i)
		reqs[i] = &api.RequestMessage{
			ID:       id,
			Created:  now.Unix(),
			Deadline: now.Add(f.deadline).Unix(),
			Payload: json.RawMessage(fmt.Sprintf(
				`{"model":"bench","prompt":%q,"bench_id":%q,"sent_ns":%d}`, f.prompt, id, sent)),
		}
	}
	return reqs
}

func newFactory(payloadBytes int, deadline time.Duration) requestFactory {
	return requestFactory{prompt: strings.Repeat("x", payloadBytes), deadline: deadline}
}

// preload submits n requests spread across queues in chunks of chunk, with
// up to parallel chunks in flight. They are not stamped: a preloaded
// request's wait is not dispatch lag.
func preload(ctx context.Context, s submitter, f requestFactory, queues []string, n, chunk, parallel int) error {
	var (
		wg   sync.WaitGroup
		errs = make(chan error, 1)
		sem  = make(chan struct{}, parallel)
	)
	for from, i := 0, 0; from < n; from, i = from+chunk, i+1 {
		count := min(chunk, n-from)
		reqs := f.batch(from, count, false)
		queue := queues[i%len(queues)]
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if err := s.submit(ctx, queue, reqs); err != nil {
				select {
				case errs <- err:
				default:
				}
			}
		}()
	}
	wg.Wait()
	select {
	case err := <-errs:
		return err
	default:
		return nil
	}
}

// offered is what an open-loop run actually submitted.
type offered struct {
	Submitted int64
	Failed    int64
	// LateTicks counts ticks whose batch could not start on time because
	// too many submissions were still in flight.
	LateTicks int64
	Elapsed   time.Duration
}

// openLoop submits rate requests per second for duration, in one batch per
// tick, whether or not earlier batches have finished (up to maxInFlight).
func openLoop(ctx context.Context, s submitter, f requestFactory, queues []string, rate int, duration time.Duration, maxInFlight int) offered {
	const tick = 10 * time.Millisecond
	perTick := float64(rate) * tick.Seconds()
	var (
		out     offered
		wg      sync.WaitGroup
		sem     = make(chan struct{}, maxInFlight)
		carry   float64
		next    int
		ticks   int
		start   = time.Now()
		ticker  = time.NewTicker(tick)
		stopped = time.After(duration)
		sub     atomic.Int64
		failed  atomic.Int64
	)
	defer ticker.Stop()
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-stopped:
			break loop
		case <-ticker.C:
		}
		carry += perTick
		count := int(carry)
		carry -= float64(count)
		if count == 0 {
			continue
		}
		reqs := f.batch(next, count, true)
		next += count
		queue := queues[ticks%len(queues)]
		ticks++
		select {
		case sem <- struct{}{}:
		default:
			out.LateTicks++
			sem <- struct{}{}
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			if err := s.submit(ctx, queue, reqs); err != nil {
				failed.Add(int64(len(reqs)))
				return
			}
			sub.Add(int64(len(reqs)))
		}()
	}
	wg.Wait()
	out.Submitted, out.Failed, out.Elapsed = sub.Load(), failed.Load(), time.Since(start)
	return out
}
