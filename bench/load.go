package main

import (
	"context"
	"encoding/json"
	"fmt"
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
			PollInterval:     resultPollInterval,
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

func newFactory(isl int, deadline time.Duration) requestFactory {
	return requestFactory{prompt: text(isl, 1), deadline: deadline}
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
func openLoop(ctx context.Context, s submitter, f requestFactory, queues []string, rate int, duration time.Duration, maxInFlight int, sent *sync.Map) offered {
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
		at := time.Now()
		for _, r := range reqs {
			sent.Store(r.ReqID(), at)
		}
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

// resultPollInterval is how often a reader that found no result asks again.
const resultPollInterval = 10 * time.Millisecond

// resultReader takes finished results off the result queue, as a producer
// does, and returns their request IDs.
type resultReader interface {
	read(ctx context.Context) ([]string, error)
}

// rustReader long-polls the Rust processor for one result at a time.
type rustReader struct {
	client *asyncclient.Client
}

func newRustReader(apiURL string) (*rustReader, error) {
	c, err := asyncclient.New(apiURL,
		asyncclient.WithResultQueue(resultQueue),
		asyncclient.WithPollWait(time.Second))
	if err != nil {
		return nil, err
	}
	return &rustReader{client: c}, nil
}

func (r *rustReader) read(ctx context.Context) ([]string, error) {
	res, err := r.client.PopResult(ctx)
	if err != nil {
		return nil, err
	}
	return []string{res.ID}, nil
}

// goReader pops batches through the Go producer.
type goReader struct {
	producer *producersql.Producer
}

func (s *goSubmitter) reader(queue string) *goReader {
	return &goReader{producer: s.producers[queue]}
}

func (r *goReader) read(ctx context.Context) ([]string, error) {
	results, err := r.producer.GetResults(ctx, 256)
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(results))
	for i, res := range results {
		ids[i] = res.ID
	}
	return ids, nil
}

// consumed is what result readers have taken so far.
type consumed struct {
	mu         sync.Mutex
	n          int64
	duplicates int64
	latencies  []float64
}

func (c *consumed) count() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

// consume runs workers readers until ctx ends, timing each result from
// when its request was sent.
func consume(ctx context.Context, r resultReader, workers int, sent *sync.Map, c *consumed) *sync.WaitGroup {
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				ids, err := r.read(ctx)
				if err != nil {
					continue
				}
				now := time.Now()
				c.mu.Lock()
				for _, id := range ids {
					at, ok := sent.LoadAndDelete(id)
					if !ok {
						c.duplicates++
						continue
					}
					if t, ok := at.(time.Time); ok {
						c.latencies = append(c.latencies, float64(now.Sub(t))/1e6)
					}
					c.n++
				}
				c.mu.Unlock()
			}
		}()
	}
	return &wg
}
