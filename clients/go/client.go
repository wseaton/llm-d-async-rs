// Package asyncclient submits requests to the llm-d-async processor over its
// HTTP API and reads their results.
//
// A Client satisfies producer.Producer from github.com/llm-d/llm-d-async, so
// code written against the Redis producer runs against the processor
// unchanged. Leased result delivery uses this package's Delivery, since
// producer.ResultDelivery cannot be built outside its own package.
package asyncclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/llm-d/llm-d-async/api"
)

var (
	// ErrResultDeliveryOwnershipLost means a leased result is no longer held
	// by this consumer: its lease lapsed and it may have been redelivered.
	ErrResultDeliveryOwnershipLost = errors.New("result delivery ownership lost")

	// ErrNotFound means the processor has no such result body.
	ErrNotFound = errors.New("not found")
)

const (
	defaultResultQueue = "result-list"
	defaultPollWait    = 30 * time.Second
	defaultLease       = 5 * time.Minute
)

// Client talks to one processor URL. With the Postgres store any replica
// serves any call, so the URL can front all of them.
type Client struct {
	base         *url.URL
	http         *http.Client
	requestQueue string
	resultQueue  string
	pollWait     time.Duration
	lease        time.Duration
}

type Option func(*Client)

// WithHTTPClient replaces http.DefaultClient. It must not time out long polls
// shorter than WithPollWait.
func WithHTTPClient(c *http.Client) Option { return func(cl *Client) { cl.http = c } }

// WithRequestQueue sets the queue submissions go to when they name none
// (default: the processor's first queue).
func WithRequestQueue(name string) Option { return func(c *Client) { c.requestQueue = name } }

// WithResultQueue sets the route this client reads results from, and the
// route its submissions ask results to go to when they name none (default
// "result-list"). Give each consumer its own route: a result read by one is
// gone for the others.
func WithResultQueue(name string) Option { return func(c *Client) { c.resultQueue = name } }

// WithPollWait bounds each long poll for results (default 30s, server max 60s).
func WithPollWait(d time.Duration) Option { return func(c *Client) { c.pollWait = d } }

// WithResultLease sets how long ReceiveResult holds a result (default 5m).
func WithResultLease(d time.Duration) Option { return func(c *Client) { c.lease = d } }

func New(baseURL string, opts ...Option) (*Client, error) {
	base, err := url.Parse(strings.TrimSuffix(baseURL, "/"))
	if err != nil {
		return nil, fmt.Errorf("parse processor URL: %w", err)
	}
	if base.Scheme != "http" && base.Scheme != "https" {
		return nil, fmt.Errorf("processor URL %q: want http or https", baseURL)
	}
	c := &Client{
		base:        base,
		http:        http.DefaultClient,
		resultQueue: defaultResultQueue,
		pollWait:    defaultPollWait,
		lease:       defaultLease,
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.resultQueue == "" {
		return nil, errors.New("result queue name must not be empty")
	}
	if c.pollWait <= 0 || c.lease <= 0 {
		return nil, errors.New("poll wait and result lease must be positive")
	}
	return c, nil
}

// Submission is one request. Payload is sent verbatim as the request body
// upstream (application/json); use SubmitStream for other content types or
// large bodies.
type Submission struct {
	ID       string            `json:"id"`
	Created  int64             `json:"created,omitempty"`
	Deadline int64             `json:"deadline"`
	Payload  json.RawMessage   `json:"payload,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
	Headers  map[string]string `json:"headers,omitempty"`
	Endpoint string            `json:"endpoint,omitempty"`
	Model    string            `json:"model,omitempty"`
	// RequestQueue defaults to the client's request queue.
	RequestQueue string `json:"request_queue_name,omitempty"`
	// ResultQueue defaults to the client's result queue.
	ResultQueue string `json:"result_queue_name,omitempty"`
}

// Submitted identifies one accepted submission. RequestToken tells apart
// submissions that reuse an ID.
type Submitted struct {
	ID           string `json:"id"`
	RequestToken string `json:"request_token"`
}

// Result is a result as the processor sends it. When PayloadRef is set, the
// body was binary and is read with OpenResultBody; Payload is empty.
// PayloadLocation is then the body's URL in the object store holding it, if
// it is in one, for readers that can copy it there directly.
type Result struct {
	api.ResultMessage
	PayloadRef      string `json:"payload_ref,omitempty"`
	PayloadLocation string `json:"payload_location,omitempty"`
	ContentType     string `json:"content_type,omitempty"`
	PayloadSize     int64  `json:"payload_size,omitempty"`
	PayloadSHA256   string `json:"payload_sha256,omitempty"`
	RequestToken    string `json:"request_token,omitempty"`
}

// Delivery is a leased result. It stays stored until AckResult; if the lease
// lapses first it is delivered again.
type Delivery struct {
	Result     *Result
	ClaimID    uint64
	ownerToken string
}

// StatusError is a response the processor refused.
type StatusError struct {
	Status  int
	Message string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("llm-d-async: %d: %s", e.Status, e.Message)
}

func (c *Client) endpoint(path string, query url.Values) string {
	u := *c.base
	u.Path = strings.TrimSuffix(u.Path, "/") + path
	u.RawQuery = query.Encode()
	return u.String()
}

func (c *Client) do(ctx context.Context, method, path string, query url.Values, contentType string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.endpoint(path, query), body)
	if err != nil {
		return nil, fmt.Errorf("build %s %s: %w", method, path, err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w", method, path, err)
	}
	return resp, nil
}

// call sends a JSON body and decodes a JSON reply into out. A 204 leaves out
// untouched and reports false.
func (c *Client) call(ctx context.Context, method, path string, query url.Values, in, out any) (bool, error) {
	var body io.Reader
	contentType := ""
	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return false, fmt.Errorf("encode %s body: %w", path, err)
		}
		body, contentType = bytes.NewReader(raw), "application/json"
	}
	resp, err := c.do(ctx, method, path, query, contentType, body)
	if err != nil {
		return false, err
	}
	return decode(resp, out)
}

func decode(resp *http.Response, out any) (bool, error) {
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode == http.StatusNoContent {
		return false, nil
	}
	if resp.StatusCode >= 300 {
		return false, statusError(resp)
	}
	if out == nil {
		return true, nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return false, fmt.Errorf("decode %s reply: %w", resp.Request.URL.Path, err)
	}
	return true, nil
}

func statusError(resp *http.Response) error {
	var body struct {
		Error string `json:"error"`
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	msg := strings.TrimSpace(string(raw))
	if json.Unmarshal(raw, &body) == nil && body.Error != "" {
		msg = body.Error
	}
	switch resp.StatusCode {
	case http.StatusConflict:
		return fmt.Errorf("%w: %s", ErrResultDeliveryOwnershipLost, msg)
	case http.StatusNotFound:
		return fmt.Errorf("%w: %s", ErrNotFound, msg)
	}
	return &StatusError{Status: resp.StatusCode, Message: msg}
}

// FromRequest converts an api.Request, carrying over per-message queue names
// of an api.RedisRequest.
func FromRequest(req api.Request) (Submission, error) {
	payload, err := json.Marshal(req.ReqPayload())
	if err != nil {
		return Submission{}, fmt.Errorf("encode payload of %q: %w", req.ReqID(), err)
	}
	s := Submission{
		ID:       req.ReqID(),
		Created:  req.ReqCreated(),
		Deadline: req.ReqDeadline(),
		Payload:  payload,
		Metadata: req.ReqMetadata(),
		Headers:  req.ReqHeaders(),
		Endpoint: req.ReqEndpoint(),
	}
	if r, ok := req.(*api.RedisRequest); ok {
		s.RequestQueue, s.ResultQueue = r.RequestQueueName, r.ResultQueueName
	}
	return s, nil
}

// SubmitRequest enqueues req. It satisfies producer.Producer.
func (c *Client) SubmitRequest(ctx context.Context, req api.Request) error {
	s, err := FromRequest(req)
	if err != nil {
		return err
	}
	_, err = c.Submit(ctx, s)
	return err
}

func (c *Client) routed(s Submission) Submission {
	if s.RequestQueue == "" {
		s.RequestQueue = c.requestQueue
	}
	if s.ResultQueue == "" {
		s.ResultQueue = c.resultQueue
	}
	return s
}

func (c *Client) Submit(ctx context.Context, s Submission) (Submitted, error) {
	s = c.routed(s)
	var out Submitted
	if _, err := c.call(ctx, http.MethodPost, "/v1/requests", nil, s, &out); err != nil {
		return Submitted{}, fmt.Errorf("submit %q: %w", s.ID, err)
	}
	return out, nil
}

// SubmitBatch enqueues all of subs in one transaction, or none.
func (c *Client) SubmitBatch(ctx context.Context, subs []Submission) ([]Submitted, error) {
	routed := make([]Submission, len(subs))
	for i, s := range subs {
		routed[i] = c.routed(s)
	}
	subs = routed
	var out []Submitted
	if _, err := c.call(ctx, http.MethodPost, "/v1/requests/batch", nil, subs, &out); err != nil {
		return nil, fmt.Errorf("submit batch of %d: %w", len(subs), err)
	}
	return out, nil
}

// SubmitStream enqueues s with body as its payload, streamed without
// buffering and sent upstream with contentType. s.Payload must be empty.
func (c *Client) SubmitStream(ctx context.Context, s Submission, contentType string, body io.Reader) (Submitted, error) {
	if len(s.Payload) > 0 {
		return Submitted{}, errors.New("SubmitStream takes the payload as body, not Submission.Payload")
	}
	envelope, err := json.Marshal(c.routed(s))
	if err != nil {
		return Submitted{}, fmt.Errorf("encode %q: %w", s.ID, err)
	}
	pr, pw := io.Pipe()
	form := multipart.NewWriter(pw)
	go func() {
		pw.CloseWithError(writeForm(form, envelope, contentType, body))
	}()
	resp, err := c.do(ctx, http.MethodPost, "/v1/requests", nil, form.FormDataContentType(), pr)
	if err != nil {
		_ = pr.CloseWithError(err)
		return Submitted{}, fmt.Errorf("submit %q: %w", s.ID, err)
	}
	var out Submitted
	if _, err := decode(resp, &out); err != nil {
		return Submitted{}, fmt.Errorf("submit %q: %w", s.ID, err)
	}
	return out, nil
}

func writeForm(form *multipart.Writer, envelope []byte, contentType string, body io.Reader) error {
	request, err := form.CreateFormField("request")
	if err != nil {
		return err
	}
	if _, err := request.Write(envelope); err != nil {
		return err
	}
	header := textproto.MIMEHeader{}
	header.Set("Content-Disposition", `form-data; name="payload"; filename="payload"`)
	header.Set("Content-Type", contentType)
	part, err := form.CreatePart(header)
	if err != nil {
		return err
	}
	if _, err := io.Copy(part, body); err != nil {
		return err
	}
	return form.Close()
}

// CancelRequests marks requests cancelled before dispatch; best effort and
// idempotent. It satisfies producer.Producer.
func (c *Client) CancelRequests(ctx context.Context, ids []string) error {
	_, err := c.Cancel(ctx, ids)
	return err
}

// Cancel returns how many IDs had a live request to cancel.
func (c *Client) Cancel(ctx context.Context, ids []string) (int, error) {
	var out struct {
		Cancelled int `json:"cancelled"`
	}
	in := map[string][]string{"ids": ids}
	if _, err := c.call(ctx, http.MethodPost, "/v1/requests/cancel", nil, in, &out); err != nil {
		return 0, fmt.Errorf("cancel %d requests: %w", len(ids), err)
	}
	return out.Cancelled, nil
}

func (c *Client) waitQuery(ctx context.Context) url.Values {
	wait := c.pollWait
	if deadline, ok := ctx.Deadline(); ok {
		wait = min(wait, time.Until(deadline))
	}
	q := url.Values{}
	q.Set("wait_ms", strconv.FormatInt(max(wait.Milliseconds(), 0), 10))
	return q
}

func (c *Client) resultPath(suffix string) string {
	return "/v1/results/" + url.PathEscape(c.resultQueue) + suffix
}

// GetResult destructively takes the next result, waiting until one arrives
// or ctx ends. It satisfies producer.Producer.
func (c *Client) GetResult(ctx context.Context) (*api.ResultMessage, error) {
	r, err := c.PopResult(ctx)
	if err != nil {
		return nil, err
	}
	return &r.ResultMessage, nil
}

// PopResult is GetResult with the by-reference fields.
func (c *Client) PopResult(ctx context.Context) (*Result, error) {
	for {
		var r Result
		found, err := c.call(ctx, http.MethodPost, c.resultPath("/pop"), c.waitQuery(ctx), nil, &r)
		if err != nil {
			if ctx.Err() != nil {
				return nil, fmt.Errorf("await result: %w", ctx.Err())
			}
			return nil, fmt.Errorf("pop result: %w", err)
		}
		if found {
			return &r, nil
		}
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("await result: %w", err)
		}
	}
}

// ReceiveResult leases the next result, waiting until one arrives or ctx
// ends. Checkpoint it, then AckResult.
func (c *Client) ReceiveResult(ctx context.Context) (*Delivery, error) {
	for {
		q := c.waitQuery(ctx)
		q.Set("lease_ms", strconv.FormatInt(c.lease.Milliseconds(), 10))
		var claim struct {
			ClaimID    uint64 `json:"claim_id"`
			OwnerToken string `json:"owner_token"`
			Result     Result `json:"result"`
		}
		found, err := c.call(ctx, http.MethodPost, c.resultPath("/claims"), q, nil, &claim)
		if err != nil {
			if ctx.Err() != nil {
				return nil, fmt.Errorf("await result: %w", ctx.Err())
			}
			return nil, fmt.Errorf("claim result: %w", err)
		}
		if found {
			return &Delivery{Result: &claim.Result, ClaimID: claim.ClaimID, ownerToken: claim.OwnerToken}, nil
		}
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("await result: %w", err)
		}
	}
}

func (c *Client) claimPath(d *Delivery, action string) string {
	return c.resultPath("/claims/" + strconv.FormatUint(d.ClaimID, 10) + "/" + action)
}

// RenewResult extends the lease by the configured lease. It fails with
// ErrResultDeliveryOwnershipLost once the lease has lapsed.
func (c *Client) RenewResult(ctx context.Context, d *Delivery) error {
	in := map[string]any{"owner_token": d.ownerToken, "lease_ms": c.lease.Milliseconds()}
	if _, err := c.call(ctx, http.MethodPost, c.claimPath(d, "renew"), nil, in, nil); err != nil {
		return fmt.Errorf("renew result %d: %w", d.ClaimID, err)
	}
	return nil
}

// AckResult deletes a leased result and its body. Repeating it is safe.
func (c *Client) AckResult(ctx context.Context, d *Delivery) error {
	in := map[string]string{"owner_token": d.ownerToken}
	if _, err := c.call(ctx, http.MethodPost, c.claimPath(d, "ack"), nil, in, nil); err != nil {
		return fmt.Errorf("ack result %d: %w", d.ClaimID, err)
	}
	return nil
}

// ResultBody is a result body stored by reference.
type ResultBody struct {
	io.ReadCloser
	ContentType string
	Size        int64
}

const resultRefPrefix = "blob://results/"

// OpenResultBody streams the body of a result whose PayloadRef is set. It
// fails with ErrNotFound once the result is acknowledged or expired.
func (c *Client) OpenResultBody(ctx context.Context, r *Result) (*ResultBody, error) {
	name, ok := strings.CutPrefix(r.PayloadRef, resultRefPrefix)
	if !ok || name == "" {
		return nil, fmt.Errorf("result %q has no body by reference", r.ID)
	}
	resp, err := c.do(ctx, http.MethodGet, "/v1/blobs/results/"+url.PathEscape(name), nil, "", nil)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer func() { _ = resp.Body.Close() }()
		return nil, fmt.Errorf("open body of %q: %w", r.ID, statusError(resp))
	}
	return &ResultBody{ReadCloser: resp.Body, ContentType: resp.Header.Get("Content-Type"), Size: resp.ContentLength}, nil
}

// QueueDepth is the number of requests waiting in queue name.
func (c *Client) QueueDepth(ctx context.Context, name string) (int64, error) {
	var queues []struct {
		QueueName string `json:"queue_name"`
		Depth     int64  `json:"depth"`
	}
	if _, err := c.call(ctx, http.MethodGet, "/v1/queues", nil, nil, &queues); err != nil {
		return 0, fmt.Errorf("list queues: %w", err)
	}
	for _, q := range queues {
		if q.QueueName == name {
			return q.Depth, nil
		}
	}
	return 0, fmt.Errorf("queue %q: %w", name, ErrNotFound)
}

// ResultQueueDepth is the number of unleased results waiting.
func (c *Client) ResultQueueDepth(ctx context.Context) (int64, error) {
	var out struct {
		Depth int64 `json:"depth"`
	}
	if _, err := c.call(ctx, http.MethodGet, c.resultPath("/depth"), nil, nil, &out); err != nil {
		return 0, fmt.Errorf("result depth: %w", err)
	}
	return out.Depth, nil
}

// Close releases idle connections. It satisfies producer.Producer.
func (c *Client) Close() error {
	c.http.CloseIdleConnections()
	return nil
}
