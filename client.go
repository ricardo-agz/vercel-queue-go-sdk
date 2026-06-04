package queue

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Client publishes messages to Vercel Queues and performs lease operations
// (ack, extend) for consumers. It is safe for concurrent use.
//
// A zero-config client resolves its token, base URL, and deployment ID from
// the environment, which is the expected setup inside a Vercel deployment.
type Client struct {
	httpClient   *http.Client
	token        string
	baseURL      string
	basePath     string
	deploymentID deploymentID
	headers      map[string]string
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithToken sets an explicit bearer token, overriding environment lookup.
func WithToken(token string) ClientOption {
	return func(c *Client) { c.token = token }
}

// WithRegion targets a region-specific Queue Service endpoint.
func WithRegion(region string) ClientOption {
	return func(c *Client) {
		if region != "" {
			c.baseURL = "https://" + region + ".vercel-queue.com"
		}
	}
}

// WithBaseURL overrides the Queue Service base URL.
func WithBaseURL(baseURL string) ClientOption {
	return func(c *Client) { c.baseURL = strings.TrimRight(baseURL, "/") }
}

// WithBasePath overrides the Queue Service base path (default /api/v3/topic).
func WithBasePath(basePath string) ClientOption {
	return func(c *Client) { c.basePath = basePath }
}

// WithHTTPClient sets the underlying *http.Client used for all requests.
func WithHTTPClient(hc *http.Client) ClientOption {
	return func(c *Client) {
		if hc != nil {
			c.httpClient = hc
		}
	}
}

// WithDeploymentID pins published messages to a specific deployment.
func WithDeploymentID(id string) ClientOption {
	return func(c *Client) { c.deploymentID = deploymentID{value: id} }
}

// WithoutDeploymentPinning disables deployment pinning entirely.
func WithoutDeploymentPinning() ClientOption {
	return func(c *Client) { c.deploymentID = deploymentID{disabled: true} }
}

// WithHeader adds a default header sent on every request.
func WithHeader(key, value string) ClientOption {
	return func(c *Client) {
		if c.headers == nil {
			c.headers = make(map[string]string)
		}
		c.headers[key] = value
	}
}

// NewClient creates a Client. With no options it is configured entirely from
// the environment. Construction performs no I/O; the token is resolved lazily
// on first use.
func NewClient(opts ...ClientOption) *Client {
	c := &Client{
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// defaultClient is the package-level client used by [Topic.Send]. It is
// configured entirely from the environment and resolves its auth token per call
// from the context, so a single shared instance is safe for concurrent use.
var defaultClient = NewClient()

// DefaultClient returns the shared client used by [Topic.Send]. Use it to call
// the low-level [Client.Send] without constructing your own client.
func DefaultClient() *Client { return defaultClient }

// resolvedToken returns the token for this client, considering an explicit
// client token, a per-request token carried on ctx, then the environment.
func (c *Client) resolvedToken(ctx context.Context) (string, error) {
	return resolveToken(ctx, c.token)
}

func (c *Client) topicURL(topic string) string {
	base := resolveBaseURL(c.baseURL)
	path := resolveBasePath(c.basePath)
	return base + path + "/" + urlEscape(topic)
}

func (c *Client) consumerURL(topic, consumer string) string {
	return c.topicURL(topic) + "/consumer/" + urlEscape(consumer)
}

func (c *Client) leaseURL(topic, consumer, receiptHandle string) string {
	return c.consumerURL(topic, consumer) + "/lease/" + urlEscape(receiptHandle)
}

func (c *Client) messageURL(topic, consumer, messageID string) string {
	return c.consumerURL(topic, consumer) + "/id/" + urlEscape(messageID)
}

// newRequest builds an authenticated request with default headers applied.
func (c *Client) newRequest(ctx context.Context, method, url string, body []byte) (*http.Request, error) {
	token, err := c.resolvedToken(ctx)
	if err != nil {
		return nil, err
	}
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rdr)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	for k, v := range c.headers {
		req.Header.Set(k, v)
	}
	if id := resolveDeploymentID(c.deploymentID); id != "" {
		req.Header.Set("Vqs-Deployment-Id", id)
	}
	return req, nil
}

// statusError maps a non-success HTTP response to a typed *Error. It returns
// nil for success codes.
func statusError(op, topic string, resp *http.Response) error {
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	}
	e := &Error{
		Op:         op,
		Topic:      topic,
		StatusCode: resp.StatusCode,
		RequestID:  resp.Header.Get("X-Request-Id"),
	}
	if body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096)); len(body) > 0 {
		e.Message = strings.TrimSpace(string(body))
	}
	switch resp.StatusCode {
	case http.StatusBadRequest:
		e.err = ErrBadRequest
	case http.StatusUnauthorized:
		e.err = ErrUnauthorized
	case http.StatusForbidden:
		e.err = ErrForbidden
	case http.StatusConflict:
		// On send, 409 means a duplicate idempotency key. On lease ops it means
		// the lease is gone; callers override Op so the sentinel stays meaningful.
		if op == "Send" {
			e.err = ErrDuplicateIdempotencyKey
		} else {
			e.err = ErrMessageNotAvailable
		}
	case http.StatusNotFound, http.StatusGone:
		e.err = ErrMessageNotFound
	case http.StatusTooManyRequests:
		e.err = ErrThrottled
		e.RetryAfter = parseRetryAfter(resp)
	default:
		if resp.StatusCode >= 500 {
			e.err = ErrServer
		} else {
			e.err = errors.New("queue: unexpected status " + strconv.Itoa(resp.StatusCode))
		}
	}
	return e
}

func parseRetryAfter(resp *http.Response) time.Duration {
	v := resp.Header.Get("Retry-After")
	if v == "" {
		return 0
	}
	secs, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil || secs < 0 {
		return 0
	}
	return time.Duration(secs) * time.Second
}
