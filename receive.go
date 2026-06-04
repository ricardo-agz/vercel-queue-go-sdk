package queue

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// defaultVisibilityTimeout and defaultRefreshInterval mirror the Node/Python
// consumer defaults: a 30s lease refreshed every 10s while a handler runs.
const (
	defaultVisibilityTimeout = 30 * time.Second
	defaultRefreshInterval   = 10 * time.Second
)

// lease represents an outstanding message lease that can be acknowledged
// (deleted) or extended (visibility changed).
type lease struct {
	client        *Client
	topic         string
	consumer      string
	messageID     string
	receiptHandle string
	// token is the per-request auth token (e.g. the Vercel OIDC token from the
	// callback request). It is used for background lease extensions, whose
	// context would otherwise carry no credential.
	token string
}

// ack deletes the message, acknowledging successful processing.
func (l *lease) ack(ctx context.Context) error {
	req, err := l.client.newRequest(ctx, http.MethodDelete, l.client.leaseURL(l.topic, l.consumer, l.receiptHandle), nil)
	if err != nil {
		return err
	}
	resp, err := l.client.httpClient.Do(req)
	if err != nil {
		return &Error{Op: "ack", Topic: l.topic, err: err}
	}
	defer resp.Body.Close()
	return statusError("ack", l.topic, resp)
}

// extend changes the message's visibility timeout to d.
func (l *lease) extend(ctx context.Context, d time.Duration) error {
	body, _ := json.Marshal(map[string]int{"visibilityTimeoutSeconds": durationSeconds(d)})
	req, err := l.client.newRequest(ctx, http.MethodPatch, l.client.leaseURL(l.topic, l.consumer, l.receiptHandle), body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := l.client.httpClient.Do(req)
	if err != nil {
		return &Error{Op: "extend", Topic: l.topic, err: err}
	}
	defer resp.Body.Close()
	return statusError("extend", l.topic, resp)
}

// rawMessage is a message fetched from the Queue Service.
type rawMessage struct {
	body          []byte
	contentType   string
	deliveryCount int
	createdAt     time.Time
	receiptHandle string
}

// receiveByID fetches a single message (and obtains a receipt handle) by id.
// Used when a callback is metadata-only and carries no inline payload.
func (c *Client) receiveByID(ctx context.Context, topic, consumer, messageID string, visibility time.Duration) (*rawMessage, error) {
	req, err := c.newRequest(ctx, http.MethodPost, c.messageURL(topic, consumer, messageID), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "multipart/mixed")
	if visibility > 0 {
		req.Header.Set("Vqs-Visibility-Timeout-Seconds", strconv.Itoa(durationSeconds(visibility)))
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, &Error{Op: "receive", Topic: topic, err: err}
	}
	defer resp.Body.Close()
	if err := statusError("receive", topic, resp); err != nil {
		return nil, err
	}

	headers, payload, err := parseSingleMultipart(resp)
	if err != nil {
		return nil, &Error{Op: "receive", Topic: topic, err: ErrMessageCorrupted, Message: err.Error()}
	}
	receipt := headers.Get("Vqs-Receipt-Handle")
	if receipt == "" {
		return nil, &Error{Op: "receive", Topic: topic, err: ErrMessageCorrupted, Message: "missing Vqs-Receipt-Handle"}
	}
	return &rawMessage{
		body:          payload,
		contentType:   headers.Get("Content-Type"),
		deliveryCount: atoiOr(headers.Get("Vqs-Delivery-Count"), 0),
		createdAt:     parseTime(headers.Get("Vqs-Timestamp")),
		receiptHandle: receipt,
	}, nil
}

// parseSingleMultipart reads the first part of a multipart/mixed response.
func parseSingleMultipart(resp *http.Response) (http.Header, []byte, error) {
	mediaType, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil {
		return nil, nil, err
	}
	if !strings.HasPrefix(mediaType, "multipart/") {
		return nil, nil, errors.New("expected multipart/mixed response, got " + mediaType)
	}
	mr := multipart.NewReader(resp.Body, params["boundary"])
	part, err := mr.NextPart()
	if err != nil {
		return nil, nil, err
	}
	defer part.Close()
	body, err := io.ReadAll(part)
	if err != nil {
		return nil, nil, err
	}
	return http.Header(part.Header), body, nil
}

// visibilityExtender periodically extends a lease until stopped, mirroring the
// Node/Python heartbeat so slow handlers don't get their message redelivered.
type visibilityExtender struct {
	lease     *lease
	timeout   time.Duration
	interval  time.Duration
	stop      chan struct{}
	stopped   chan struct{}
	startedAt time.Time
}

func newVisibilityExtender(l *lease, timeout, interval time.Duration) *visibilityExtender {
	return &visibilityExtender{
		lease:    l,
		timeout:  timeout,
		interval: interval,
		stop:     make(chan struct{}),
		stopped:  make(chan struct{}),
	}
}

func (v *visibilityExtender) start() {
	if v.interval <= 0 {
		close(v.stopped)
		return
	}
	go func() {
		defer close(v.stopped)
		t := time.NewTicker(v.interval)
		defer t.Stop()
		for {
			select {
			case <-v.stop:
				return
			case <-t.C:
				base := context.Background()
				if v.lease.token != "" {
					base = ContextWithToken(base, v.lease.token)
				}
				ctx, cancel := context.WithTimeout(base, 10*time.Second)
				err := v.lease.extend(ctx, v.timeout)
				cancel()
				if err != nil {
					// Stop on failure; rely on the current visibility window.
					return
				}
			}
		}
	}()
}

func (v *visibilityExtender) close() {
	select {
	case <-v.stop:
	default:
		close(v.stop)
	}
	<-v.stopped
}

func atoiOr(s string, fallback int) int {
	if s == "" {
		return fallback
	}
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return fallback
	}
	return n
}

func parseTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, s); err == nil {
			return t
		}
	}
	return time.Time{}
}
