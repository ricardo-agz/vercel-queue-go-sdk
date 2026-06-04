package queue

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

// SendResult is returned by a successful publish.
//
// MessageID is empty when the server returns 202 Accepted (deferred delivery),
// in which case Deferred is true.
type SendResult struct {
	MessageID string
	Deferred  bool
}

// sendConfig accumulates per-message options.
type sendConfig struct {
	idempotencyKey string
	delay          time.Duration
	retention      time.Duration
	hasDelay       bool
	hasRetention   bool
	deploymentID   *deploymentID
	headers        map[string]string
}

// SendOption configures a single publish.
type SendOption func(*sendConfig)

// WithIdempotencyKey deduplicates messages sharing the same key.
func WithIdempotencyKey(key string) SendOption {
	return func(s *sendConfig) { s.idempotencyKey = key }
}

// WithDelay defers first delivery of the message by d.
func WithDelay(d time.Duration) SendOption {
	return func(s *sendConfig) {
		s.delay = d
		s.hasDelay = true
	}
}

// WithRetention overrides how long the message is retained.
func WithRetention(d time.Duration) SendOption {
	return func(s *sendConfig) {
		s.retention = d
		s.hasRetention = true
	}
}

// WithSendDeploymentID pins this message to a specific deployment.
func WithSendDeploymentID(id string) SendOption {
	return func(s *sendConfig) { s.deploymentID = &deploymentID{value: id} }
}

// WithSendHeader adds a header to this publish request.
func WithSendHeader(key, value string) SendOption {
	return func(s *sendConfig) {
		if s.headers == nil {
			s.headers = make(map[string]string)
		}
		s.headers[key] = value
	}
}

// Send publishes payload to a topic by name. It is the low-level, untyped
// escape hatch; prefer [Topic.Send] for compile-time payload safety.
//
// payload is JSON-encoded unless it is already a []byte, in which case it is
// sent verbatim with a JSON content type.
func (c *Client) Send(ctx context.Context, topic string, payload any, opts ...SendOption) (*SendResult, error) {
	cfg := sendConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}

	body, err := encodePayload(topic, payload)
	if err != nil {
		return nil, err
	}

	req, err := c.newRequest(ctx, http.MethodPost, c.topicURL(topic), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if cfg.idempotencyKey != "" {
		req.Header.Set("Vqs-Idempotency-Key", cfg.idempotencyKey)
	}
	if cfg.hasDelay {
		req.Header.Set("Vqs-Delay-Seconds", strconv.Itoa(durationSeconds(cfg.delay)))
	}
	if cfg.hasRetention {
		req.Header.Set("Vqs-Retention-Seconds", strconv.Itoa(durationSeconds(cfg.retention)))
	}
	if cfg.deploymentID != nil {
		if id := resolveDeploymentID(*cfg.deploymentID); id != "" {
			req.Header.Set("Vqs-Deployment-Id", id)
		}
	}
	for k, v := range cfg.headers {
		req.Header.Set(k, v)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, &Error{Op: "Send", Topic: topic, err: err}
	}
	defer resp.Body.Close()

	if err := statusError("Send", topic, resp); err != nil {
		return nil, err
	}

	if resp.StatusCode == http.StatusAccepted {
		return &SendResult{Deferred: true}, nil
	}

	var decoded struct {
		MessageID string `json:"messageId"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&decoded); err != nil {
		return nil, &Error{Op: "Send", Topic: topic, StatusCode: resp.StatusCode, err: ErrMessageCorrupted, Message: "missing messageId in response"}
	}
	return &SendResult{MessageID: decoded.MessageID}, nil
}

func encodePayload(topic string, payload any) ([]byte, error) {
	if b, ok := payload.([]byte); ok {
		return b, nil
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, &Error{Op: "Send", Topic: topic, err: err, Message: "failed to encode payload"}
	}
	return b, nil
}

// durationSeconds converts d to whole non-negative seconds.
func durationSeconds(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	return int(d / time.Second)
}
