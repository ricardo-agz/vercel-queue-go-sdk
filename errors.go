package queue

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Sentinel errors returned by the Queue Service. Use errors.Is to match them
// and errors.As with *Error to inspect status code, retry-after, etc.
var (
	ErrBadRequest              = errors.New("queue: bad request")
	ErrUnauthorized            = errors.New("queue: unauthorized")
	ErrForbidden               = errors.New("queue: forbidden")
	ErrDuplicateIdempotencyKey = errors.New("queue: duplicate idempotency key")
	ErrThrottled               = errors.New("queue: throttled")
	ErrMessageNotFound         = errors.New("queue: message not found")
	ErrMessageNotAvailable     = errors.New("queue: message not available")
	ErrMessageCorrupted        = errors.New("queue: message corrupted")
	ErrServer                  = errors.New("queue: server error")
	ErrNoToken                 = errors.New("queue: no auth token resolved")
)

// Error is a structured error from a Queue Service operation. It wraps one of
// the sentinel errors above so callers can use both errors.Is (coarse) and
// errors.As (detailed).
type Error struct {
	Op         string        // operation: "Send", "receive", "ack", "extend"
	Topic      string        // topic name, when known
	StatusCode int           // HTTP status from the Queue Service
	RetryAfter time.Duration // populated for 429 responses; 0 otherwise
	RequestID  string        // server correlation id, when returned
	Message    string        // server-provided detail, when available
	err        error         // wrapped sentinel
}

func (e *Error) Error() string {
	var b []byte
	b = append(b, "queue: "...)
	if e.Op != "" {
		b = append(b, e.Op...)
		b = append(b, ' ')
	}
	if e.Topic != "" {
		b = append(b, "topic="...)
		b = append(b, e.Topic...)
		b = append(b, ' ')
	}
	if e.err != nil {
		b = append(b, e.err.Error()...)
	}
	if e.StatusCode != 0 {
		b = append(b, fmt.Sprintf(" (%d)", e.StatusCode)...)
	}
	if e.Message != "" {
		b = append(b, ": "...)
		b = append(b, e.Message...)
	}
	return string(b)
}

func (e *Error) Unwrap() error { return e.err }

// IsRetryable reports whether err is worth retrying by a producer. Throttling,
// server errors, and context deadlines are retryable; client (4xx) errors other
// than 429 are not.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrThrottled) || errors.Is(err, ErrServer) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	return false
}
