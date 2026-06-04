package queue

import (
	"errors"
	"time"
)

// SkipRetry, when wrapped into a handler's returned error, deletes the message
// without retrying. Prefer the [Drop] helper for readability.
var SkipRetry = errors.New("queue: skip retry")

// RetryAfter wraps err to request redelivery of the current message after d.
// Returning it from a handler is the explicit, per-message retry control.
func RetryAfter(err error, d time.Duration) error {
	if d < 0 {
		d = 0
	}
	return &retryAfterError{err: err, after: d}
}

// Drop wraps err to acknowledge (delete) the current message without retrying.
// Use it for permanent failures where a retry would never succeed.
func Drop(err error) error {
	return &dropError{err: err}
}

type retryAfterError struct {
	err   error
	after time.Duration
}

func (e *retryAfterError) Error() string {
	if e.err != nil {
		return "queue: retry after " + e.after.String() + ": " + e.err.Error()
	}
	return "queue: retry after " + e.after.String()
}
func (e *retryAfterError) Unwrap() error { return e.err }

type dropError struct{ err error }

func (e *dropError) Error() string {
	if e.err != nil {
		return "queue: drop: " + e.err.Error()
	}
	return "queue: drop"
}
func (e *dropError) Unwrap() error { return e.err }

// disposition is the outcome the dispatcher applies after running a handler.
type disposition int

const (
	dispositionAck        disposition = iota // delete the message
	dispositionRetryAfter                    // change visibility to a delay
	dispositionRetry                         // leave for default platform retry
)

// classify maps a handler's returned error to a disposition.
func classify(err error) (disposition, time.Duration) {
	if err == nil {
		return dispositionAck, 0
	}
	var ra *retryAfterError
	if errors.As(err, &ra) {
		return dispositionRetryAfter, ra.after
	}
	var de *dropError
	if errors.As(err, &de) {
		return dispositionAck, 0
	}
	if errors.Is(err, SkipRetry) {
		return dispositionAck, 0
	}
	return dispositionRetry, 0
}
