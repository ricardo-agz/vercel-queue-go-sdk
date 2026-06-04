package queue

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

// ServeMux routes queue callbacks to registered handlers by topic. It
// implements [http.Handler], so it can be served by [ListenAndServe], by
// [Serve], or mounted on any net/http server.
type ServeMux struct {
	mu       sync.RWMutex
	handlers map[string]func(ctx context.Context, d *delivery) error

	client            *Client
	visibilityTimeout time.Duration
	refreshInterval   time.Duration
	logger            *log.Logger
}

// ServeMuxOption configures a ServeMux.
type ServeMuxOption func(*ServeMux)

// WithConsumerClient sets the client used for lease operations (ack, extend,
// fetch). By default a zero-config client is created from the environment.
func WithConsumerClient(c *Client) ServeMuxOption {
	return func(m *ServeMux) {
		if c != nil {
			m.client = c
		}
	}
}

// WithVisibilityTimeout sets the per-message visibility lease duration.
func WithVisibilityTimeout(d time.Duration) ServeMuxOption {
	return func(m *ServeMux) { m.visibilityTimeout = d }
}

// WithRefreshInterval sets how often the lease is auto-extended while a handler
// runs. A non-positive value disables auto-extension.
func WithRefreshInterval(d time.Duration) ServeMuxOption {
	return func(m *ServeMux) { m.refreshInterval = d }
}

// WithLogger sets the logger used for dispatch errors.
func WithLogger(l *log.Logger) ServeMuxOption {
	return func(m *ServeMux) {
		if l != nil {
			m.logger = l
		}
	}
}

// NewServeMux creates an empty ServeMux.
func NewServeMux(opts ...ServeMuxOption) *ServeMux {
	m := &ServeMux{
		handlers:          make(map[string]func(ctx context.Context, d *delivery) error),
		client:            NewClient(),
		visibilityTimeout: defaultVisibilityTimeout,
		refreshInterval:   defaultRefreshInterval,
		logger:            log.Default(),
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Topics returns the set of registered topic names.
func (m *ServeMux) Topics() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	topics := make([]string, 0, len(m.handlers))
	for t := range m.handlers {
		topics = append(topics, t)
	}
	return topics
}

func (m *ServeMux) register(topic string, fn func(ctx context.Context, d *delivery) error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.handlers[topic]; exists {
		panic("queue: duplicate handler registered for topic " + topic)
	}
	m.handlers[topic] = fn
}

func (m *ServeMux) lookup(topic string) (func(ctx context.Context, d *delivery) error, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	fn, ok := m.handlers[topic]
	return fn, ok
}

// delivery carries a resolved message into a registered handler.
type delivery struct {
	body  []byte
	meta  Metadata
	lease *lease
}

// Handle registers a typed handler for a topic. Because Go methods cannot have
// type parameters, this is a package-level function rather than a method.
func Handle[T any](mux *ServeMux, t *Topic[T], h func(context.Context, *Message[T]) error) {
	HandleFunc(mux, t.name, h)
}

// HandleFunc registers a typed handler for a topic name. It is the low-level
// escape hatch used when you don't have a [Topic] value.
func HandleFunc[T any](mux *ServeMux, topic string, h func(context.Context, *Message[T]) error) {
	mux.register(topic, func(ctx context.Context, d *delivery) error {
		var payload T
		if len(d.body) > 0 {
			if err := json.Unmarshal(d.body, &payload); err != nil {
				// A malformed payload is a poison message; surface it as a
				// normal error so the platform retries and eventually
				// dead-letters via max-delivery, rather than silently dropping.
				return &Error{Op: "decode", Topic: topic, err: ErrMessageCorrupted, Message: err.Error()}
			}
		}
		return h(ctx, &Message[T]{Payload: payload, Metadata: d.meta, lease: d.lease})
	})
}

// ServeHTTP implements http.Handler.
func (m *ServeMux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && r.URL.Path == "/" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
		return
	}
	if r.Method != http.MethodPost || !isQueueCallback(r) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	m.dispatch(w, r)
}

func (m *ServeMux) dispatch(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read body"})
		return
	}

	pc, err := parseCallback(r, body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	handler, ok := m.lookup(pc.topic)
	if !ok {
		// No handler for this topic: respond 5xx so the mismatch is visible in
		// logs and the message is retried rather than silently consumed.
		m.logger.Printf("queue: no handler registered for topic %q (consumer %q)", pc.topic, pc.consumer)
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": "no-matching-subscriber",
			"topic": pc.topic,
		})
		return
	}

	ctx := r.Context()

	// Resolve the payload + receipt handle. Metadata-only callbacks require a
	// follow-up fetch.
	bodyBytes := pc.body
	receipt := pc.receiptHandle
	deliveryCount := pc.deliveryCount
	createdAt := pc.createdAt
	if !pc.inline || receipt == "" {
		raw, err := m.client.receiveByID(ctx, pc.topic, pc.consumer, pc.messageID, m.visibilityTimeout)
		if err != nil {
			m.replyError(w, "receive", err)
			return
		}
		bodyBytes = raw.body
		receipt = raw.receiptHandle
		deliveryCount = raw.deliveryCount
		createdAt = raw.createdAt
	}

	l := &lease{
		client:        m.client,
		topic:         pc.topic,
		consumer:      pc.consumer,
		messageID:     pc.messageID,
		receiptHandle: receipt,
	}

	extender := newVisibilityExtender(l, m.visibilityTimeout, m.refreshInterval)
	extender.start()

	d := &delivery{
		body: bodyBytes,
		meta: Metadata{
			MessageID:     pc.messageID,
			Topic:         pc.topic,
			Consumer:      pc.consumer,
			DeliveryCount: deliveryCount,
			CreatedAt:     createdAt,
		},
		lease: l,
	}

	handlerErr := safeInvoke(handler, ctx, d)

	// Stop auto-extension before the terminal lease op so they cannot race.
	extender.close()

	disp, after := classify(handlerErr)
	switch disp {
	case dispositionAck:
		if err := l.ack(ctx); err != nil {
			m.replyError(w, "ack", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})
	case dispositionRetryAfter:
		if err := l.extend(ctx, after); err != nil {
			m.replyError(w, "extend", err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "retryAfterSeconds": durationSeconds(after)})
	default: // dispositionRetry
		m.logger.Printf("queue: handler for topic %q failed (message %s, attempt %d): %v",
			pc.topic, pc.messageID, deliveryCount, handlerErr)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "handler-error"})
	}
}

// safeInvoke runs a handler, converting a panic into a retryable error so a
// single bad message can't crash the instance and its other in-flight work.
func safeInvoke(handler func(ctx context.Context, d *delivery) error, ctx context.Context, d *delivery) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &Error{Op: "handler", Topic: d.meta.Topic, Message: "panic in handler"}
			log.Printf("queue: recovered panic in handler for topic %q: %v", d.meta.Topic, r)
		}
	}()
	return handler(ctx, d)
}

func (m *ServeMux) replyError(w http.ResponseWriter, op string, err error) {
	m.logger.Printf("queue: %s error: %v", op, err)
	status := http.StatusInternalServerError
	var qe *Error
	if errors.As(err, &qe) && qe.StatusCode != 0 {
		status = qe.StatusCode
	}
	writeJSON(w, status, map[string]string{"error": op})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
