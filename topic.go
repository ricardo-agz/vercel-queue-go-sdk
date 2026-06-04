package queue

import "context"

// Topic binds a topic name to a payload type T. Declare a Topic once (typically
// as a package-level variable) and share it between producer and consumer so
// the name and payload type have a single source of truth.
//
//	var Emails = queue.NewTopic[EmailPayload]("emails")
type Topic[T any] struct {
	name string
}

// NewTopic creates a typed topic with the given name.
func NewTopic[T any](name string) *Topic[T] {
	return &Topic[T]{name: name}
}

// Name returns the topic's name.
func (t *Topic[T]) Name() string { return t.name }

// Send publishes a typed payload to the topic using the default client. The
// payload type is checked at compile time against T.
//
// The auth token is resolved from ctx — on Vercel that is the OIDC token seeded
// onto the request context by [Middleware] — so pass the inbound request's
// context:
//
//	topics.Emails.Send(r.Context(), EmailPayload{To: "a@b.com"})
//
// For a custom client, bind one with [Topic.With].
func (t *Topic[T]) Send(ctx context.Context, payload T, opts ...SendOption) (*SendResult, error) {
	return defaultClient.Send(ctx, t.name, payload, opts...)
}

// With binds the topic to an explicit client, returning a [Publisher] that
// sends to this topic through that client. Use it when you need custom
// configuration (base URL, HTTP client, deployment pinning) instead of the
// default client used by [Topic.Send].
//
// The returned *Publisher[T] is a reusable, injectable capability — store it on
// a struct or pass it to constructors:
//
//	pub := topics.Emails.With(client)
//	pub.Send(ctx, EmailPayload{To: "a@b.com"})
//
// T comes from the receiver, so this stays type-safe (a generic method on
// Client would not compile).
func (t *Topic[T]) With(c *Client) *Publisher[T] {
	return &Publisher[T]{topic: t, client: c}
}

// Publisher sends messages to a single topic through a specific client. Create
// one with [Topic.With]. It is safe for concurrent use.
type Publisher[T any] struct {
	topic  *Topic[T]
	client *Client
}

// Send publishes a typed payload to the publisher's topic through its client.
// The auth token is resolved from ctx, the same as [Topic.Send].
func (p *Publisher[T]) Send(ctx context.Context, payload T, opts ...SendOption) (*SendResult, error) {
	return p.client.Send(ctx, p.topic.name, payload, opts...)
}
