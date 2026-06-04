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

// Send publishes a typed payload to the topic. The payload type is checked at
// compile time against T.
func (t *Topic[T]) Send(ctx context.Context, c *Client, payload T, opts ...SendOption) (*SendResult, error) {
	return c.Send(ctx, t.name, payload, opts...)
}
