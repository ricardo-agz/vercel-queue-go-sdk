package queue

import (
	"context"
	"time"
)

// Metadata describes a delivered queue message.
type Metadata struct {
	// MessageID uniquely identifies the message.
	MessageID string
	// Topic is the topic (queue) the message was published to.
	Topic string
	// Consumer is the consumer group receiving the message.
	Consumer string
	// DeliveryCount is the 1-based attempt number. Values > 1 indicate a
	// redelivery; use it to make handlers idempotent or to give up after N.
	DeliveryCount int
	// CreatedAt is when the message was created, if provided by the server.
	CreatedAt time.Time
}

// Message is a typed, delivered queue message handed to a handler.
type Message[T any] struct {
	// Payload is the decoded message body.
	Payload T
	// Metadata describes the delivery.
	Metadata Metadata

	lease *lease
}

// ExtendVisibility extends the message's visibility lease by d, preventing
// redelivery while a long-running handler is still working. The SDK also
// extends the lease automatically; call this only for additional control.
func (m *Message[T]) ExtendVisibility(ctx context.Context, d time.Duration) error {
	if m.lease == nil {
		return nil
	}
	return m.lease.extend(ctx, d)
}
