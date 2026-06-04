// Command worker is the consumer (worker) service. It receives queue callbacks
// and dispatches them to typed handlers.
package main

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/vercel/queue-go"
	"github.com/vercel/queue-go/examples/orders/topics"
)

func main() {
	mux := queue.NewServeMux()
	queue.Handle(mux, topics.Emails, handleEmail)
	queue.Handle(mux, topics.Reports, handleReport)
	log.Fatal(queue.ListenAndServe(context.Background(), mux))
}

func handleEmail(ctx context.Context, msg *queue.Message[topics.EmailPayload]) error {
	err := sendEmail(ctx, msg.Payload.To, msg.Payload.Subject)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, errRateLimited):
		return queue.RetryAfter(err, time.Minute) // back off, redeliver later
	case errors.Is(err, errInvalidAddress):
		return queue.Drop(err) // permanent: ack without retrying
	default:
		return err // default platform retry
	}
}

func handleReport(ctx context.Context, msg *queue.Message[topics.ReportPayload]) error {
	log.Printf("generating report %s (attempt %d)", msg.Payload.ReportID, msg.Metadata.DeliveryCount)
	return generateReport(ctx, msg.Payload.ReportID)
}

var (
	errRateLimited    = errors.New("rate limited")
	errInvalidAddress = errors.New("invalid address")
)

func sendEmail(_ context.Context, to, _ string) error {
	if to == "" {
		return errInvalidAddress
	}
	return nil
}

func generateReport(_ context.Context, _ string) error { return nil }
