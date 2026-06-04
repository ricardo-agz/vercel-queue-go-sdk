// Package queue is the Vercel Queues SDK for Go.
//
// It provides a small, net/http-shaped surface for publishing and consuming
// messages on Vercel Queues. The producer is a thin client over the Queue
// Service HTTP API; the consumer is an HTTP server that receives queue
// callbacks and dispatches them to typed handlers.
//
// # Topics
//
// A [Topic] binds a topic name to a payload type. Declare topics once and
// share them between producer and consumer:
//
//	var Emails = queue.NewTopic[EmailPayload]("emails")
//
// # Producing
//
//	client := queue.NewClient()
//	_, err := Emails.Send(ctx, client, EmailPayload{To: "a@b.com"},
//		queue.WithDelay(time.Minute),
//		queue.WithIdempotencyKey("k1"),
//	)
//
// # Consuming
//
//	func handleEmail(ctx context.Context, msg *queue.Message[EmailPayload]) error {
//		return sendEmail(ctx, msg.Payload.To)
//	}
//
//	func main() {
//		mux := queue.NewServeMux()
//		queue.Handle(mux, Emails, handleEmail)
//		log.Fatal(queue.ListenAndServe(context.Background(), mux))
//	}
//
// A handler controls redelivery via its return value:
//
//   - nil acknowledges the message (it is deleted).
//   - [RetryAfter] requests redelivery after a delay.
//   - [Drop] (or wrapping [SkipRetry]) deletes the message without retrying.
//   - any other error triggers the default platform retry.
//
// # Authentication
//
// Calls to the Queue Service are authenticated with a bearer token. On Vercel
// the token is delivered per request as the X-Vercel-Oidc-Token header (the
// VERCEL_OIDC_TOKEN environment variable exists only at build time, so it
// cannot be relied on at runtime).
//
// Producers wrap their HTTP handler with [Middleware], which copies that header
// into the request context so [Topic.Send]/[Client.Send] called with
// r.Context() authenticate automatically:
//
//	log.Fatal(http.ListenAndServe(addr, queue.Middleware(mux)))
//
// Workers need no setup: [ServeMux] extracts the token from each callback
// request and uses it for the message fetch, the handler, and lease operations
// (including background visibility extension).
//
// The resolution order for a token is: an explicit [WithToken] on the client, a
// token carried on the context (via [Middleware]/[ContextWithToken]), the
// VERCEL_QUEUE_TOKEN env var (set by `vercel dev`), then VERCEL_OIDC_TOKEN.
package queue
