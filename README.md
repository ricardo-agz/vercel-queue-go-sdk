# queue-go

> [!WARNING]
> This is an experimental project.
> There are no guarantees of support, maintenance, stability, or backward
> compatibility. Use it at your own risk.

The Vercel Queues SDK for Go. A small, `net/http`-shaped API for publishing and
consuming messages on Vercel Queues.

- **Producer:** a thin client over the Queue Service HTTP API.
- **Consumer:** an HTTP server that receives queue callbacks and dispatches them
  to typed handlers.

## Install

```bash
go get github.com/ricardo-agz/vercel-queue-go-sdk
```

## Topics

A `Topic[T]` binds a topic name to a payload type. Declare topics once and share
them between producer and consumer so the name and payload type have a single
source of truth.

```go
var Emails = queue.NewTopic[EmailPayload]("emails")
```

## Producing

Publish from an HTTP handler using the inbound request's context. On Vercel the
Queue Service is authenticated with the per-request OIDC token, which
`queue.Middleware` copies from the `X-Vercel-Oidc-Token` header into the request
context — so pass `r.Context()` and there is no client to construct or thread:

```go
func handleCheckout(w http.ResponseWriter, r *http.Request) {
    _, err := Emails.Send(r.Context(), EmailPayload{To: "a@b.com"},
        queue.WithDelay(time.Minute),
        queue.WithIdempotencyKey("order-123"),
    )
    // ...
}

func main() {
    mux := http.NewServeMux()
    mux.HandleFunc("POST /checkout", handleCheckout)
    log.Fatal(http.ListenAndServe(":"+port(), queue.Middleware(mux)))
}
```

Need a custom client (base URL, HTTP client, deployment pinning)? Bind one with
`Emails.With(client)`, which returns a reusable `*queue.Publisher[EmailPayload]`:

```go
pub := Emails.With(client) // store on a struct, inject into constructors, mock in tests
_, err := pub.Send(ctx, EmailPayload{To: "a@b.com"})
```

`client.Send(ctx, "emails", anyPayload, ...)` remains as a low-level untyped call.

## Consuming

```go
func handleEmail(ctx context.Context, msg *queue.Message[EmailPayload]) error {
    return sendEmail(ctx, msg.Payload.To)
}

func main() {
    mux := queue.NewServeMux()
    queue.Handle(mux, Emails, handleEmail)
    log.Fatal(queue.ListenAndServe(context.Background(), mux))
}
```

`ServeMux` implements `http.Handler`, so you can also mount it on your own
server: `http.ListenAndServe(addr, mux)`. `ListenAndServe` adds `$PORT` binding
and graceful SIGTERM draining.

### Handler return values

| Return | Effect |
| --- | --- |
| `nil` | Acknowledge (delete) the message |
| `queue.RetryAfter(err, d)` | Redeliver after `d` |
| `queue.Drop(err)` (or wrapping `queue.SkipRetry`) | Delete without retrying |
| any other error | Default platform retry |

The lease is auto-extended while a handler runs; use
`msg.ExtendVisibility(ctx, d)` for additional control, and
`msg.Metadata.DeliveryCount` to detect redeliveries.

## Errors

Producer errors wrap sentinels for `errors.Is` and carry detail via `*queue.Error`
(`errors.As`):

```go
if errors.Is(err, queue.ErrDuplicateIdempotencyKey) { /* already enqueued */ }

var qe *queue.Error
if errors.As(err, &qe) && qe.StatusCode == 429 {
    time.Sleep(qe.RetryAfter)
}
```

## Configuration (`vercel.json`)

Only the consumer (worker) service declares topics; the producer just publishes.

```json
{
  "experimentalServices": {
    "web":    { 
      "runtime": "go", 
      "entrypoint": "cmd/server/main.go", 
      "mount": "/" 
    },
    "worker": {
      "type": "job",
      "trigger": "queue",
      "runtime": "go",
      "entrypoint": "cmd/worker/main.go",
      "topics": [
        { "topic": "emails",  "retryAfterSeconds": 60 },
        { "topic": "reports", "retryAfterSeconds": 300 }
      ]
    }
  }
}
```

See [`examples/orders`](./examples/orders) for a complete producer + worker setup.
