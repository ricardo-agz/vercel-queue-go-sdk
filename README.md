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

```go
client := queue.NewClient() // configured from the environment

_, err := Emails.Send(ctx, client, EmailPayload{To: "a@b.com"},
    queue.WithDelay(time.Minute),
    queue.WithIdempotencyKey("order-123"),
)
```

`client.Send(ctx, "emails", anyPayload, ...)` is available as a low-level,
untyped escape hatch.

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
      "route": "/" 
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
