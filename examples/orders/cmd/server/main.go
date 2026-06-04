// Command server is the producer (web) service. It enqueues jobs onto Vercel
// Queues in response to HTTP requests.
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"

	"github.com/vercel/queue-go"
	"github.com/vercel/queue-go/examples/orders/topics"
)

func main() {
	client := queue.NewClient()

	http.HandleFunc("GET /", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(indexHTML))
	})

	http.HandleFunc("POST /checkout", func(w http.ResponseWriter, r *http.Request) {
		res, err := topics.Emails.Send(r.Context(), client, topics.EmailPayload{
			To:      "customer@example.com",
			Subject: "Your order is confirmed",
		}, queue.WithIdempotencyKey(r.Header.Get("Idempotency-Key")))
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"messageId": res.MessageID,
			"deferred":  res.Deferred,
		})
	})

	addr := ":" + envOr("PORT", "3000")
	log.Printf("web listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}

const indexHTML = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Vercel Queues - Orders demo</title>
  <style>
    :root { color-scheme: light dark; }
    body {
      font-family: ui-sans-serif, system-ui, -apple-system, sans-serif;
      max-width: 32rem; margin: 4rem auto; padding: 0 1.5rem; line-height: 1.5;
    }
    h1 { font-size: 1.5rem; margin-bottom: 0.25rem; }
    p.sub { color: #6b7280; margin-top: 0; }
    button {
      font-size: 1rem; font-weight: 600; padding: 0.65rem 1.25rem;
      border: 0; border-radius: 0.5rem; background: #000; color: #fff;
      cursor: pointer; transition: opacity 0.15s;
    }
    button:hover { opacity: 0.85; }
    button:disabled { opacity: 0.5; cursor: not-allowed; }
    #result {
      margin-top: 1.5rem; padding: 1rem; border-radius: 0.5rem;
      background: rgba(127,127,127,0.1); display: none; white-space: pre-wrap;
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.85rem;
    }
    #result.show { display: block; }
    .ok { border-left: 3px solid #16a34a; }
    .err { border-left: 3px solid #dc2626; }
  </style>
</head>
<body>
  <h1>Orders demo</h1>
  <p class="sub">Click to enqueue an <code>emails</code> job onto Vercel Queues.</p>
  <button id="checkout">Checkout &amp; enqueue job</button>
  <div id="result"></div>
  <script>
    const btn = document.getElementById('checkout');
    const out = document.getElementById('result');
    btn.addEventListener('click', async () => {
      btn.disabled = true;
      out.className = 'show';
      out.textContent = 'Enqueuing...';
      try {
        const res = await fetch('/checkout', {
          method: 'POST',
          headers: { 'Idempotency-Key': crypto.randomUUID() },
        });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error || ('HTTP ' + res.status));
        out.className = 'show ok';
        out.textContent = 'Enqueued job\n\nmessageId: ' + (data.messageId || '(deferred)') +
          (data.deferred ? '\ndeferred: true' : '');
      } catch (err) {
        out.className = 'show err';
        out.textContent = 'Failed: ' + err.message;
      } finally {
        btn.disabled = false;
      }
    });
  </script>
</body>
</html>`

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
