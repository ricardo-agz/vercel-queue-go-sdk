package queue

import (
	"context"
	"net/http"
)

// oidcHeaderName is the request header carrying the Vercel OIDC token on inbound
// requests to a deployment (both producer requests and queue callbacks). In
// production this is the credential used to call the Queue Service; the
// VERCEL_OIDC_TOKEN environment variable is only populated at build time.
const oidcHeaderName = "X-Vercel-Oidc-Token"

type tokenContextKey struct{}

// ContextWithToken returns a copy of ctx carrying an explicit auth token. The
// Client prefers this token over environment variables, which is how a
// per-request Vercel OIDC token is threaded into Send and lease operations.
func ContextWithToken(ctx context.Context, token string) context.Context {
	if token == "" {
		return ctx
	}
	return context.WithValue(ctx, tokenContextKey{}, token)
}

// tokenFromContext returns the token stored with ContextWithToken, or "".
func tokenFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(tokenContextKey{}).(string); ok {
		return v
	}
	return ""
}

// OIDCTokenFromRequest extracts the Vercel OIDC token from an inbound request.
// On Vercel the token is provided per request as the X-Vercel-Oidc-Token header.
// It returns "" when absent (for example local development without OIDC).
func OIDCTokenFromRequest(r *http.Request) string {
	if r == nil {
		return ""
	}
	return r.Header.Get(oidcHeaderName)
}

// Middleware injects the inbound request's Vercel OIDC token into the request
// context so that Client and Topic Send calls made with r.Context()
// authenticate automatically. Wrap a producer's HTTP handler with it:
//
//	log.Fatal(http.ListenAndServe(addr, queue.Middleware(mux)))
//
// Workers do not need this: ServeMux extracts the token from each callback
// request on its own.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token := OIDCTokenFromRequest(r); token != "" {
			r = r.WithContext(ContextWithToken(r.Context(), token))
		}
		next.ServeHTTP(w, r)
	})
}
