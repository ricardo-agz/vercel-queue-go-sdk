package queue

import (
	"context"
	"fmt"
	"os"
	"strings"
)

// Environment variables read by the SDK. These mirror the Vercel runtime and
// the TypeScript/Python clients so behavior is consistent across runtimes.
const (
	envQueueToken    = "VERCEL_QUEUE_TOKEN"
	envOIDCToken     = "VERCEL_OIDC_TOKEN"
	envQueueBaseURL  = "VERCEL_QUEUE_BASE_URL"
	envQueueBasePath = "VERCEL_QUEUE_BASE_PATH"
	envRegion        = "VERCEL_REGION"
	envDeploymentID  = "VERCEL_DEPLOYMENT_ID"
	envPort          = "PORT"

	defaultBaseHost = "https://vercel-queue.com"
	defaultBasePath = "/api/v3/topic"

	// devQueueToken is the local token injected by `vercel dev`. When present,
	// deployment pinning is disabled (matching the TS/Python clients).
	devQueueToken = "vc-dev-token"
)

// resolveBaseURL returns the base URL for the Queue Service API.
//
// Resolution order: explicit override, VERCEL_QUEUE_BASE_URL, region-specific
// endpoint derived from VERCEL_REGION, then the default host.
func resolveBaseURL(override string) string {
	if override != "" {
		return strings.TrimRight(override, "/")
	}
	if v := os.Getenv(envQueueBaseURL); v != "" {
		return strings.TrimRight(v, "/")
	}
	if region := os.Getenv(envRegion); region != "" {
		return fmt.Sprintf("https://%s.vercel-queue.com", region)
	}
	return defaultBaseHost
}

// resolveBasePath returns the base path for the v3 topic endpoints.
func resolveBasePath(override string) string {
	path := override
	if path == "" {
		path = os.Getenv(envQueueBasePath)
	}
	if path == "" {
		path = defaultBasePath
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return path
}

// resolveToken resolves the bearer token used to authenticate with the Queue
// Service.
//
// Order: explicit override, a per-request token carried on ctx (the Vercel OIDC
// token extracted from the inbound X-Vercel-Oidc-Token header), the
// VERCEL_QUEUE_TOKEN env var (set by `vercel dev`), then the VERCEL_OIDC_TOKEN
// env var. VERCEL_OIDC_TOKEN is only present at build time on Vercel, so in
// production the ctx token is the credential that actually authenticates.
func resolveToken(ctx context.Context, override string) (string, error) {
	if override != "" {
		return override, nil
	}
	if v := tokenFromContext(ctx); v != "" {
		return v, nil
	}
	if v := os.Getenv(envQueueToken); v != "" {
		return v, nil
	}
	if v := os.Getenv(envOIDCToken); v != "" {
		return v, nil
	}
	return "", &Error{Op: "auth", err: ErrNoToken}
}

// deploymentPinningDisabled reports whether deployment IDs should be omitted.
// This is the case under `vercel dev` (identified by the dev token).
func deploymentPinningDisabled() bool {
	return os.Getenv(envQueueToken) == devQueueToken
}

// resolveDeploymentID resolves the deployment ID header value.
//
// The zero value of deploymentID (the empty string with pinExplicit false)
// means "auto": use VERCEL_DEPLOYMENT_ID when available. An explicitly set id
// is always used; explicitly disabling pinning returns "".
func resolveDeploymentID(d deploymentID) string {
	if deploymentPinningDisabled() || d.disabled {
		return ""
	}
	if d.value != "" {
		return d.value
	}
	return os.Getenv(envDeploymentID)
}

// deploymentID captures the three states of deployment pinning: auto (zero
// value), an explicit id, or explicitly disabled.
type deploymentID struct {
	value    string
	disabled bool
}

func port() string {
	if p := os.Getenv(envPort); p != "" {
		return p
	}
	return "3000"
}
