package queue

import "net/url"

// urlEscape percent-encodes a path segment (topic, consumer, id, handle).
func urlEscape(s string) string {
	return url.PathEscape(s)
}
