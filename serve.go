package queue

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os/signal"
	"syscall"
	"time"
)

// shutdownGraceperiod bounds how long in-flight callbacks have to finish on
// shutdown before the server is forced closed.
const shutdownGracePeriod = 25 * time.Second

// ListenAndServe binds the port from $PORT (default 3000) and serves queue
// callbacks from mux until ctx is canceled or the process receives SIGINT or
// SIGTERM, then drains in-flight handlers gracefully.
func ListenAndServe(ctx context.Context, mux *ServeMux) error {
	ln, err := net.Listen("tcp", ":"+port())
	if err != nil {
		return err
	}
	return Serve(ctx, ln, mux)
}

// Serve serves queue callbacks from mux on the provided listener until ctx is
// canceled or the process receives SIGINT or SIGTERM, then drains in-flight
// handlers gracefully.
func Serve(ctx context.Context, ln net.Listener, mux *ServeMux) error {
	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	signalCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(ln)
	}()

	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-signalCtx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGracePeriod)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return err
		}
		return nil
	}
}
