// Package cliapp wires the hub and the HTTP relay together behind the flags
// and environment variable documented in the README. It is kept separate
// from main so the wiring can be exercised in tests without touching a real
// terminal or OS signals.
package cliapp

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/karen-m361/sse-relay/internal/hub"
	"github.com/karen-m361/sse-relay/internal/relay"
)

// Run parses args, starts the HTTP server, and blocks until ctx is canceled
// or the listener fails. On cancellation every stream is finished first, so
// open SSE subscribers see event: done instead of the connection cutting mid
// frame, and only then does the HTTP server get its shutdown grace period.
func Run(ctx context.Context, args []string, getenv func(string) string, stderr io.Writer) error {
	fs := flag.NewFlagSet("sse-relay", flag.ContinueOnError)
	fs.SetOutput(stderr)
	addr := fs.String("addr", ":8080", "listen address")
	buffer := fs.Int("buffer", hub.DefaultCapacity, "events kept per stream for replay")
	heartbeat := fs.Duration("heartbeat", 15*time.Second, "delay between heartbeat comment frames")
	retry := fs.Duration("retry", 2*time.Second, "reconnect delay advertised in the retry field")
	shutdownTimeout := fs.Duration("shutdown-timeout", 10*time.Second, "grace period for in-flight requests on shutdown")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ln, err := net.Listen("tcp", *addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", *addr, err)
	}

	h := hub.New(*buffer)
	srv := relay.NewServer(h, relay.Config{
		Heartbeat: *heartbeat,
		RetryHint: *retry,
		Token:     getenv("RELAY_TOKEN"),
	})
	httpServer := &http.Server{Handler: srv}

	logger := log.New(stderr, "", log.LstdFlags)
	logger.Printf("sse-relay listening on %s", ln.Addr())

	serveErr := make(chan error, 1)
	go func() { serveErr <- httpServer.Serve(ln) }()

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	case <-ctx.Done():
		logger.Printf("shutting down")
	}

	h.CloseAll()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), *shutdownTimeout)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		return err
	}
	// Shutdown makes the pending Serve call return http.ErrServerClosed;
	// that is the expected outcome of a graceful stop, not a failure.
	if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
