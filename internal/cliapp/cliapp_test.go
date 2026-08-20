package cliapp

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"
)

func noEnv(string) string { return "" }

func TestRunRejectsUnknownFlag(t *testing.T) {
	err := Run(context.Background(), []string{"-bogus"}, noEnv, io.Discard)
	if err == nil {
		t.Fatal("expected an error for an unknown flag, got nil")
	}
}

func TestRunRejectsBadAddr(t *testing.T) {
	err := Run(context.Background(), []string{"-addr", "not-a-valid-address"}, noEnv, io.Discard)
	if err == nil {
		t.Fatal("expected an error for an unlistenable address, got nil")
	}
}

func TestRunStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	var logs bytes.Buffer
	go func() {
		done <- Run(ctx, []string{"-addr", "127.0.0.1:0"}, noEnv, &logs)
	}()

	// Give the listener a moment to come up before asking it to stop.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned an error on graceful shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return within the shutdown timeout")
	}
}
