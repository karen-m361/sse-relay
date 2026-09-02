// Command subscribe is a minimal SSE client for sse-relay. It exists to show
// what Last-Event-ID resumption looks like from the consumer side: whenever
// the connection drops, for any reason, it reconnects with the id of the last
// event it actually saw, so a restarted server or a flaky network never turns
// into a gap in the printed output.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {
	base := flag.String("url", "http://localhost:8080", "base URL of the sse-relay server")
	stream := flag.String("stream", "", "stream id to subscribe to (required)")
	lastEventID := flag.Uint64("last-event-id", 0, "resume as if a previous session had already seen up to this event id")
	flag.Parse()

	if *stream == "" {
		fmt.Fprintln(os.Stderr, "-stream is required")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, *base, *stream, *lastEventID, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run subscribes to the stream and reconnects with Last-Event-ID whenever the
// connection drops, until the server reports the stream is done or ctx is
// canceled.
func run(ctx context.Context, base, stream string, lastID uint64, stdout, stderr io.Writer) error {
	retry := 2 * time.Second
	url := strings.TrimRight(base, "/") + "/streams/" + stream + "/events"

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		seen, hint, done, err := subscribeOnce(ctx, url, lastID, stdout)
		if seen > lastID {
			lastID = seen
		}
		if hint > 0 {
			retry = hint
		}
		if done {
			return nil
		}
		if err != nil {
			fmt.Fprintf(stderr, "connection lost after event %d, reconnecting in %s: %v\n", lastID, retry, err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retry):
		}
	}
}

// subscribeOnce makes one HTTP request and reads frames until the body ends.
// It returns the highest event id seen, the retry hint the server advertised,
// whether the stream reported event: done, and any error that ended the read.
func subscribeOnce(ctx context.Context, url string, lastID uint64, stdout io.Writer) (seen uint64, retryHint time.Duration, done bool, err error) {
	seen = lastID

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return seen, 0, false, err
	}
	if lastID > 0 {
		req.Header.Set("Last-Event-ID", strconv.FormatUint(lastID, 10))
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return seen, 0, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return seen, 0, false, fmt.Errorf("unexpected status %s", resp.Status)
	}

	var eventType string
	var dataLines []string
	flush := func() {
		if eventType == "" && len(dataLines) == 0 {
			return
		}
		switch eventType {
		case "done":
			done = true
		case "lagged":
			fmt.Fprintln(stdout, "*** dropped for lagging, resuming from the last id seen ***")
		case "gap":
			fmt.Fprintln(stdout, "*** replay buffer no longer holds everything after the requested id, some history is lost ***")
		default:
			if len(dataLines) > 0 {
				fmt.Fprintln(stdout, strings.Join(dataLines, "\n"))
			}
		}
		eventType = ""
		dataLines = nil
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case line == "":
			flush()
			if done {
				return seen, retryHint, true, nil
			}
		case strings.HasPrefix(line, ":"):
			// heartbeat or other comment frame, nothing to do
		case strings.HasPrefix(line, "id:"):
			if id, perr := strconv.ParseUint(strings.TrimSpace(strings.TrimPrefix(line, "id:")), 10, 64); perr == nil {
				seen = id
			}
		case strings.HasPrefix(line, "event:"):
			eventType = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			dataLines = append(dataLines, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		case strings.HasPrefix(line, "retry:"):
			if ms, perr := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "retry:"))); perr == nil {
				retryHint = time.Duration(ms) * time.Millisecond
			}
		}
	}
	if serr := scanner.Err(); serr != nil {
		return seen, retryHint, false, serr
	}
	return seen, retryHint, false, errors.New("server closed the connection")
}
