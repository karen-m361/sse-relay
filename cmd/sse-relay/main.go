// Command sse-relay is the cmd/ mirror of the module-root binary, for
// callers that install by package path (github.com/karen-m361/sse-relay/cmd/sse-relay)
// rather than by module path. Both build the same program; see the README
// for the primary "go install .../sse-relay@latest" form.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/karen-m361/sse-relay/internal/cliapp"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := cliapp.Run(ctx, os.Args[1:], os.Getenv, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
