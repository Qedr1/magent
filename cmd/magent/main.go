package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"magent/internal/app"
)

const (
	exitCodeFailure = 1
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

// run starts the agent process.
// Params: none.
// Returns: process exit code.
func run() int {
	var (
		configPath string
		showInfo   bool
	)

	flag.StringVar(&configPath, "config", "config.toml", "path to TOML config")
	flag.BoolVar(&showInfo, "v", false, "show build information")
	flag.BoolVar(&showInfo, "version", false, "show build information")
	flag.Parse()

	if showInfo {
		fmt.Printf("magent version=%s commit=%s date=%s\n", version, commit, date)
		return 0
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := app.Run(ctx, app.Runtime{ConfigPath: configPath}); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return exitCodeFailure
	}

	return 0
}

func main() {
	os.Exit(run())
}
