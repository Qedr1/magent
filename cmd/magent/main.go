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

	flag.StringVar(&configPath, "config", "config.toml", "path to TOML config file or directory")
	flag.BoolVar(&showInfo, "v", false, "show build information")
	flag.BoolVar(&showInfo, "version", false, "show build information")
	flag.Parse()

	if showInfo {
		fmt.Printf("magent version=%s commit=%s date=%s\n", version, commit, date)
		return 0
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reloadSignal := make(chan os.Signal, 1)
	signal.Notify(reloadSignal, syscall.SIGHUP)
	defer signal.Stop(reloadSignal)

	reload := make(chan struct{}, 1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-reloadSignal:
				select {
				case reload <- struct{}{}:
				default:
				}
			}
		}
	}()

	if err := app.Run(ctx, app.Runtime{ConfigPath: configPath, Reload: reload}); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return exitCodeFailure
	}

	return 0
}

func main() {
	os.Exit(run())
}
