package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	pprofhttp "net/http/pprof"
	"sync"
	"time"

	"magent/internal/config"
)

const (
	pprofShutdownTimeout = 3 * time.Second
	pprofReadHeaderTO    = 2 * time.Second
)

// startPprofServer starts optional pprof HTTP endpoint and wires graceful shutdown.
// Params: ctx controls lifecycle; cfg provides enabled/listen options; logger reports runtime events.
// Returns: stop function (idempotent) and startup error.
func startPprofServer(ctx context.Context, cfg config.PprofConfig, logger *slog.Logger) (func(), error) {
	if !cfg.Enabled {
		return func() {}, nil
	}

	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return nil, fmt.Errorf("listen %q: %w", cfg.Listen, err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprofhttp.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprofhttp.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprofhttp.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprofhttp.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprofhttp.Trace)

	server := &http.Server{
		Addr:              cfg.Listen,
		Handler:           mux,
		ReadHeaderTimeout: pprofReadHeaderTO,
	}

	var once sync.Once
	stop := func() {
		once.Do(func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), pprofShutdownTimeout)
			defer cancel()
			if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Warn("pprof shutdown error", slog.String("error", err.Error()))
			}
		})
	}

	go func() {
		<-ctx.Done()
		stop()
	}()

	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("pprof server failed", slog.String("addr", cfg.Listen), slog.String("error", err.Error()))
		}
	}()

	logger.Info("pprof server started", slog.String("addr", cfg.Listen))
	return stop, nil
}
