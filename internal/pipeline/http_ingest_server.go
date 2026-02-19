package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// httpIngestServer runs an HTTP server tied to a lifecycle context.
// Params: listen address, handler, and logger for diagnostics.
// Returns: runnable HTTP server instance.
type httpIngestServer struct {
	listen string
	ln     net.Listener
	server *http.Server
	logger *slog.Logger
}

// newHTTPIngestServer creates an HTTP server and binds to the listen address.
// Params: listen address in host:port; handler HTTP handler; logger root logger.
// Returns: server instance or bind error.
func newHTTPIngestServer(listen string, handler http.Handler, logger *slog.Logger) (*httpIngestServer, error) {
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, fmt.Errorf("listen %q: %w", listen, err)
	}

	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	return &httpIngestServer{
		listen: listen,
		ln:     ln,
		server: server,
		logger: logger,
	}, nil
}

// run starts serving and shuts down on context cancellation.
// Params: ctx lifecycle context.
// Returns: nil on graceful stop; error on early serve failures.
func (s *httpIngestServer) run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.server.Serve(s.ln)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.server.Shutdown(shutdownCtx)
		err := <-errCh
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case err := <-errCh:
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		s.logger.Error("http ingest server stopped unexpectedly", slog.String("listen", s.listen), slog.String("error", err.Error()))
		return err
	}
}
