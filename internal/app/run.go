package app

import (
	"context"
	"fmt"
	"log/slog"

	"magent/internal/config"
	"magent/internal/logging"
	"magent/internal/pipeline"
)

// Runtime defines runtime inputs required to start the agent.
// Params: ConfigPath points to the TOML configuration file.
// Returns: Runtime value used by Run.
type Runtime struct {
	ConfigPath string
}

// Run loads configuration, starts logging, and blocks until context cancellation.
// Params: ctx controls lifecycle; rt provides runtime inputs.
// Returns: error on startup failure, nil on graceful stop.
func Run(ctx context.Context, rt Runtime) error {
	cfg, err := config.Load(rt.ConfigPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger, closeFn, err := logging.New(cfg.Log)
	if err != nil {
		return fmt.Errorf("init logger: %w", err)
	}
	defer closeFn()

	logStartup(logger, cfg)

	engine, err := pipeline.NewFromConfig(ctx, cfg, logger)
	if err != nil {
		return fmt.Errorf("build pipeline: %w", err)
	}

	if err := engine.Run(ctx); err != nil {
		return fmt.Errorf("run pipeline: %w", err)
	}

	logger.Info("agent stopped", slog.String("reason", ctx.Err().Error()))
	return nil
}

// logStartup emits initial startup metadata.
// Params: logger is initialized slog logger; cfg is validated runtime config.
// Returns: none.
func logStartup(logger *slog.Logger, cfg *config.Config) {
	logger.Info(
		"agent started",
		slog.String("dc", cfg.Global.DC),
		slog.String("project", cfg.Global.Project),
		slog.String("role", cfg.Global.Role),
		slog.String("host", cfg.Global.Host),
		slog.Int("collectors", len(cfg.Collector)),
	)
}
