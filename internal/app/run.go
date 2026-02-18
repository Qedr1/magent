package app

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"magent/internal/config"
	"magent/internal/logging"
	"magent/internal/pipeline"
)

// Runtime defines runtime inputs required to start the agent.
// Params: ConfigPath points to the TOML configuration file.
// Returns: Runtime value used by Run.
type Runtime struct {
	ConfigPath string
	Reload     <-chan struct{}
}

type engineRunner interface {
	Run(context.Context) error
}

type runDeps struct {
	loadConfig func(string) (*config.Config, error)
	newLogger  func(config.LogConfig) (*slog.Logger, func(), error)
	startPprof func(context.Context, config.PprofConfig, *slog.Logger) (func(), error)
	newEngine  func(context.Context, *config.Config, *slog.Logger) (engineRunner, error)
}

type activeRuntime struct {
	cfg         *config.Config
	logger      *slog.Logger
	closeLogger func()
	cancel      context.CancelFunc
	done        chan error
	stopPprof   func()
}

// Run loads configuration, starts runtime, and supports hot reload via Runtime.Reload.
// Params: ctx controls lifecycle; rt provides runtime inputs and optional reload trigger channel.
// Returns: error on startup/reload failure without rollback, nil on graceful stop.
func Run(ctx context.Context, rt Runtime) error {
	return runWithDeps(ctx, rt, defaultRunDeps())
}

// runWithDeps executes runtime lifecycle using injectable dependencies.
// Params: ctx controls lifecycle; rt runtime inputs; deps start/reload dependencies.
// Returns: runtime error or nil on graceful stop.
func runWithDeps(ctx context.Context, rt Runtime, deps runDeps) error {
	if strings.TrimSpace(rt.ConfigPath) == "" {
		return fmt.Errorf("config path is required")
	}

	active, err := buildRuntimeFromPath(ctx, rt.ConfigPath, deps)
	if err != nil {
		return err
	}

	reloadCh := rt.Reload
	for {
		select {
		case runErr := <-active.done:
			active.done = nil
			active.stopRuntime()

			if ctx.Err() != nil {
				reason := ctx.Err().Error()
				active.logger.Info("agent stopped", slog.String("reason", reason))
				active.closeLoggerSink()
				return nil
			}

			if runErr != nil {
				active.logger.Error("pipeline stopped unexpectedly", slog.String("error", runErr.Error()))
				active.closeLoggerSink()
				return fmt.Errorf("run pipeline: %w", runErr)
			}

			active.logger.Error("pipeline stopped unexpectedly", slog.String("error", "runner exited without context cancellation"))
			active.closeLoggerSink()
			return fmt.Errorf("run pipeline: runner exited without context cancellation")
		case <-ctx.Done():
			active.stopRuntime()
			reason := "canceled"
			if ctx.Err() != nil {
				reason = ctx.Err().Error()
			}
			active.logger.Info("agent stopped", slog.String("reason", reason))
			active.closeLoggerSink()
			return nil
		case _, ok := <-reloadCh:
			if !ok {
				reloadCh = nil
				continue
			}
			if ctx.Err() != nil {
				continue
			}

			next, reloadErr := reloadActiveRuntime(ctx, rt.ConfigPath, active, deps)
			if next == nil {
				return reloadErr
			}
			active = next
			if reloadErr != nil {
				continue
			}
		}
	}
}

// defaultRunDeps provides production runtime dependencies.
// Params: none.
// Returns: dependency set used by Run.
func defaultRunDeps() runDeps {
	return runDeps{
		loadConfig: config.Load,
		newLogger:  logging.New,
		startPprof: startPprofServer,
		newEngine: func(ctx context.Context, cfg *config.Config, logger *slog.Logger) (engineRunner, error) {
			return pipeline.NewFromConfig(ctx, cfg, logger)
		},
	}
}

// buildRuntimeFromPath loads validated config from file path and starts runtime components.
// Params: ctx root lifecycle context; path config file path; deps runtime dependency set.
// Returns: active runtime or startup error.
func buildRuntimeFromPath(ctx context.Context, path string, deps runDeps) (*activeRuntime, error) {
	cfg, err := deps.loadConfig(path)
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	runtime, err := buildRuntimeFromConfig(ctx, cfg, deps, nil, nil)
	if err != nil {
		return nil, err
	}
	return runtime, nil
}

// buildRuntimeFromConfig starts runtime components from already loaded config.
// Params: ctx root lifecycle context; cfg validated config; deps runtime dependency set; logger/closeFn optional logger override.
// Returns: active runtime or startup error.
func buildRuntimeFromConfig(
	ctx context.Context,
	cfg *config.Config,
	deps runDeps,
	logger *slog.Logger,
	closeFn func(),
) (*activeRuntime, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config is nil")
	}
	if ctx.Err() != nil {
		return nil, fmt.Errorf("runtime context canceled: %w", ctx.Err())
	}

	ownsLogger := false
	if logger == nil {
		createdLogger, loggerCloseFn, err := deps.newLogger(cfg.Log)
		if err != nil {
			return nil, fmt.Errorf("init logger: %w", err)
		}
		logger = createdLogger
		closeFn = loggerCloseFn
		ownsLogger = true
	}

	runCtx, cancel := context.WithCancel(ctx)
	stopPprof, err := deps.startPprof(runCtx, cfg.Pprof, logger)
	if err != nil {
		cancel()
		if ownsLogger && closeFn != nil {
			closeFn()
		}
		return nil, fmt.Errorf("start pprof: %w", err)
	}

	engine, err := deps.newEngine(runCtx, cfg, logger)
	if err != nil {
		stopPprof()
		cancel()
		if ownsLogger && closeFn != nil {
			closeFn()
		}
		return nil, fmt.Errorf("build pipeline: %w", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- engine.Run(runCtx)
	}()

	logStartup(logger, cfg)
	return &activeRuntime{
		cfg:         cfg,
		logger:      logger,
		closeLogger: closeFn,
		cancel:      cancel,
		done:        done,
		stopPprof:   stopPprof,
	}, nil
}

// reloadActiveRuntime applies config reload with validation and rollback.
// Params: ctx root lifecycle context; path config file path; active currently running runtime; deps runtime dependency set.
// Returns: active runtime to keep running and optional reload error (non-fatal when rollback succeeds).
func reloadActiveRuntime(
	ctx context.Context,
	path string,
	active *activeRuntime,
	deps runDeps,
) (*activeRuntime, error) {
	active.logger.Info("config reload requested")

	nextCfg, err := deps.loadConfig(path)
	if err != nil {
		active.logger.Error("config reload validation failed", slog.String("error", err.Error()))
		return active, fmt.Errorf("reload config: %w", err)
	}

	nextLogger, nextCloseFn, err := deps.newLogger(nextCfg.Log)
	if err != nil {
		active.logger.Error("config reload logger init failed", slog.String("error", err.Error()))
		return active, fmt.Errorf("init reload logger: %w", err)
	}

	active.stopRuntime()
	nextRuntime, startErr := buildRuntimeFromConfig(ctx, nextCfg, deps, nextLogger, nextCloseFn)
	if startErr == nil {
		active.closeLoggerSink()
		nextRuntime.logger.Info("config reload applied")
		return nextRuntime, nil
	}
	nextCloseFn()
	if ctx.Err() != nil {
		active.logger.Info("config reload interrupted by shutdown")
		return active, nil
	}

	active.logger.Error("config reload apply failed, restoring previous runtime", slog.String("error", startErr.Error()))
	rollbackRuntime, rollbackErr := buildRuntimeFromConfig(ctx, active.cfg, deps, active.logger, active.closeLogger)
	if rollbackErr != nil {
		active.closeLoggerSink()
		return nil, fmt.Errorf("apply reload: %w; rollback failed: %w", startErr, rollbackErr)
	}

	rollbackRuntime.logger.Warn("config reload rejected, previous runtime restored", slog.String("error", startErr.Error()))
	return rollbackRuntime, fmt.Errorf("apply reload: %w", startErr)
}

// stopRuntime stops engine and pprof components while keeping logger open.
// Params: none.
// Returns: none.
func (r *activeRuntime) stopRuntime() {
	if r == nil {
		return
	}
	if r.cancel != nil {
		r.cancel()
		r.cancel = nil
	}
	if r.done != nil {
		<-r.done
		r.done = nil
	}
	if r.stopPprof != nil {
		r.stopPprof()
		r.stopPprof = nil
	}
}

// closeLoggerSink closes active logger resources.
// Params: none.
// Returns: none.
func (r *activeRuntime) closeLoggerSink() {
	if r == nil {
		return
	}
	if r.closeLogger != nil {
		r.closeLogger()
		r.closeLogger = nil
	}
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
