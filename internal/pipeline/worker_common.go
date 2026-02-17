package pipeline

import (
	"fmt"
	"log/slog"
	"strings"
)

type workerWindowBaseConfig struct {
	Metric      string
	Instance    string
	Percentiles []int
	Tags        EventTags
	EmitFilter  EmitFilter
	KeepKnown   bool
	DropVar     []string
	FilterVar   []string
	DropEvent   []DropCondition
}

// buildWorkerWindow validates shared worker fields and builds window config.
// Params: cfg shared worker fields; sink event sink; logger root logger.
// Returns: window instance and normalized worker instance name.
func buildWorkerWindow(
	cfg workerWindowBaseConfig,
	sink Sink,
	logger *slog.Logger,
) (*window, string, error) {
	if sink == nil {
		return nil, "", fmt.Errorf("sink is required")
	}
	if logger == nil {
		return nil, "", fmt.Errorf("logger is required")
	}
	for idx, expression := range cfg.DropEvent {
		if strings.TrimSpace(expression.Raw) == "" {
			return nil, "", fmt.Errorf("drop_event[%d] cannot be empty", idx)
		}
	}

	instance := strings.TrimSpace(cfg.Instance)
	if instance == "" {
		instance = "default"
	}

	return newWindow(windowConfig{
		Metric:      cfg.Metric,
		Instance:    instance,
		Percentiles: cfg.Percentiles,
		Tags:        cfg.Tags,
		Sink:        sink,
		Logger:      logger,
		EmitFilter:  cfg.EmitFilter,
		KeepKnown:   cfg.KeepKnown,
		DropVar:     cfg.DropVar,
		FilterVar:   cfg.FilterVar,
		DropEvent:   cfg.DropEvent,
	}), instance, nil
}
