package pipeline

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"magent/internal/match"
	"magent/internal/metrics"
)

type windowConfig struct {
	Metric      string
	Instance    string
	Percentiles []int
	Tags        EventTags
	Sink        Sink
	Logger      *slog.Logger
	EmitFilter  EmitFilter
	KeepKnown   bool
	DropVar     []string
	FilterVar   []string
	DropEvent   []DropCondition
}

type window struct {
	cfg windowConfig

	buffer map[string]map[string]*series
	known  map[string]map[string]metrics.ValueKind

	filterVarPatterns []match.WildcardPattern
	dropVarPatterns   []match.WildcardPattern

	windowDT uint64
}

func newWindow(cfg windowConfig) *window {
	return &window{
		cfg:               cfg,
		buffer:            make(map[string]map[string]*series),
		known:             make(map[string]map[string]metrics.ValueKind),
		filterVarPatterns: compileWildcardPatterns(cfg.FilterVar),
		dropVarPatterns:   compileWildcardPatterns(cfg.DropVar),
	}
}

func (w *window) appendPoints(points []metrics.Point) bool {
	appended := false

	for _, point := range points {
		key := strings.TrimSpace(point.Key)
		if key == "" {
			key = "total"
		}

		for varName, value := range point.Values {
			valueName := strings.TrimSpace(varName)
			if valueName == "" {
				continue
			}
			if !isVariableAllowedCompiled(valueName, w.filterVarPatterns, w.dropVarPatterns) {
				continue
			}

			keyBuffer, ok := w.buffer[key]
			if !ok {
				keyBuffer = make(map[string]*series)
				w.buffer[key] = keyBuffer
			}
			keyKnown, ok := w.known[key]
			if !ok {
				keyKnown = make(map[string]metrics.ValueKind)
				w.known[key] = keyKnown
			}

			seriesBuffer, ok := keyBuffer[valueName]
			if !ok {
				seriesBuffer = &series{kind: value.Kind}
				keyBuffer[valueName] = seriesBuffer
			}
			seriesBuffer.kind = value.Kind
			seriesBuffer.values = append(seriesBuffer.values, value.Raw)
			keyKnown[valueName] = value.Kind
			appended = true
		}
	}

	return appended
}

func (w *window) observeDT(dtMillis uint64) {
	if w.windowDT == 0 {
		w.windowDT = dtMillis
	}
}

func (w *window) emitWindow(ctx context.Context, dtFallback time.Duration) {
	if len(w.buffer) == 0 && len(w.known) == 0 {
		return
	}

	sendAt := time.Now()
	dts := uint64(sendAt.Unix())
	dt := w.windowDT
	if dt == 0 {
		dt = uint64(sendAt.Add(-dtFallback).UnixMilli())
	}
	if dt/1000 >= dts {
		if dts > 0 {
			dt = (dts - 1) * 1000
		} else if dt > 0 {
			dt--
		}
	}

	for key, vars := range w.known {
		keySamples := w.buffer[key]

		if w.cfg.EmitFilter != nil {
			seriesMap := make(map[string]series, len(vars))
			for varName, kind := range vars {
				currentSeries := series{kind: kind}
				if buffered, ok := keySamples[varName]; ok {
					currentSeries = *buffered
				}
				seriesMap[varName] = currentSeries
			}
			if !w.cfg.EmitFilter(key, seriesMap) {
				continue
			}
		}

		data := make(map[string]map[string]any, len(vars))
		for varName, kind := range vars {
			currentSeries := series{kind: kind}
			if buffered, ok := keySamples[varName]; ok {
				currentSeries = *buffered
			}
			data[varName] = aggregateSeries(currentSeries, w.cfg.Percentiles)
		}

		if shouldDropEvent(
			w.cfg.DropEvent,
			EventEvalContext{
				Metric: w.cfg.Metric,
				Key:    key,
				Data:   data,
			},
		) {
			continue
		}

		event := Event{
			DT:      dt,
			DTS:     dts,
			Metric:  w.cfg.Metric,
			DC:      w.cfg.Tags.DC,
			Host:    w.cfg.Tags.Host,
			Project: w.cfg.Tags.Project,
			Role:    w.cfg.Tags.Role,
			Key:     key,
			Data:    data,
		}

		if err := w.cfg.Sink.Consume(ctx, event); err != nil {
			w.cfg.Logger.Error(
				"emit failed",
				slog.String("metric", w.cfg.Metric),
				slog.String("instance", w.cfg.Instance),
				slog.String("key", key),
				slog.String("error", err.Error()),
			)
		}
	}

	w.buffer = make(map[string]map[string]*series)
	w.windowDT = 0
	if !w.cfg.KeepKnown {
		w.known = make(map[string]map[string]metrics.ValueKind)
	}
}
