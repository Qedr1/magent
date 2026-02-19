package logging

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"regexp"
	"sort"
	"strings"

	"magent/internal/config"
)

const (
	ansiReset  = "\x1b[0m"
	ansiBlue   = "\x1b[34m"
	ansiGreen  = "\x1b[32m"
	ansiYellow = "\x1b[33m"
	ansiCyan   = "\x1b[36m"
	ansiRed    = "\x1b[31m"
	ansiGray   = "\x1b[90m"
)

var (
	stringPattern = regexp.MustCompile(`"[^"\n]*"`)
	ipv4Pattern   = regexp.MustCompile(`\b(?:\d{1,3}\.){3}\d{1,3}\b`)
	numberPattern = regexp.MustCompile(`\b\d+(?:\.\d+)?\b`)
)

// New builds a logger for configured sinks and returns a cleanup function.
// Params: cfg contains console/file sink settings.
// Returns: slog logger, cleanup callback, and setup error.
func New(cfg config.LogConfig) (*slog.Logger, func(), error) {
	var (
		handlers []slog.Handler
		closers  []io.Closer
	)

	if cfg.Console.Enabled {
		handler, err := buildConsoleHandler(cfg.Console)
		if err != nil {
			return nil, nil, fmt.Errorf("build console handler: %w", err)
		}
		handlers = append(handlers, handler)
	}

	if cfg.File.Enabled {
		handler, closer, err := buildFileHandler(cfg.File)
		if err != nil {
			return nil, nil, fmt.Errorf("build file handler: %w", err)
		}
		handlers = append(handlers, handler)
		closers = append(closers, closer)
	}

	if len(handlers) == 0 {
		return nil, nil, fmt.Errorf("no log sinks enabled")
	}

	closeFn := func() {
		for _, closer := range closers {
			_ = closer.Close()
		}
	}

	if len(handlers) == 1 {
		return slog.New(handlers[0]), closeFn, nil
	}

	return slog.New(teeHandler{handlers: handlers}), closeFn, nil
}

// buildConsoleHandler creates a console sink handler.
// Params: sink contains enabled level and format.
// Returns: configured slog handler or error.
func buildConsoleHandler(sink config.LogSinkConfig) (slog.Handler, error) {
	level, err := parseLevel(sink.Level)
	if err != nil {
		return nil, err
	}

	opts := &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
			if attr.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return attr
		},
	}

	switch sink.Format {
	case "line":
		return slog.NewTextHandler(&colorLineWriter{dst: os.Stdout}, opts), nil
	case "json":
		return slog.NewJSONHandler(os.Stdout, opts), nil
	default:
		return nil, fmt.Errorf("unsupported console format %q", sink.Format)
	}
}

// buildFileHandler creates a file sink handler.
// Params: sink contains path, level, and format.
// Returns: handler, file closer, and error.
func buildFileHandler(sink config.LogSinkConfig) (slog.Handler, io.Closer, error) {
	level, err := parseLevel(sink.Level)
	if err != nil {
		return nil, nil, err
	}

	file, err := os.OpenFile(sink.Path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("open file %q: %w", sink.Path, err)
	}

	opts := &slog.HandlerOptions{Level: level}
	switch sink.Format {
	case "line":
		return slog.NewTextHandler(file, opts), file, nil
	case "json":
		return slog.NewJSONHandler(file, opts), file, nil
	default:
		_ = file.Close()
		return nil, nil, fmt.Errorf("unsupported file format %q", sink.Format)
	}
}

// parseLevel converts configuration level into slog.Level.
// Params: value is lower-case log level name.
// Returns: slog level or error.
func parseLevel(value string) (slog.Level, error) {
	switch strings.TrimSpace(strings.ToLower(value)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	case "panic":
		return slog.Level(12), nil
	default:
		return slog.LevelInfo, fmt.Errorf("unsupported level %q", value)
	}
}

// teeHandler fan-outs one record to multiple handlers.
// Params: handlers list to call.
// Returns: composed handler behavior.
type teeHandler struct {
	handlers []slog.Handler
}

// Enabled checks if at least one downstream handler is enabled.
// Params: ctx context and level.
// Returns: true when any sink accepts the level.
func (t teeHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, handler := range t.handlers {
		if handler.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

// Handle forwards the record to all enabled downstream handlers.
// Params: ctx context and record to write.
// Returns: first error if any sink fails.
func (t teeHandler) Handle(ctx context.Context, record slog.Record) error {
	for _, handler := range t.handlers {
		if !handler.Enabled(ctx, record.Level) {
			continue
		}

		if err := handler.Handle(ctx, record.Clone()); err != nil {
			return err
		}
	}
	return nil
}

// WithAttrs applies attrs to each downstream handler.
// Params: attrs to attach.
// Returns: new tee handler with attrs.
func (t teeHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := make([]slog.Handler, 0, len(t.handlers))
	for _, handler := range t.handlers {
		next = append(next, handler.WithAttrs(attrs))
	}
	return teeHandler{handlers: next}
}

// WithGroup applies group to each downstream handler.
// Params: group name.
// Returns: new tee handler with group.
func (t teeHandler) WithGroup(name string) slog.Handler {
	next := make([]slog.Handler, 0, len(t.handlers))
	for _, handler := range t.handlers {
		next = append(next, handler.WithGroup(name))
	}
	return teeHandler{handlers: next}
}

// colorLineWriter wraps console line logs with level-based color.
// Params: dst is output writer.
// Returns: bytes written or write error.
type colorLineWriter struct {
	dst io.Writer
}

// Write colors one line according to level markers.
// Params: payload is rendered slog line.
// Returns: bytes written or write error.
func (w *colorLineWriter) Write(payload []byte) (int, error) {
	line := string(payload)
	levelTone := levelColor(line)
	if levelTone == "" {
		return w.dst.Write(payload)
	}

	renderedLine := highlightLineTokens(line, levelTone)
	rendered := levelTone + renderedLine + ansiReset
	n, err := w.dst.Write([]byte(rendered))
	if n > len(payload) {
		n = len(payload)
	}
	return n, err
}

// levelColor maps rendered level token to ANSI code.
// Params: line is one rendered slog line.
// Returns: ANSI color sequence or empty string.
func levelColor(line string) string {
	switch {
	case strings.Contains(line, "level=DEBUG"):
		return ansiGray
	case strings.Contains(line, "level=INFO"):
		return ansiBlue
	case strings.Contains(line, "level=WARN"):
		return ansiYellow
	case strings.Contains(line, "level=ERROR"):
		return ansiRed
	case strings.Contains(line, "level=PANIC"):
		return ansiRed
	default:
		return ""
	}
}

type colorRegion struct {
	start int
	end   int
	color string
}

type weightedColorRegion struct {
	colorRegion
	priority int
}

// highlightLineTokens applies token-level colors over line while preserving level color as base.
// Params: line rendered line text; baseColor line-level color to restore after token coloring.
// Returns: line text with ANSI token highlights.
func highlightLineTokens(line, baseColor string) string {
	regions := collectColorRegions(line)
	if len(regions) == 0 {
		return line
	}

	var builder strings.Builder
	builder.Grow(len(line) + len(regions)*12)

	cursor := 0
	for _, region := range regions {
		if region.start < cursor {
			continue
		}
		if region.start > len(line) || region.end > len(line) || region.start >= region.end {
			continue
		}

		builder.WriteString(line[cursor:region.start])
		builder.WriteString(region.color)
		builder.WriteString(line[region.start:region.end])
		builder.WriteString(ansiReset)
		builder.WriteString(baseColor)
		cursor = region.end
	}

	builder.WriteString(line[cursor:])
	return builder.String()
}

// collectColorRegions extracts non-overlapping token regions for strings, IPs, and numbers.
// Params: line rendered line text.
// Returns: sorted non-overlapping regions.
func collectColorRegions(line string) []colorRegion {
	weighted := make([]weightedColorRegion, 0, 32)
	weighted = append(weighted, findPatternRegions(line, stringPattern, ansiGreen, 1)...)
	weighted = append(weighted, findPatternRegions(line, ipv4Pattern, ansiCyan, 2)...)
	weighted = append(weighted, findPatternRegions(line, numberPattern, ansiYellow, 3)...)

	sort.SliceStable(weighted, func(i, j int) bool {
		if weighted[i].start == weighted[j].start {
			if weighted[i].priority == weighted[j].priority {
				return weighted[i].end > weighted[j].end
			}
			return weighted[i].priority < weighted[j].priority
		}
		return weighted[i].start < weighted[j].start
	})

	out := make([]colorRegion, 0, len(weighted))
	cursor := 0
	for _, region := range weighted {
		if region.start < cursor {
			continue
		}
		out = append(out, region.colorRegion)
		cursor = region.end
	}
	return out
}

// findPatternRegions finds regexp regions and tags them with color and priority.
// Params: line rendered text; pattern regexp; color ansi color; priority overlap order (lower first).
// Returns: list of matched color regions with priority.
func findPatternRegions(
	line string,
	pattern *regexp.Regexp,
	color string,
	priority int,
) []weightedColorRegion {
	indices := pattern.FindAllStringIndex(line, -1)
	out := make([]weightedColorRegion, 0, len(indices))
	for _, indexPair := range indices {
		out = append(out, weightedColorRegion{
			colorRegion: colorRegion{
				start: indexPair[0],
				end:   indexPair[1],
				color: color,
			},
			priority: priority,
		})
	}
	return out
}
