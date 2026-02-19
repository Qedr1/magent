package metrics

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"
)

type cappedBuffer struct {
	buffer bytes.Buffer
	max    int
}

// Write appends data up to configured cap and silently drops the rest.
// Params: payload chunk bytes.
// Returns: consumed input size to keep writer contract for command pipes.
func (b *cappedBuffer) Write(payload []byte) (int, error) {
	if b.max <= 0 || b.buffer.Len() >= b.max {
		return len(payload), nil
	}

	remaining := b.max - b.buffer.Len()
	if len(payload) > remaining {
		_, _ = b.buffer.Write(payload[:remaining])
		return len(payload), nil
	}

	_, _ = b.buffer.Write(payload)
	return len(payload), nil
}

// Bytes returns buffered bytes.
// Params: none.
// Returns: current buffer content.
func (b *cappedBuffer) Bytes() []byte {
	return b.buffer.Bytes()
}

// String returns buffered text.
// Params: none.
// Returns: current buffer text.
func (b *cappedBuffer) String() string {
	return b.buffer.String()
}

// ScriptCollector executes external script and converts JSON stdout into metric points.
// Params: metricName emitted into event.metric and script execution options.
// Returns: SCRIPT collector instance.
type ScriptCollector struct {
	metricName string
	path       string
	timeout    time.Duration
	commandEnv []string
	format     string
	prometheus *PrometheusParser
}

// NewScriptCollector creates a SCRIPT collector.
// Params: metricName emitted into event.metric; path script path; timeout execution timeout; env extra environment.
// Returns: configured SCRIPT collector.
func NewScriptCollector(
	metricName string,
	path string,
	timeout time.Duration,
	env map[string]string,
	options HTTPClientCollectorOptions,
) *ScriptCollector {
	format := strings.ToLower(strings.TrimSpace(options.Format))
	if format == "" {
		format = "json"
	}
	varMode := strings.ToLower(strings.TrimSpace(options.VarMode))
	if varMode == "" {
		varMode = PrometheusVarModeFull
	}

	return &ScriptCollector{
		metricName: metricName,
		path:       strings.TrimSpace(path),
		timeout:    timeout,
		commandEnv: mergeEnvironment(env),
		format:     format,
		prometheus: NewPrometheusParser(PrometheusParseConfig{
			VarMode: varMode,
		}),
	}
}

// Name returns logical metric name.
// Params: none.
// Returns: metric name string.
func (c *ScriptCollector) Name() string {
	return c.metricName
}

// Scrape runs script and parses stdout JSON into keyed metric points.
// Params: ctx for cancellation.
// Returns: parsed points or execution/parse error.
func (c *ScriptCollector) Scrape(ctx context.Context) ([]Point, error) {
	runCtx := ctx
	if c.timeout > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	command := exec.CommandContext(runCtx, c.path)
	command.Env = c.commandEnv

	stdout := &cappedBuffer{max: MaxPointsJSONBytes + 1}
	stderr := &cappedBuffer{max: 8 * 1024}
	command.Stdout = stdout
	command.Stderr = stderr

	err := command.Run()
	if err != nil {
		if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
			return nil, fmt.Errorf("script %q timed out after %s", c.path, c.timeout)
		}

		stderrText := strings.TrimSpace(stderr.String())
		if stderrText == "" {
			return nil, fmt.Errorf("run script %q: %w", c.path, err)
		}
		return nil, fmt.Errorf("run script %q: %w (stderr: %s)", c.path, err, stderrText)
	}

	var points []Point
	switch c.format {
	case "prometheus":
		points, err = c.prometheus.ParseFromReader(bytes.NewReader(stdout.Bytes()))
	default:
		points, err = ParsePointsJSON(stdout.Bytes())
	}
	if err != nil {
		return nil, fmt.Errorf("parse script %q stdout: %w", c.path, err)
	}

	if len(points) == 0 {
		return nil, fmt.Errorf("script %q returned empty result", c.path)
	}

	return points, nil
}

// mergeEnvironment builds command environment with overrides from config.
// Params: overrides key-value map.
// Returns: process environment slice.
func mergeEnvironment(overrides map[string]string) []string {
	out := make([]string, 0, len(os.Environ())+len(overrides))
	out = append(out, os.Environ()...)

	keys := make([]string, 0, len(overrides))
	for key := range overrides {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		out = append(out, key+"="+overrides[key])
	}

	return out
}

// parseScriptPoints parses script stdout payload into metric points.
// Params: payload raw stdout bytes.
// Returns: parsed points or contract error.
func parseScriptPoints(payload []byte, format string, promCfg PrometheusParseConfig) ([]Point, error) {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "prometheus":
		return ParsePointsPrometheusFromReader(bytes.NewReader(payload), promCfg)
	default:
		return ParsePointsJSON(payload)
	}
}
