package metrics

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"
)

// ScriptCollector executes external script and converts JSON stdout into metric points.
// Params: metricName emitted into event.metric and script execution options.
// Returns: SCRIPT collector instance.
type ScriptCollector struct {
	metricName string
	path       string
	timeout    time.Duration
	env        map[string]string
}

// NewScriptCollector creates a SCRIPT collector.
// Params: metricName emitted into event.metric; path script path; timeout execution timeout; env extra environment.
// Returns: configured SCRIPT collector.
func NewScriptCollector(
	metricName string,
	path string,
	timeout time.Duration,
	env map[string]string,
) *ScriptCollector {
	copiedEnv := make(map[string]string, len(env))
	for key, value := range env {
		copiedEnv[key] = value
	}

	return &ScriptCollector{
		metricName: metricName,
		path:       strings.TrimSpace(path),
		timeout:    timeout,
		env:        copiedEnv,
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
	command.Env = mergeEnvironment(c.env)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr

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

	points, err := parseScriptPoints(stdout.Bytes())
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
func parseScriptPoints(payload []byte) ([]Point, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()

	var raw any
	if err := decoder.Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode JSON: %w", err)
	}
	if err := ensureNoExtraJSON(decoder); err != nil {
		return nil, err
	}

	switch value := raw.(type) {
	case map[string]any:
		point, err := parseScriptPoint(value)
		if err != nil {
			return nil, err
		}
		return []Point{point}, nil
	case []any:
		points := make([]Point, 0, len(value))
		for idx, item := range value {
			record, ok := item.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("items[%d] must be an object", idx)
			}
			point, err := parseScriptPoint(record)
			if err != nil {
				return nil, fmt.Errorf("items[%d]: %w", idx, err)
			}
			points = append(points, point)
		}
		return points, nil
	default:
		return nil, fmt.Errorf("root JSON must be object or array")
	}
}

// ensureNoExtraJSON verifies that payload contains exactly one JSON value.
// Params: decoder positioned after first value.
// Returns: error when extra tokens are present.
func ensureNoExtraJSON(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("unexpected extra JSON value after root object")
		}
		return fmt.Errorf("invalid trailing JSON data: %w", err)
	}
	return nil
}

// parseScriptPoint converts one script object into metric Point.
// Params: record object with key/data.
// Returns: parsed point or contract error.
func parseScriptPoint(record map[string]any) (Point, error) {
	keyRaw, ok := record["key"]
	if !ok {
		return Point{}, fmt.Errorf("missing key field")
	}
	key, ok := keyRaw.(string)
	if !ok {
		return Point{}, fmt.Errorf("key must be string")
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return Point{}, fmt.Errorf("key cannot be empty")
	}

	dataRaw, ok := record["data"]
	if !ok {
		return Point{}, fmt.Errorf("missing data field")
	}
	data, ok := dataRaw.(map[string]any)
	if !ok {
		return Point{}, fmt.Errorf("data must be object")
	}
	if len(data) == 0 {
		return Point{}, fmt.Errorf("data cannot be empty")
	}

	values := make(map[string]Value, len(data))
	for varName, raw := range data {
		name := strings.TrimSpace(varName)
		if name == "" {
			return Point{}, fmt.Errorf("data contains empty variable name")
		}

		value, err := parseScriptValue(name, raw)
		if err != nil {
			return Point{}, fmt.Errorf("data.%s: %w", name, err)
		}
		values[name] = value
	}

	return Point{
		Key:    key,
		Values: values,
	}, nil
}

// parseScriptValue converts one script variable payload into Value.
// Params: varName metric variable name; raw variable value.
// Returns: typed value or contract error.
func parseScriptValue(varName string, raw any) (Value, error) {
	switch typed := raw.(type) {
	case bool:
		if typed {
			return Value{Raw: 1, Kind: KindNumber}, nil
		}
		return Value{Raw: 0, Kind: KindNumber}, nil
	case json.Number:
		number, err := parseJSONNumber(typed)
		if err != nil {
			return Value{}, err
		}
		return Value{Raw: number, Kind: inferScriptKind(varName)}, nil
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) {
			return Value{}, fmt.Errorf("number must be finite")
		}
		return Value{Raw: typed, Kind: inferScriptKind(varName)}, nil
	case map[string]any:
		return parseScriptValueObject(varName, typed)
	default:
		return Value{}, fmt.Errorf("unsupported value type %T", raw)
	}
}

// parseScriptValueObject parses extended value object with value/last and optional kind.
// Params: varName metric variable name; raw object payload.
// Returns: typed value or contract error.
func parseScriptValueObject(varName string, raw map[string]any) (Value, error) {
	kind := inferScriptKind(varName)
	if kindRaw, ok := raw["kind"]; ok {
		parsedKind, err := parseScriptKind(kindRaw)
		if err != nil {
			return Value{}, err
		}
		kind = parsedKind
	}

	valueRaw, ok := raw["value"]
	if !ok {
		valueRaw, ok = raw["last"]
	}
	if !ok {
		return Value{}, fmt.Errorf("value object must contain value or last field")
	}

	number, err := parseFlexibleNumber(valueRaw)
	if err != nil {
		return Value{}, err
	}

	return Value{
		Raw:  number,
		Kind: kind,
	}, nil
}

// parseScriptKind parses optional script kind selector.
// Params: raw kind field.
// Returns: parsed ValueKind or error.
func parseScriptKind(raw any) (ValueKind, error) {
	kindString, ok := raw.(string)
	if !ok {
		return KindNumber, fmt.Errorf("kind must be string")
	}

	switch strings.TrimSpace(strings.ToLower(kindString)) {
	case "number", "num", "uint64":
		return KindNumber, nil
	case "percent", "pct", "%", "uint8_percent":
		return KindPercent, nil
	default:
		return KindNumber, fmt.Errorf("unsupported kind %q", kindString)
	}
}

// parseJSONNumber parses json.Number into finite float64.
// Params: raw JSON number.
// Returns: parsed float or error.
func parseJSONNumber(raw json.Number) (float64, error) {
	parsed, err := raw.Float64()
	if err != nil {
		return 0, fmt.Errorf("invalid number %q", raw.String())
	}
	if math.IsNaN(parsed) || math.IsInf(parsed, 0) {
		return 0, fmt.Errorf("number must be finite")
	}
	return parsed, nil
}

// parseFlexibleNumber parses supported numeric representations into float64.
// Params: raw numeric value.
// Returns: parsed float or error.
func parseFlexibleNumber(raw any) (float64, error) {
	switch typed := raw.(type) {
	case json.Number:
		return parseJSONNumber(typed)
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) {
			return 0, fmt.Errorf("number must be finite")
		}
		return typed, nil
	case float32:
		value := float64(typed)
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return 0, fmt.Errorf("number must be finite")
		}
		return value, nil
	case int:
		return float64(typed), nil
	case int8:
		return float64(typed), nil
	case int16:
		return float64(typed), nil
	case int32:
		return float64(typed), nil
	case int64:
		return float64(typed), nil
	case uint:
		return float64(typed), nil
	case uint8:
		return float64(typed), nil
	case uint16:
		return float64(typed), nil
	case uint32:
		return float64(typed), nil
	case uint64:
		return float64(typed), nil
	default:
		return 0, fmt.Errorf("value must be numeric")
	}
}

// inferScriptKind infers percent kind for standard util variable names.
// Params: varName metric variable name.
// Returns: inferred ValueKind.
func inferScriptKind(varName string) ValueKind {
	name := strings.TrimSpace(strings.ToLower(varName))
	if name == "util" || strings.HasSuffix(name, "_util") {
		return KindPercent
	}
	return KindNumber
}
