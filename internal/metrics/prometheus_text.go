package metrics

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
)

const (
	// PrometheusVarModeFull keeps full metric name in Point.Values key.
	PrometheusVarModeFull = "full"
	// PrometheusVarModeShort removes the first prefix segment before '_' from metric name.
	PrometheusVarModeShort = "short"
)

// PrometheusParseConfig controls Prometheus exposition parsing.
// Params: VarMode controls variable naming.
// Returns: parser behavior options.
type PrometheusParseConfig struct {
	VarMode string
}

// PrometheusParser holds precompiled Prometheus parsing config for repeated scrapes.
// Params: var mode is pre-normalized.
// Returns: reusable parser instance.
type PrometheusParser struct {
	varMode string
}

// NewPrometheusParser precompiles Prometheus parse options for repeated parsing.
// Params: cfg parser options.
// Returns: reusable Prometheus parser.
func NewPrometheusParser(cfg PrometheusParseConfig) *PrometheusParser {
	varMode := strings.ToLower(strings.TrimSpace(cfg.VarMode))
	if varMode == "" {
		varMode = PrometheusVarModeFull
	}

	return &PrometheusParser{
		varMode: varMode,
	}
}

// ParsePointsPrometheusFromReader parses Prometheus text exposition into keyed metric points.
// Params: r provides Prometheus text payload; cfg controls selection and key mapping.
// Returns: parsed points or parsing/contract error.
func ParsePointsPrometheusFromReader(r io.Reader, cfg PrometheusParseConfig) ([]Point, error) {
	parser := NewPrometheusParser(cfg)
	return parser.ParseFromReader(r)
}

// ParseFromReader parses Prometheus text exposition into keyed metric points.
// Params: r provides Prometheus text payload.
// Returns: parsed points or parsing/contract error.
func (p *PrometheusParser) ParseFromReader(r io.Reader) ([]Point, error) {
	if r == nil {
		return nil, fmt.Errorf("nil reader")
	}

	lim := &io.LimitedReader{R: r, N: int64(MaxPointsJSONBytes) + 1}
	payload, err := io.ReadAll(lim)
	if err != nil {
		return nil, fmt.Errorf("read Prometheus payload: %w", err)
	}
	if len(payload) > MaxPointsJSONBytes {
		return nil, fmt.Errorf("Prometheus payload exceeds %d bytes", MaxPointsJSONBytes)
	}

	return p.Parse(string(payload))
}

// ParsePointsPrometheus parses Prometheus text exposition into keyed metric points.
// Params: payload contains text exposition; cfg controls selection and key mapping.
// Returns: parsed points or parsing/contract error.
func ParsePointsPrometheus(payload string, cfg PrometheusParseConfig) ([]Point, error) {
	parser := NewPrometheusParser(cfg)
	return parser.Parse(payload)
}

// Parse parses Prometheus text exposition into keyed metric points.
// Params: payload contains text exposition.
// Returns: parsed points or parsing/contract error.
func (p *PrometheusParser) Parse(payload string) ([]Point, error) {
	metricTypes := make(map[string]string)
	pointsByKey := make(map[string]map[string]Value)
	malformedLines := 0

	scanner := bufio.NewScanner(strings.NewReader(payload))
	scanner.Buffer(make([]byte, 0, 64*1024), MaxPointsJSONBytes)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		if strings.HasPrefix(line, "#") {
			parsePrometheusTypeLine(line, metricTypes)
			continue
		}

		metricName, value, err := parsePrometheusSampleLine(line)
		if err != nil {
			malformedLines++
			continue
		}

		metricType := metricTypes[metricName]
		if metricType != "counter" && metricType != "gauge" {
			continue
		}
		varName := metricName
		if p.varMode == PrometheusVarModeShort {
			varName = shortPrometheusMetricName(metricName)
		}

		values := pointsByKey["total"]
		if values == nil {
			values = make(map[string]Value)
			pointsByKey["total"] = values
		}
		current := values[varName]
		current.Raw += value
		current.Kind = KindNumber
		values[varName] = current
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan Prometheus payload: %w", err)
	}

	if len(pointsByKey) == 0 {
		if malformedLines > 0 {
			return nil, fmt.Errorf("no matching Prometheus samples found (malformed lines=%d)", malformedLines)
		}
		return nil, fmt.Errorf("no matching Prometheus samples found")
	}

	keys := make([]string, 0, len(pointsByKey))
	for key := range pointsByKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	points := make([]Point, 0, len(keys))
	for _, key := range keys {
		points = append(points, Point{Key: key, Values: pointsByKey[key]})
	}

	return points, nil
}

// parsePrometheusTypeLine parses '# TYPE <name> <type>' declarations.
// Params: line is one comment line; metricTypes stores parsed type by metric name.
// Returns: none.
func parsePrometheusTypeLine(line string, metricTypes map[string]string) {
	fields := strings.Fields(line)
	if len(fields) != 4 {
		return
	}
	if fields[0] != "#" || strings.ToUpper(fields[1]) != "TYPE" {
		return
	}
	metricName := strings.TrimSpace(fields[2])
	metricType := strings.ToLower(strings.TrimSpace(fields[3]))
	if metricName == "" || metricType == "" {
		return
	}
	metricTypes[metricName] = metricType
}

// parsePrometheusSampleLine parses one sample line.
// Params: line contains metric sample in exposition format.
// Returns: metric name, numeric value, parse error.
func parsePrometheusSampleLine(line string) (string, float64, error) {
	seriesToken, valuePart, err := splitPrometheusSeriesAndValue(line)
	if err != nil {
		return "", 0, err
	}
	if seriesToken == "" || valuePart == "" {
		return "", 0, fmt.Errorf("invalid sample format")
	}

	metricName, err := parsePrometheusSeriesToken(seriesToken)
	if err != nil {
		return "", 0, err
	}

	valueFields := strings.Fields(valuePart)
	if len(valueFields) == 0 {
		return "", 0, fmt.Errorf("missing sample value")
	}

	value, err := strconv.ParseFloat(valueFields[0], 64)
	if err != nil {
		return "", 0, fmt.Errorf("invalid sample value")
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return "", 0, fmt.Errorf("sample value must be finite")
	}

	return metricName, value, nil
}

// splitPrometheusSeriesAndValue splits sample line into series token and numeric/timestamp segment.
// Params: line is one sample line.
// Returns: series token, value segment, parse error.
func splitPrometheusSeriesAndValue(line string) (string, string, error) {
	inBraces := false
	inQuotes := false
	escaped := false

	for idx := 0; idx < len(line); idx++ {
		ch := line[idx]

		if inQuotes {
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == '"' {
				inQuotes = false
			}
			continue
		}

		switch ch {
		case '{':
			inBraces = true
		case '}':
			inBraces = false
		case '"':
			if inBraces {
				inQuotes = true
			}
		case ' ', '\t':
			if !inBraces {
				seriesToken := strings.TrimSpace(line[:idx])
				valuePart := strings.TrimSpace(line[idx+1:])
				return seriesToken, valuePart, nil
			}
		}
	}

	return "", "", fmt.Errorf("missing sample value")
}

// parsePrometheusSeriesToken parses '<metric>{labels}' or '<metric>'.
// Params: token contains metric and optional labels segment.
// Returns: metric name, parse error.
func parsePrometheusSeriesToken(token string) (string, error) {
	openIdx := strings.IndexByte(token, '{')
	if openIdx < 0 {
		name := strings.TrimSpace(token)
		if name == "" {
			return "", fmt.Errorf("empty metric name")
		}
		return name, nil
	}

	closeIdx := strings.LastIndexByte(token, '}')
	if closeIdx <= openIdx || closeIdx != len(token)-1 {
		return "", fmt.Errorf("invalid labels block")
	}

	name := strings.TrimSpace(token[:openIdx])
	if name == "" {
		return "", fmt.Errorf("empty metric name")
	}
	return name, nil
}

// shortPrometheusMetricName strips the first namespace segment from metric name.
// Params: metricName is full Prometheus metric name.
// Returns: shortened metric name or original when no namespace separator is present.
func shortPrometheusMetricName(metricName string) string {
	index := strings.IndexByte(metricName, '_')
	if index <= 0 || index+1 >= len(metricName) {
		return metricName
	}
	return metricName[index+1:]
}
