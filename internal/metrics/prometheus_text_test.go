package metrics

import (
	"strings"
	"testing"
)

func TestParsePointsPrometheus_MatchingMetricsAndTypes(t *testing.T) {
	payload := strings.Join([]string{
		`# HELP vector_build_info build info`,
		`# TYPE vector_build_info gauge`,
		`vector_build_info{host="dev",version="0.53.0"} 1`,
		`# TYPE vector_component_received_bytes_total counter`,
		`vector_component_received_bytes_total{host="dev",component_id="in",component_kind="source"} 42`,
		`# TYPE vector_buffer_send_duration_seconds histogram`,
		`vector_buffer_send_duration_seconds_bucket{host="dev",component_id="out",le="+Inf"} 5`,
	}, "\n")

	points, err := ParsePointsPrometheus(payload, PrometheusParseConfig{
		VarMode: PrometheusVarModeFull,
	})
	if err != nil {
		t.Fatalf("ParsePointsPrometheus() error: %v", err)
	}

	if len(points) != 1 {
		t.Fatalf("unexpected point count: got=%d want=1", len(points))
	}

	assertPointValue(t, points, "total", "vector_build_info", 1)
	assertPointValue(t, points, "total", "vector_component_received_bytes_total", 42)
}

func TestParsePointsPrometheus_VarModeShort(t *testing.T) {
	payload := strings.Join([]string{
		`# TYPE vector_buffer_byte_size gauge`,
		`vector_buffer_byte_size{host="dev",component_id="sink"} 128`,
	}, "\n")

	points, err := ParsePointsPrometheus(payload, PrometheusParseConfig{
		VarMode: PrometheusVarModeShort,
	})
	if err != nil {
		t.Fatalf("ParsePointsPrometheus() error: %v", err)
	}

	if len(points) != 1 {
		t.Fatalf("unexpected point count: got=%d want=1", len(points))
	}

	assertPointValue(t, points, "total", "buffer_byte_size", 128)
}

func TestParsePointsPrometheus_NoMatches(t *testing.T) {
	payload := strings.Join([]string{
		`# TYPE vector_buffer_send_duration_seconds histogram`,
		`vector_buffer_send_duration_seconds_bucket{host="dev",le="+Inf"} 5`,
	}, "\n")

	_, err := ParsePointsPrometheus(payload, PrometheusParseConfig{
		VarMode: PrometheusVarModeFull,
	})
	if err == nil {
		t.Fatalf("expected error for missing matching samples")
	}
}

func TestParsePointsPrometheus_LabelValueWithSpaces(t *testing.T) {
	payload := strings.Join([]string{
		`# TYPE vector_build_info gauge`,
		`vector_build_info{host="dev",revision="abc 2026-01-27 21:46:39"} 1`,
	}, "\n")

	points, err := ParsePointsPrometheus(payload, PrometheusParseConfig{
		VarMode: PrometheusVarModeFull,
	})
	if err != nil {
		t.Fatalf("ParsePointsPrometheus() error: %v", err)
	}
	if len(points) != 1 {
		t.Fatalf("unexpected point count: got=%d want=1", len(points))
	}
	assertPointValue(t, points, "total", "vector_build_info", 1)
}

func TestParsePointsPrometheus_DuplicateSeriesSummedByMetricName(t *testing.T) {
	payload := strings.Join([]string{
		`# TYPE vector_requests_total counter`,
		`vector_requests_total{component_id="a"} 10`,
		`vector_requests_total{component_id="b"} 15`,
	}, "\n")

	points, err := ParsePointsPrometheus(payload, PrometheusParseConfig{
		VarMode: PrometheusVarModeFull,
	})
	if err != nil {
		t.Fatalf("ParsePointsPrometheus() error: %v", err)
	}
	if len(points) != 1 {
		t.Fatalf("unexpected point count: got=%d want=1", len(points))
	}
	assertPointValue(t, points, "total", "vector_requests_total", 25)
}

func assertPointValue(t *testing.T, points []Point, key string, varName string, want float64) {
	t.Helper()

	for _, point := range points {
		if point.Key != key {
			continue
		}
		value, ok := point.Values[varName]
		if !ok {
			t.Fatalf("point key=%q missing var %q", key, varName)
		}
		if value.Kind != KindNumber {
			t.Fatalf("point key=%q var=%q has unexpected kind %v", key, varName, value.Kind)
		}
		if value.Raw != want {
			t.Fatalf("point key=%q var=%q value=%v want=%v", key, varName, value.Raw, want)
		}
		return
	}

	t.Fatalf("point with key=%q not found", key)
}
