package metrics

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHTTPClientCollectorScrapeJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"key":"total","data":{"util":{"last":67},"jobs":{"last":7}}}`))
	}))
	defer server.Close()

	collector := NewHTTPClientCollector(
		"http_client_demo",
		server.URL,
		time.Second,
		HTTPClientCollectorOptions{},
	)

	points, err := collector.Scrape(context.Background())
	if err != nil {
		t.Fatalf("Scrape() error: %v", err)
	}
	if len(points) != 1 {
		t.Fatalf("unexpected point count: got=%d want=1", len(points))
	}
	if points[0].Key != "total" {
		t.Fatalf("unexpected key: %q", points[0].Key)
	}
	if got := points[0].Values["util"].Raw; got != 67.0 {
		t.Fatalf("unexpected util value: %v", got)
	}
	if got := points[0].Values["jobs"].Raw; got != 7.0 {
		t.Fatalf("unexpected jobs value: %v", got)
	}
}

func TestHTTPClientCollectorScrapePrometheus(t *testing.T) {
	payload := strings.Join([]string{
		`# TYPE vector_build_info gauge`,
		`vector_build_info{host="dev",version="0.53.0"} 1`,
		`# TYPE vector_component_received_bytes_total counter`,
		`vector_component_received_bytes_total{host="dev",component_id="in"} 42`,
	}, "\n")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(payload))
	}))
	defer server.Close()

	collector := NewHTTPClientCollector(
		"vector_monitor",
		server.URL,
		time.Second,
		HTTPClientCollectorOptions{
			Format:  "prometheus",
			VarMode: PrometheusVarModeFull,
		},
	)

	points, err := collector.Scrape(context.Background())
	if err != nil {
		t.Fatalf("Scrape() error: %v", err)
	}
	if len(points) != 1 {
		t.Fatalf("unexpected point count: got=%d want=1", len(points))
	}
	assertPointValue(t, points, "total", "vector_build_info", 1)
	assertPointValue(t, points, "total", "vector_component_received_bytes_total", 42)
}
