package metrics

import (
	"bytes"
	"strings"
	"testing"
)

// TestParsePointsJSON_Object verifies root-object payload parsing.
// Params: testing.T for assertions.
// Returns: none.
func TestParsePointsJSON_Object(t *testing.T) {
	payload := []byte(`{"key":"total","data":{"conn":12,"util":{"last":77.7}}}`)

	points, err := ParsePointsJSON(payload)
	if err != nil {
		t.Fatalf("parse points: %v", err)
	}
	if len(points) != 1 {
		t.Fatalf("unexpected points len: %d", len(points))
	}

	point := points[0]
	if point.Key != "total" {
		t.Fatalf("unexpected key: %q", point.Key)
	}

	if got := point.Values["conn"]; got.Kind != KindNumber || got.Raw != 12 {
		t.Fatalf("unexpected conn value: %#v", got)
	}
	if got := point.Values["util"]; got.Kind != KindPercent || got.Raw != 77.7 {
		t.Fatalf("unexpected util value: %#v", got)
	}
}

// TestParsePointsJSON_Array verifies root-array payload parsing.
// Params: testing.T for assertions.
// Returns: none.
func TestParsePointsJSON_Array(t *testing.T) {
	payload := []byte(`[{"key":"a","data":{"x":1}},{"key":"b","data":{"flag":true}}]`)

	points, err := ParsePointsJSON(payload)
	if err != nil {
		t.Fatalf("parse points: %v", err)
	}
	if len(points) != 2 {
		t.Fatalf("unexpected points len: %d", len(points))
	}
	if points[1].Values["flag"].Raw != 1 {
		t.Fatalf("expected bool->1 mapping, got %#v", points[1].Values["flag"])
	}
}

// TestParsePointsJSON_ContractErrors verifies key/data validation messages.
// Params: testing.T for assertions.
// Returns: none.
func TestParsePointsJSON_ContractErrors(t *testing.T) {
	tests := []struct {
		name       string
		payload    string
		wantSubstr string
	}{
		{name: "missing key", payload: `{"data":{"x":1}}`, wantSubstr: "missing key field"},
		{name: "key type", payload: `{"key":1,"data":{"x":1}}`, wantSubstr: "key must be string"},
		{name: "missing data", payload: `{"key":"x"}`, wantSubstr: "missing data field"},
		{name: "data type", payload: `{"key":"x","data":1}`, wantSubstr: "data must be object"},
		{name: "value missing", payload: `{"key":"x","data":{"util":{"kind":"percent"}}}`, wantSubstr: "value object must contain value or last field"},
	}

	for _, tc := range tests {
		_, err := ParsePointsJSON([]byte(tc.payload))
		if err == nil {
			t.Fatalf("%s: expected parse error", tc.name)
		}
		if !strings.Contains(err.Error(), tc.wantSubstr) {
			t.Fatalf("%s: expected %q in error, got %v", tc.name, tc.wantSubstr, err)
		}
	}
}

// TestParsePointsJSONFromReader_Limit verifies payload size guard for reader path.
// Params: testing.T for assertions.
// Returns: none.
func TestParsePointsJSONFromReader_Limit(t *testing.T) {
	tooLarge := bytes.Repeat([]byte(" "), MaxPointsJSONBytes+1)
	_, err := ParsePointsJSONFromReader(bytes.NewReader(tooLarge))
	if err == nil {
		t.Fatalf("expected size limit error")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("expected size-limit text, got: %v", err)
	}
}
