package logging

import (
	"bytes"
	"strings"
	"testing"
)

// TestColorLineWriter_HighlightsLevelAndTokens verifies level and token coloring.
// Params: testing.T for assertions.
// Returns: none.
func TestColorLineWriter_HighlightsLevelAndTokens(t *testing.T) {
	var dst bytes.Buffer
	writer := &colorLineWriter{dst: &dst}

	line := `level=INFO msg="hello" peer=10.20.30.40 retries=3`
	if _, err := writer.Write([]byte(line)); err != nil {
		t.Fatalf("write: %v", err)
	}

	rendered := dst.String()
	if !strings.HasPrefix(rendered, ansiBlue) {
		t.Fatalf("expected INFO line base color")
	}
	if !strings.Contains(rendered, ansiGreen+`"hello"`+ansiReset+ansiBlue) {
		t.Fatalf("expected quoted string token color")
	}
	if !strings.Contains(rendered, ansiCyan+`10.20.30.40`+ansiReset+ansiBlue) {
		t.Fatalf("expected IP token color")
	}
	if !strings.Contains(rendered, ansiYellow+`3`+ansiReset+ansiBlue) {
		t.Fatalf("expected number token color")
	}
	if !strings.HasSuffix(rendered, ansiReset) {
		t.Fatalf("expected trailing reset sequence")
	}
}

// TestColorLineWriter_NoLevelColor verifies passthrough for unknown levels.
// Params: testing.T for assertions.
// Returns: none.
func TestColorLineWriter_NoLevelColor(t *testing.T) {
	var dst bytes.Buffer
	writer := &colorLineWriter{dst: &dst}

	line := `msg="plain" value=42`
	if _, err := writer.Write([]byte(line)); err != nil {
		t.Fatalf("write: %v", err)
	}

	if got := dst.String(); got != line {
		t.Fatalf("expected passthrough line, got %q", got)
	}
}
