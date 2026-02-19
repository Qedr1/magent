package pipeline

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestDiskQueue_EnqueuePeekAck verifies append/peek/ack lifecycle.
// Params: testing.T for assertions.
// Returns: none.
func TestDiskQueue_EnqueuePeekAck(t *testing.T) {
	queue, err := OpenDiskQueue(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatalf("open queue: %v", err)
	}
	t.Cleanup(func() {
		_ = queue.Close()
	})

	if err := queue.Enqueue([]byte("hello")); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if got := queue.Pending(); got != 1 {
		t.Fatalf("unexpected pending count: %d", got)
	}

	record, err := queue.Peek()
	if err != nil {
		t.Fatalf("peek: %v", err)
	}
	if string(record.payload) != "hello" {
		t.Fatalf("unexpected payload: %q", string(record.payload))
	}

	if err := queue.Ack(record); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if got := queue.Pending(); got != 0 {
		t.Fatalf("expected empty queue, got %d", got)
	}
	if _, err := queue.Peek(); !errors.Is(err, errQueueEmpty) {
		t.Fatalf("expected errQueueEmpty, got %v", err)
	}
}

// TestDiskQueue_MaxEventsLimit verifies rejection when max_events is reached.
// Params: testing.T for assertions.
// Returns: none.
func TestDiskQueue_MaxEventsLimit(t *testing.T) {
	queue, err := OpenDiskQueue(t.TempDir(), 1, 0)
	if err != nil {
		t.Fatalf("open queue: %v", err)
	}
	t.Cleanup(func() {
		_ = queue.Close()
	})

	if err := queue.Enqueue([]byte("first")); err != nil {
		t.Fatalf("enqueue first: %v", err)
	}
	if err := queue.Enqueue([]byte("second")); !errors.Is(err, errQueueFull) {
		t.Fatalf("expected errQueueFull, got %v", err)
	}
}

// TestDiskQueue_MaxAgeLimit verifies rejection when oldest pending record exceeds max_age.
// Params: testing.T for assertions.
// Returns: none.
func TestDiskQueue_MaxAgeLimit(t *testing.T) {
	queue, err := OpenDiskQueue(t.TempDir(), 0, time.Second)
	if err != nil {
		t.Fatalf("open queue: %v", err)
	}
	t.Cleanup(func() {
		_ = queue.Close()
	})

	if err := queue.Enqueue([]byte("first")); err != nil {
		t.Fatalf("enqueue first: %v", err)
	}

	time.Sleep(1100 * time.Millisecond)

	if err := queue.Enqueue([]byte("second")); !errors.Is(err, errQueueFull) {
		t.Fatalf("expected errQueueFull by age, got %v", err)
	}
}

// TestDiskQueue_RecoverFromTruncatedTail verifies startup recovery from partial last record.
// Params: testing.T for assertions.
// Returns: none.
func TestDiskQueue_RecoverFromTruncatedTail(t *testing.T) {
	dir := t.TempDir()
	queue, err := OpenDiskQueue(dir, 0, 0)
	if err != nil {
		t.Fatalf("open queue: %v", err)
	}

	if err := queue.Enqueue([]byte("hello")); err != nil {
		t.Fatalf("enqueue valid payload: %v", err)
	}
	if err := queue.Close(); err != nil {
		t.Fatalf("close queue: %v", err)
	}

	file, err := os.OpenFile(filepath.Join(dir, "queue.bin"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open queue data for tail corruption: %v", err)
	}
	var header [queueRecordHeaderSize]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(10))
	binary.LittleEndian.PutUint64(header[4:12], uint64(time.Now().Unix()))
	if _, err := file.Write(header[:]); err != nil {
		t.Fatalf("write tail header: %v", err)
	}
	if _, err := file.Write([]byte{1, 2}); err != nil {
		t.Fatalf("write partial tail payload: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close queue data after corruption: %v", err)
	}

	recovered, err := OpenDiskQueue(dir, 0, 0)
	if err != nil {
		t.Fatalf("open recovered queue: %v", err)
	}
	t.Cleanup(func() {
		_ = recovered.Close()
	})

	if got := recovered.Pending(); got != 1 {
		t.Fatalf("unexpected pending count after recovery: %d", got)
	}

	record, err := recovered.Peek()
	if err != nil {
		t.Fatalf("peek recovered payload: %v", err)
	}
	if string(record.payload) != "hello" {
		t.Fatalf("unexpected recovered payload: %q", string(record.payload))
	}
}
