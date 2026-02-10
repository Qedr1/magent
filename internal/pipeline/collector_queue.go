package pipeline

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	queueRecordHeaderSize = 4 + 8 // uint32 payload length + int64 created unix sec
	offsetSyncAckBatch    = 128
	offsetSyncInterval    = 2 * time.Second
)

var (
	errQueueEmpty = errors.New("queue is empty")
	errQueueFull  = errors.New("queue limits reached; rejecting new payload")
)

type queueRecord struct {
	payload []byte
	size    int64
	created int64
}

// DiskQueue stores prepared collector payloads in append-only format.
// Params: directory and queue limits.
// Returns: queue instance with persisted state.
type DiskQueue struct {
	mu sync.Mutex

	dataPath   string
	offsetPath string

	dataFile   *os.File
	offsetFile *os.File

	maxEvents uint64
	maxAge    time.Duration

	offset  int64
	pending uint64
	oldest  int64

	fileSize int64

	offsetDirty    bool
	ackSinceSync   uint64
	lastOffsetSync time.Time
}

// OpenDiskQueue opens/creates queue files and restores persisted offsets.
// Params: dir queue directory; maxEvents/maxAge queue limits.
// Returns: initialized queue or error.
func OpenDiskQueue(dir string, maxEvents uint64, maxAge time.Duration) (*DiskQueue, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create queue dir %q: %w", dir, err)
	}

	queue := &DiskQueue{
		dataPath:   filepath.Join(dir, "queue.bin"),
		offsetPath: filepath.Join(dir, "offset.bin"),
		maxEvents:  maxEvents,
		maxAge:     maxAge,
	}

	dataFile, err := os.OpenFile(queue.dataPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open queue data file: %w", err)
	}

	offsetFile, err := os.OpenFile(queue.offsetPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		_ = dataFile.Close()
		return nil, fmt.Errorf("open queue offset file: %w", err)
	}

	queue.dataFile = dataFile
	queue.offsetFile = offsetFile

	if err := queue.loadOffset(); err != nil {
		_ = queue.closeFiles()
		return nil, err
	}
	if err := queue.reindex(); err != nil {
		_ = queue.closeFiles()
		return nil, err
	}
	queue.lastOffsetSync = time.Now()

	return queue, nil
}

// Enqueue appends one payload to queue tail if limits allow.
// Params: payload encoded batch payload.
// Returns: nil on append, errQueueFull when limits reached, or IO error.
func (q *DiskQueue) Enqueue(payload []byte) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if err := q.rejectByLimits(time.Now().Unix()); err != nil {
		return err
	}

	if q.dataFile == nil {
		return fmt.Errorf("queue data file is not initialized")
	}

	if _, err := q.dataFile.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek queue data to end: %w", err)
	}

	nowUnix := time.Now().Unix()
	var header [queueRecordHeaderSize]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint64(header[4:12], uint64(nowUnix))

	if _, err := q.dataFile.Write(header[:]); err != nil {
		return fmt.Errorf("write queue record header: %w", err)
	}
	if _, err := q.dataFile.Write(payload); err != nil {
		return fmt.Errorf("write queue record payload: %w", err)
	}

	q.pending++
	q.fileSize += int64(queueRecordHeaderSize) + int64(len(payload))
	if q.pending == 1 {
		q.oldest = nowUnix
	}

	return nil
}

// Peek reads the first pending queue record at current offset.
// Params: none.
// Returns: record data, errQueueEmpty when no records exist.
func (q *DiskQueue) Peek() (queueRecord, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.peekLocked()
}

// Ack marks one record as consumed and advances persisted offset.
// Params: consumed record from Peek.
// Returns: nil or persistence/IO error.
func (q *DiskQueue) Ack(consumed queueRecord) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if consumed.size <= 0 {
		return fmt.Errorf("ack requires positive consumed size")
	}
	if q.pending == 0 {
		return fmt.Errorf("ack on empty queue")
	}

	q.offset += consumed.size
	if q.offset > q.fileSize {
		q.offset = q.fileSize
	}
	q.pending--
	q.offsetDirty = true
	q.ackSinceSync++

	if q.pending == 0 || q.offset >= q.fileSize {
		if err := q.resetFiles(); err != nil {
			return err
		}
		return nil
	}

	if err := q.syncOffsetMaybe(false); err != nil {
		return err
	}

	next, err := q.peekLocked()
	if err == nil {
		q.oldest = next.created
	}

	return nil
}

// Pending returns current pending queue record count.
// Params: none.
// Returns: pending record count.
func (q *DiskQueue) Pending() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.pending
}

// loadOffset restores persisted queue offset.
// Params: none.
// Returns: nil or IO error.
func (q *DiskQueue) loadOffset() error {
	if q.offsetFile == nil {
		return fmt.Errorf("queue offset file is not initialized")
	}

	var data [8]byte
	n, err := q.offsetFile.ReadAt(data[:], 0)
	if errors.Is(err, io.EOF) && n == 0 {
		q.offset = 0
		return nil
	}
	if err != nil {
		return fmt.Errorf("read queue offset: %w", err)
	}
	if n < 8 {
		return fmt.Errorf("invalid queue offset file size %d", n)
	}

	q.offset = int64(binary.LittleEndian.Uint64(data[:]))
	if q.offset < 0 {
		q.offset = 0
	}
	return nil
}

// storeOffset persists queue read offset.
// Params: none.
// Returns: nil or IO error.
func (q *DiskQueue) storeOffset() error {
	if q.offsetFile == nil {
		return fmt.Errorf("queue offset file is not initialized")
	}

	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(q.offset))
	if _, err := q.offsetFile.WriteAt(buf[:], 0); err != nil {
		return fmt.Errorf("write queue offset: %w", err)
	}
	if err := q.offsetFile.Truncate(8); err != nil {
		return fmt.Errorf("truncate queue offset: %w", err)
	}
	if err := q.offsetFile.Sync(); err != nil {
		return fmt.Errorf("write queue offset: %w", err)
	}
	q.offsetDirty = false
	q.ackSinceSync = 0
	q.lastOffsetSync = time.Now()
	return nil
}

// reindex scans queue from offset and restores pending counters.
// Params: none.
// Returns: nil or parse/IO error.
func (q *DiskQueue) reindex() error {
	if q.dataFile == nil {
		return fmt.Errorf("queue data file is not initialized")
	}

	info, err := q.dataFile.Stat()
	if err != nil {
		return fmt.Errorf("stat queue data for reindex: %w", err)
	}
	q.fileSize = info.Size()
	if q.offset > info.Size() {
		q.offset = 0
		if err := q.storeOffset(); err != nil {
			return err
		}
	}

	position := q.offset
	q.pending = 0
	q.oldest = 0

	var header [queueRecordHeaderSize]byte
	for {
		if _, err := q.dataFile.ReadAt(header[:], position); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return fmt.Errorf("read queue header at %d: %w", position, err)
		}

		payloadSize := int64(binary.LittleEndian.Uint32(header[0:4]))
		created := int64(binary.LittleEndian.Uint64(header[4:12]))
		recordSize := int64(queueRecordHeaderSize) + payloadSize
		if payloadSize < 0 || recordSize <= int64(queueRecordHeaderSize) {
			if err := q.truncateTail(position); err != nil {
				return err
			}
			break
		}
		if position+recordSize > q.fileSize {
			if err := q.truncateTail(position); err != nil {
				return err
			}
			break
		}

		if q.pending == 0 {
			q.oldest = created
		}
		q.pending++
		position += recordSize
	}

	return nil
}

// truncateTail truncates queue data file from corrupted tail start position.
// Params: position byte offset where corruption starts.
// Returns: nil or truncate/seek error.
func (q *DiskQueue) truncateTail(position int64) error {
	if position < 0 {
		position = 0
	}
	if err := q.dataFile.Truncate(position); err != nil {
		return fmt.Errorf("truncate corrupted queue tail at %d: %w", position, err)
	}
	if _, err := q.dataFile.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek queue data after truncation: %w", err)
	}
	q.fileSize = position
	return nil
}

// rejectByLimits checks queue constraints before append.
// Params: now unix timestamp.
// Returns: errQueueFull if queue rejects new payload.
func (q *DiskQueue) rejectByLimits(now int64) error {
	if q.maxEvents > 0 && q.pending >= q.maxEvents {
		return errQueueFull
	}
	if q.maxAge > 0 && q.pending > 0 && q.oldest > 0 {
		if time.Unix(now, 0).Sub(time.Unix(q.oldest, 0)) >= q.maxAge {
			return errQueueFull
		}
	}
	return nil
}

// peekLocked reads first pending queue record, caller must hold lock.
// Params: none.
// Returns: record or errQueueEmpty.
func (q *DiskQueue) peekLocked() (queueRecord, error) {
	if q.dataFile == nil {
		return queueRecord{}, fmt.Errorf("queue data file is not initialized")
	}
	if q.offset >= q.fileSize {
		return queueRecord{}, errQueueEmpty
	}

	var header [queueRecordHeaderSize]byte
	if _, err := q.dataFile.ReadAt(header[:], q.offset); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return queueRecord{}, errQueueEmpty
		}
		return queueRecord{}, fmt.Errorf("read queue header for peek: %w", err)
	}

	payloadSize := int64(binary.LittleEndian.Uint32(header[0:4]))
	created := int64(binary.LittleEndian.Uint64(header[4:12]))
	if payloadSize < 0 {
		return queueRecord{}, fmt.Errorf("invalid queue payload size %d", payloadSize)
	}
	if q.offset+int64(queueRecordHeaderSize)+payloadSize > q.fileSize {
		return queueRecord{}, fmt.Errorf("queue payload exceeds file size at offset %d", q.offset)
	}

	payload := make([]byte, payloadSize)
	if _, err := q.dataFile.ReadAt(payload, q.offset+int64(queueRecordHeaderSize)); err != nil {
		return queueRecord{}, fmt.Errorf("read queue payload for peek: %w", err)
	}

	return queueRecord{
		payload: payload,
		size:    int64(queueRecordHeaderSize) + payloadSize,
		created: created,
	}, nil
}

// resetFiles clears queue data and offset after full drain.
// Params: none.
// Returns: nil or IO error.
func (q *DiskQueue) resetFiles() error {
	if q.dataFile == nil {
		return fmt.Errorf("queue data file is not initialized")
	}

	if err := q.dataFile.Truncate(0); err != nil {
		return fmt.Errorf("truncate queue data: %w", err)
	}
	if _, err := q.dataFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek queue data to start: %w", err)
	}

	q.fileSize = 0
	q.offset = 0
	q.pending = 0
	q.oldest = 0
	q.offsetDirty = true
	return q.storeOffset()
}

// syncOffsetMaybe persists offset based on time/ack thresholds.
// Params: force triggers immediate flush.
// Returns: nil or IO error.
func (q *DiskQueue) syncOffsetMaybe(force bool) error {
	if !q.offsetDirty {
		return nil
	}
	if !force {
		if q.ackSinceSync < offsetSyncAckBatch && time.Since(q.lastOffsetSync) < offsetSyncInterval {
			return nil
		}
	}
	return q.storeOffset()
}

// Close flushes pending offset and closes queue files.
// Params: none.
// Returns: nil or close/flush error.
func (q *DiskQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.dataFile == nil && q.offsetFile == nil {
		return nil
	}

	if err := q.syncOffsetMaybe(true); err != nil {
		_ = q.closeFiles()
		return err
	}

	return q.closeFiles()
}

// closeFiles closes open queue descriptors.
// Params: none.
// Returns: first close error.
func (q *DiskQueue) closeFiles() error {
	var firstErr error
	if q.dataFile != nil {
		if err := q.dataFile.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close queue data file: %w", err)
		}
		q.dataFile = nil
	}
	if q.offsetFile != nil {
		if err := q.offsetFile.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close queue offset file: %w", err)
		}
		q.offsetFile = nil
	}
	return firstErr
}
