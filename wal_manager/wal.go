package wal_manager

import (
	"DaemonDB/types"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

/*

WAL Segment File
────────────────────────────────────
| Record | Record | Record | ...   |
────────────────────────────────────

Each Record:
────────────────────────────────────────────
| LSN (8) | LEN (4) | CRC (4) | DATA (LEN) |
────────────────────────────────────────────

	RecordHeaderSize = 16
	SegmentSize      = 16 * 1024 * 1024

*/

func OpenWAL(directory string) (*WALManager, error) {
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}

	wal := &WALManager{
		Directory: directory,
		Segments:  make(map[uint64]*WALSegment),
	}

	// recover existing onces
	if err := wal.recoverWALEntries(); err != nil {
		return nil, err
	}

	if wal.CurrSegment == nil {
		if err := wal.createNewSegment(); err != nil {
			return nil, err
		}
	}

	return wal, nil
}

// recover exisitng wal entries
// updates the current lsn and current segment number
// set the segmentId to segment mapping
func (w *WALManager) recoverWALEntries() error {
	files, err := filepath.Glob(filepath.Join(w.Directory, "wal_*.log"))
	if err != nil {
		return err
	}

	// exctrating the segments id from the wal files
	var segmentIDs []uint64
	for _, file := range files {
		name := filepath.Base(file)
		if !strings.HasPrefix(name, "wal_") || !strings.HasSuffix(name, ".log") {
			continue
		}

		// extract hex part
		hexPart := strings.TrimSuffix(
			strings.TrimPrefix(name, "wal_"),
			".log",
		)
		segmentID, err := strconv.ParseUint(hexPart, 16, 64)
		if err != nil {
			continue
		}

		segmentIDs = append(segmentIDs, segmentID)
	}

	if len(segmentIDs) == 0 {
		return nil
	}

	slices.Sort(segmentIDs)

	maxLSN := uint64(0)
	for _, segmentID := range segmentIDs {
		segment := InitializeWALSegment(segmentID, w.Directory)
		if err := segment.Open(); err != nil {
			return err
		}
		w.Segments[segmentID] = segment

		// Scan segment for largest LSN
		lsn, err := w.findLargestLSN(segment)
		if err != nil {
			return err
		}
		if lsn > maxLSN {
			maxLSN = lsn
		}
	}

	// Set current segment to the last one
	lastSegmentID := segmentIDs[len(segmentIDs)-1]
	w.CurrSegment = w.Segments[lastSegmentID]
	w.CurrentLSN = maxLSN

	fmt.Printf("Recovered Successful: %+v\n", w)

	return nil
}

func (w *WALManager) createNewSegment() error {
	segmentID := uint64(len(w.Segments))
	segment := InitializeWALSegment(segmentID, w.Directory)

	if err := segment.Open(); err != nil {
		return err
	}

	w.Segments[segmentID] = segment
	w.CurrSegment = segment
	return nil
}

func (wm *WALManager) ReplayFromLSN(startLSN uint64, applyFunc func(*types.Operation) error) error {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	// Get sorted segment IDs
	var segmentIDs []uint64
	for id := range wm.Segments {
		segmentIDs = append(segmentIDs, id)
	}
	slices.Sort(segmentIDs)

	// Replay each segment
	for _, segmentID := range segmentIDs {
		segment := wm.Segments[segmentID]
		if err := wm.replaySegment(segment, startLSN, applyFunc); err != nil {
			return fmt.Errorf("failed to replay segment %d: %w", segmentID, err)
		}
	}

	return nil
}

func (wm *WALManager) replaySegment(segment *WALSegment, startLSN uint64, applyFunc func(*types.Operation) error) error {
	segment.mu.Lock()
	defer segment.mu.Unlock()

	file, err := os.Open(segment.FilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Read header
	header := make([]byte, 16)

	for {
		_, err := io.ReadFull(file, header)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		lsn := binary.BigEndian.Uint64(header[0:8])
		dataLen := binary.BigEndian.Uint32(header[8:12])
		crc := binary.BigEndian.Uint32(header[12:16])

		data := make([]byte, dataLen)
		_, err = io.ReadFull(file, data)
		if err != nil {
			return err
		}

		// Validate CRC
		if calculateCRC(lsn, data) != crc {
			return fmt.Errorf("CRC mismatch at LSN %d", lsn)
		}

		// Skip if before startLSN
		if lsn < startLSN {
			continue
		}

		// Decode and apply operation
		var op types.Operation
		if err := json.Unmarshal(data, &op); err != nil {
			return fmt.Errorf("failed to decode operation at LSN %d: %w", lsn, err)
		}

		if err := applyFunc(&op); err != nil {
			return fmt.Errorf("failed to apply operation at LSN %d: %w", lsn, err)
		}
	}

	return nil
}

func (wm *WALManager) Close() error {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	// Close all other segments
	for _, seg := range wm.Segments {
		if seg.File != nil {
			if err := wm.flushAndCloseSegment(seg); err != nil {
				return err
			}
		}
	}

	return nil
}

func (wm *WALManager) flushAndCloseSegment(seg *WALSegment) error {
	if err := seg.File.Sync(); err != nil {
		return err
	}

	if err := seg.File.Close(); err != nil {
		return err
	}

	seg.File = nil
	return nil
}

func (wm *WALManager) AppendOperation(op *types.Operation) (uint64, error) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	data := op.Encode()

	wm.CurrentLSN++
	lsn := wm.CurrentLSN

	record := &WALRecord{
		LSN:  lsn,
		Data: data,
		CRC:  calculateCRC(lsn, data),
	}

	encodedRecord := record.Encode()

	// Check if we need a new segment
	if wm.CurrSegment.IsFull() {
		if err := wm.createNewSegment(); err != nil {
			return 0, err
		}
	}

	// Append to current segment (atomic operation due to O_APPEND)
	_, err := wm.CurrSegment.Append(encodedRecord)
	if err != nil {
		return 0, err
	}

	return lsn, nil
}

func (wm *WALManager) Sync() error {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	return wm.CurrSegment.Sync()
}

func (w *WALManager) findLargestLSN(segment *WALSegment) (uint64, error) {
	segment.mu.Lock()
	defer segment.mu.Unlock()

	if segment.File == nil {
		return 0, fmt.Errorf("segment not opened")
	}

	file, err := os.Open(segment.FilePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	maxLSN := uint64(0)
	buf := make([]byte, RecordHeaderSize)
	crcBuf := make([]byte, 4)

	for {
		n, err := file.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
		if n < RecordHeaderSize {
			break
		}

		lsn := binary.BigEndian.Uint64(buf[0:8])
		dataLen := binary.BigEndian.Uint32(buf[8:12])

		if lsn > maxLSN {
			maxLSN = lsn
		}

		// Skip data portion and crc bits
		_, err = file.Seek(int64(dataLen), 1)
		if err != nil {
			break
		}

		_, err = file.Read(crcBuf)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return 0, err
		}
	}

	return maxLSN, nil
}
