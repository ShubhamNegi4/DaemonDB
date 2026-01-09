package wal_manager

import (
	"fmt"
	"os"
	"path/filepath"
)

func InitializeWALSegment(segmentId uint64, basePath string) *WALSegment {
	fileName := fmt.Sprintf("wal_%016x.log", segmentId)
	filePath := filepath.Join(basePath, fileName)

	return &WALSegment{
		SegmentId: segmentId,
		FilePath:  filePath,
	}
}

// opens the segment file in append-only mode
func (ws *WALSegment) Open() error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	if ws.File != nil {
		return nil
	}

	// O_APPEND ensures atomic appends at the OS level
	file, err := os.OpenFile(ws.FilePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return err
	}

	ws.File = file
	ws.Size = stat.Size()
	return nil
}

// Append writes data to the segment (append-only operation)
func (ws *WALSegment) Append(data []byte) (int64, error) {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	if ws.File == nil {
		return 0, fmt.Errorf("segment not opened")
	}

	// Record offset before write
	offset := ws.Size

	// Write is atomic because file was opened with O_APPEND
	n, err := ws.File.Write(data)
	if err != nil {
		return 0, err
	}

	ws.Size += int64(n)
	return offset, nil
}

// Sync ensures data is persisted to disk
func (ws *WALSegment) Sync() error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	if ws.File == nil {
		return fmt.Errorf("segment not opened")
	}

	return ws.File.Sync()
}

// Close closes the segment file
func (ws *WALSegment) Close() error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	if ws.File != nil {
		err := ws.File.Close()
		ws.File = nil
		return err
	}
	return nil
}

// IsFull checks if segment has reached size limit
func (ws *WALSegment) IsFull() bool {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	return ws.Size >= SegmentSize
}
