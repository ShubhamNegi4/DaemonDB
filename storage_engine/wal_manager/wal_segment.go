package wal_manager

import (
	"fmt"
	"os"
	"path/filepath"
)

/*
This file contains the actual internal operation wal segment

The Two of the important functions

WALSegment.Append — lowest level. Just writes raw bytes to the file and tracks size.
Returns bytes written. No fsync — data is in OS buffer, not guaranteed durable.

WALSegment.Sync — calls File.Sync() which forces OS buffer → disk.
After this, data is durable even if process crashes.

*/

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

// WALSegment.Append — lowest level. Just writes raw bytes to the file and tracks size.
// Returns bytes written. No fsync — data is in OS buffer, not guaranteed durable.
func (ws *WALSegment) Append(data []byte) (int, error) {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	if ws.File == nil {
		return 0, fmt.Errorf("segment not opened")
	}

	n, err := ws.File.Write(data)
	if err != nil {
		return 0, err
	}

	ws.Size += int64(n)
	return n, nil // return bytes written, not offset
}

// WALSegment.Sync — calls File.Sync() which forces OS buffer → disk.
// After this, data is durable even if process crashes
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
