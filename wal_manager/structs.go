package wal_manager

import (
	"os"
	"sync"
)

const (
	RecordHeaderSize = 16
	SegmentSize      = 16 * 1024 * 1024
)

type WALManager struct {
	Directory   string
	CurrSegment *WALSegment
	CurrentLSN  uint64
	Segments    map[uint64]*WALSegment
	mu          sync.RWMutex
}

type WALSegment struct {
	SegmentId uint64
	FilePath  string
	File      *os.File
	Size      int64
	mu        sync.Mutex
}

type WALRecord struct {
	LSN  uint64
	Data []byte
	CRC  uint32
}
