package heapfile

import (
	"DaemonDB/storage_engine/bufferpool"
	diskmanager "DaemonDB/storage_engine/disk_manager"
	"sync"
)

// PageHeader is the header for a single 4KB page
type HeapPageHeader struct {
	FileID         uint32 // fileID which heap file this page belongs to
	PageNo         uint32 // current page number inside the heap file
	FreePtr        uint16 // ptr to the next free location, where insertion can be done
	NumRows        uint16 // number of rows/slots it can accomodate
	NumRowsFree    uint16 // free rows/slots inside the current page
	IsPageFull     uint16 // is the page full
	SlotCount      uint16 // number of slots in the slot directory
	LastAppliedLSN uint64 // Last LSN applied to this page in wal
}

// Slot represents an entry in the slot directory at the bottom of the page
// Stored at the end of the page, grows backward
type Slot struct {
	Offset uint16 // Offset from start of page to row data
	Length uint16 // Length of the row data
}

// HeapFile represents a single heap file on disk
type HeapFile struct {
	fileID      uint32 // which file it is
	tableName   string // table this heap file belongs to
	diskManager *diskmanager.DiskManager
	bufferPool  *bufferpool.BufferPool
	filePath    string
	mu          sync.RWMutex
}

// HeapFileManager manages all heap files
type HeapFileManager struct {
	baseDir     string
	files       map[uint32]*HeapFile
	tableIndex  map[string]uint32 // tableName → catalog basd fileID (name-based lookup)
	bufferPool  *bufferpool.BufferPool
	diskManager *diskmanager.DiskManager
	mu          sync.RWMutex
}
