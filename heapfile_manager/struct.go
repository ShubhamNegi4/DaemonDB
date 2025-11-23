package heapfile

import (
	bplus "DaemonDB/bplustree"
	"sync"
)

// ############################################# ---- PAGE ----- #############################################
const (
	PageSize       = 4096 // 4KB page
	PageHeaderSize = 32   // 32 bytes
	SlotSize       = 4    // 4 bytes per slot entry (offset: 2B, length: 2B)
)

// PageHeader is the header for a single 4KB page
type PageHeader struct {
	FileID      uint32 // fileID which heap file this page belongs to
	PageNo      uint32 // current page number inside the heap file
	FreePtr     uint16 // ptr to the next free location, where insertion can be done
	NumRows     uint16 // number of rows/slots it can accomodate
	NumRowsFree uint16 // free rows/slots inside the current page
	IsPageFull  uint16 // is the page full
	SlotCount   uint16 // number of slots in the slot directory
}

// Slot represents an entry in the slot directory at the bottom of the page
// Stored at the end of the page, grows backward
type Slot struct {
	Offset uint16 // Offset from start of page to row data
	Length uint16 // Length of the row data
}

// RowPointer points to a specific row in a heap file
type RowPointer struct {
	FileID     uint32 `json:"file_id"`
	PageNumber uint32 `json:"page_number"`
	SlotIndex  uint16 `json:"slot_index"` // Index in the slot directory
}

// HeapFile represents a single heap file on disk
type HeapFile struct {
	fileID   uint32      // which file it is
	pager    bplus.Pager // pager for disk I/O
	filePath string
	mu       sync.RWMutex
}

// HeapFileManager manages all heap files
type HeapFileManager struct {
	baseDir string
	files   map[uint32]*HeapFile
	mu      sync.RWMutex
}
