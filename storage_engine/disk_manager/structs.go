package diskmanager

import (
	"os"
	"sync"
)

// ############################################# FILE DESCRIPTOR ###########################################

type PageKey struct {
	FileID   uint32
	LocalNum int64
}

// FileDescriptor represents an open file managed by the disk manager
type FileDescriptor struct {
	FileID     uint32
	FilePath   string
	File       *os.File
	NextPageID int64 // Next available page ID within this file
	mu         sync.RWMutex
}

// ############################################# DISK MANAGER #############################################

// DiskManager manages all disk I/O operations and file handles
type DiskManager struct {
	files      map[uint32]*FileDescriptor // fileID -> file descriptor
	nextFileID uint32                     // nextFileID is now only used by OpenFile, which is only called for WAL and index files.
	// Heap files always use OpenFileWithID with the catalog's fileID.
	globalPageMap map[int64]uint32  // globalPageID -> fileID mapping
	localToGlobal map[PageKey]int64 // // (fileID, localNum) → globalPageID
	mu            sync.RWMutex
}
