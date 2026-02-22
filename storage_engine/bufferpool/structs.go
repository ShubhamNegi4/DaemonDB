package bufferpool

import (
	diskmanager "DaemonDB/storage_engine/disk_manager"
	"DaemonDB/storage_engine/page"
	"sync"
)

// ############################################# BUFFER POOL #############################################

// BufferPool manages cached pages in memory with LRU eviction
// Works with both heap file pages and B+ tree index pages
type BufferPool struct {
	pages       map[int64]*page.Page // pageID -> Page
	capacity    int
	diskManager *diskmanager.DiskManager
	walManager  WALFlushedLSNGetter
	accessOrder []int64 // LRU tracking: most recently used at end
	mu          sync.Mutex
}

// Stats returns buffer pool statistics
type BufferPoolStats struct {
	TotalPages  int
	PinnedPages int
	DirtyPages  int
	Capacity    int
	HitRate     float64 // Could be tracked with counters
}

// small interface so bufferpool doesn't import the whole wal package
type WALFlushedLSNGetter interface {
	GetFlushedLSN() uint64
}
