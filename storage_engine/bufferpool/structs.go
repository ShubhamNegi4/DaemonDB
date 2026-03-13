package bufferpool

import (
	diskmanager "DaemonDB/storage_engine/disk_manager"
	"DaemonDB/storage_engine/page"
	"container/heap"
	"sync"
)

// ############################################# BUFFER POOL #############################################

// BufferPool manages cached pages in memory with GDSF eviction.
// Works with both heap file pages and B+ tree index pages
type BufferPool struct {
	pages       map[int64]*page.Page // pageID -> Page
	capacity    int
	diskManager *diskmanager.DiskManager
	walManager  WALFlushedLSNGetter

	// GDSF fields
	policy gdsfPolicy

	// Debug controls (avoid excessive syscall overhead in benchmarks).
	debugEvict bool

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

type gdsfPolicy struct {
	// H is the inflation value (set to the last evicted key).
	H float64

	// Per-page metadata used to compute score.
	meta map[int64]*gdsfMeta

	// Min-heap of eviction candidates (lowest key evicted first).
	pq gdsfPQ
}

type gdsfMeta struct {
	Frequency   float64 // access count
	LatencyNS   float64 // observed miss latency in nanoseconds (0 if never missed)
	RespSizeB   float64 // response size in bytes (page size)
	item *gdsfItem // single heap item per page (updated in-place)
}

// gdsfPQ is a min-heap ordered by key (smallest key evicted first).
type gdsfPQ []*gdsfItem

type gdsfItem struct {
	pageID  int64
	key     float64
	index   int
}

var _ heap.Interface = (*gdsfPQ)(nil)
