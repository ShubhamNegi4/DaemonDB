package bufferpool

import (
	diskmanager "DaemonDB/storage_engine/disk_manager"
	"DaemonDB/storage_engine/page"
	"sync"
)

// ############################################# BUFFER POOL #############################################

// BufferPool manages cached pages in memory.
// The eviction strategy is fully delegated to the EvictionPolicy interface —
// swap the policy at construction time to change eviction behaviour.
type BufferPool struct {
	pages       map[int64]*page.Page // pageID -> Page
	capacity    int
	diskManager *diskmanager.DiskManager
	walManager  WALFlushedLSNGetter
	policy      EvictionPolicy // lruk / tinylfu — set by NewBufferPool
	debugEvict  bool
	mu          sync.Mutex

	hits   int64
	misses int64
}

// BufferPoolStats holds a point-in-time snapshot of pool health.
type BufferPoolStats struct {
	TotalPages  int
	PinnedPages int
	DirtyPages  int
	Capacity    int
	Hits        int64
	Misses      int64
	HitRate     float64 // Hits / (Hits + Misses) * 100
}

// WALFlushedLSNGetter is a narrow interface so the buffer pool does not
// need to import the entire WAL package.
type WALFlushedLSNGetter interface {
	GetFlushedLSN() uint64
}

// ############################################# LRU-K METADATA #############################################
// lruKMeta and helpers live here because they are shared between:
//   - LRUKPolicy (policy.go) — uses them directly
//   - lruK constant          — referenced by isCold / backwardKDist
//
// Nothing else in the package touches these types.

// lruK is the history depth for LRU-K.
// K=2 matches PostgreSQL: a page must be accessed at least twice before it
// is considered "hot" and competes with other hot pages on recency.
const lruK = 2

// lruKMeta tracks the K most-recent access timestamps for one page.
//
// Cold page  (len(hist) < K):  backward-K-distance = +∞ → always evicted first.
// Hot  page  (len(hist) == K): backward-K-distance = now - hist[0].
type lruKMeta struct {
	hist []uint64 // K most-recent logical timestamps, oldest first
}

// isCold returns true when the page has fewer than K recorded accesses.
func (m *lruKMeta) isCold() bool {
	return len(m.hist) < lruK
}

// backwardKDist returns now - hist[0], the time since the K-th most recent
// access.  Callers must check isCold() first; returns 0 for cold pages.
func (m *lruKMeta) backwardKDist(now uint64) uint64 {
	if len(m.hist) < lruK {
		return 0
	}
	return now - m.hist[0]
}

// recordAccess appends now to the sliding window, keeping only the K most
// recent entries.
func (m *lruKMeta) recordAccess(now uint64) {
	m.hist = append(m.hist, now)
	if len(m.hist) > lruK {
		m.hist = m.hist[len(m.hist)-lruK:]
	}
}
