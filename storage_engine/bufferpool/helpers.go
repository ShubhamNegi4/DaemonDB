package bufferpool

import (
	"DaemonDB/storage_engine/page"
	"fmt"
)

/*
Helper / utility methods for the buffer pool.
None of these touch eviction internals directly.
*/

// GetStats returns a point-in-time snapshot of buffer pool statistics.
func (bp *BufferPool) GetStats() BufferPoolStats {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	stats := BufferPoolStats{
		TotalPages: len(bp.pages),
		Capacity:   bp.capacity,
		Hits:       bp.hits,
		Misses:     bp.misses,
	}
	if bp.hits+bp.misses > 0 {
		stats.HitRate = float64(bp.hits) / float64(bp.hits+bp.misses) * 100
	}

	for _, pg := range bp.pages {
		pg.RLock()
		if pg.PinCount > 0 {
			stats.PinnedPages++
		}
		if pg.IsDirty {
			stats.DirtyPages++
		}
		pg.RUnlock()
	}
	return stats
}

func (bp *BufferPool) ResetStats() {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.hits = 0
	bp.misses = 0
}

// Reset flushes all dirty pages and clears the pool.
// The eviction policy is re-created fresh (same type, same capacity).
func (bp *BufferPool) Reset() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	for _, pg := range bp.pages {
		pg.Lock()
		if pg.IsDirty && bp.diskManager != nil {
			if err := bp.diskManager.WritePage(pg); err != nil {
				pg.Unlock()
				return fmt.Errorf("failed to flush page during reset: %w", err)
			}
		}
		pg.Unlock()
	}

	bp.pages = make(map[int64]*page.Page, bp.capacity)

	// Re-create the policy so its internal state is clean.
	// We cannot reach into the interface to zero fields, so we rebuild it.
	bp.policy = newPolicyLike(bp.policy, bp.capacity)

	return nil
}

// newPolicyLike returns a fresh policy of the same concrete type and capacity.
// This is the only place that needs a type-switch on EvictionPolicy.
func newPolicyLike(old EvictionPolicy, capacity int) EvictionPolicy {
	switch v := old.(type) {
	case *LRUKPolicy:
		return NewLRUKPolicy(v.k)
	case *TinyLFUPolicy:
		return NewTinyLFUPolicy(capacity)
	default:
		return NewLRUKPolicy(2)
	}
}

// Size returns the number of pages currently in the pool.
func (bp *BufferPool) Size() int {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return len(bp.pages)
}

// Capacity returns the maximum number of pages the pool can hold.
func (bp *BufferPool) Capacity() int {
	return bp.capacity
}

// PolicyName returns the name of the active eviction policy.
// Useful for logging and benchmark output.
func (bp *BufferPool) PolicyName() string {
	// No lock needed — policy is set once at construction and never mutated.
	return bp.policy.Name()
}

// GetPage returns a page from the pool without loading from disk.
// Returns nil if the page is not currently buffered.
func (bp *BufferPool) GetPage(pageID int64) *page.Page {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return bp.pages[pageID]
}

// MarkDirty marks a buffered page as modified.
func (bp *BufferPool) MarkDirty(pageID int64) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	pg, exists := bp.pages[pageID]
	if !exists {
		return fmt.Errorf("page %d not in buffer pool", pageID)
	}

	pg.Lock()
	pg.IsDirty = true
	pg.Unlock()

	return nil
}
