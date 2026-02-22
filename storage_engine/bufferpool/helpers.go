package bufferpool

import (
	"DaemonDB/storage_engine/page"
	"fmt"
)

/*
This file holds helper functions for the bufferpool
*/

// GetStats returns current buffer pool statistics
func (bp *BufferPool) GetStats() BufferPoolStats {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	stats := BufferPoolStats{
		TotalPages: len(bp.pages),
		Capacity:   bp.capacity,
	}

	for _, page := range bp.pages {
		page.RLock()
		if page.PinCount > 0 {
			stats.PinnedPages++
		}
		if page.IsDirty {
			stats.DirtyPages++
		}
		page.RUnlock()
	}

	return stats
}

// Reset clears all pages from the buffer pool (for testing or reset)
func (bp *BufferPool) Reset() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	// Flush all dirty pages first
	for _, page := range bp.pages {
		page.Lock()
		if page.IsDirty && bp.diskManager != nil {
			if err := bp.diskManager.WritePage(page); err != nil {
				page.Unlock()
				return fmt.Errorf("failed to flush page during reset: %w", err)
			}
		}
		page.Unlock()
	}

	// Clear the pool
	bp.pages = make(map[int64]*page.Page, bp.capacity)
	bp.accessOrder = make([]int64, 0, bp.capacity)

	return nil
}

// Size returns the current number of pages in the buffer pool
func (bp *BufferPool) Size() int {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return len(bp.pages)
}

// Capacity returns the maximum capacity of the buffer pool
func (bp *BufferPool) Capacity() int {
	return bp.capacity
}

// GetPage returns a page from the buffer pool without loading from disk
// Returns nil if page is not in buffer pool
func (bp *BufferPool) GetPage(pageID int64) *page.Page {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return bp.pages[pageID]
}

// MarkDirty marks a page as dirty (modified)
func (bp *BufferPool) MarkDirty(pageID int64) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	page, exists := bp.pages[pageID]
	if !exists {
		return fmt.Errorf("page %d not in buffer pool", pageID)
	}

	page.Lock()
	page.IsDirty = true
	page.Unlock()

	return nil
}
