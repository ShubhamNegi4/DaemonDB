package bufferpool

import (
	diskmanager "DaemonDB/storage_engine/disk_manager"
	"DaemonDB/storage_engine/page"
	"DaemonDB/types"
	"encoding/binary"
	"fmt"
)

/*
This file is the main file of the bufferpool
The buffer pool works on LRU based caching mechanism
and holds access to disk manager for flushing the pages in the cache onto the disk
similarly if page not found in the cache, disk manager loads the page from the disk and adds in the cache for future access

Pages are identified by globalPageID
*/

// NewBufferPool creates a new buffer pool with the given capacity
func NewBufferPool(capacity int, diskManager *diskmanager.DiskManager) *BufferPool {
	return &BufferPool{
		pages:       make(map[int64]*page.Page, capacity),
		capacity:    capacity,
		diskManager: diskManager,
		accessOrder: make([]int64, 0, capacity),
	}
}

func (bp *BufferPool) SetWALManager(wal WALFlushedLSNGetter) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.walManager = wal
}

// FetchPage retrieves a page from the buffer pool, loading from disk if necessary
// Returns the page with pin count incremented
func (bp *BufferPool) FetchPage(pageID int64) (*page.Page, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	// Check if page is in buffer pool
	if pg, exists := bp.pages[pageID]; exists {
		fmt.Printf("[BufferPool] HIT  pageID=%d pinCount=%d\n", pageID, pg.PinCount)
		// Update LRU access order
		bp.updateAccessOrder(pageID)
		// Increment pin count
		pg.Lock()
		pg.PinCount++
		pg.Unlock()
		return pg, nil
	}

	fmt.Printf("[BufferPool] MISS pageID=%d — loading from disk\n", pageID)
	// Page not in buffer pool - load from disk
	if bp.diskManager == nil {
		return nil, fmt.Errorf("disk manager not set")
	}

	pg, err := bp.diskManager.ReadPage(pageID)
	if err != nil {
		return nil, fmt.Errorf("failed to read page %d from disk: %w", pageID, err)
	}

	if pg.PageType == types.PageTypeHeapData {
		if len(pg.Data) >= 8 {
			pg.LSN = binary.LittleEndian.Uint64(pg.Data[page.PageLSNOffset:])
		}
	}

	// Add to buffer pool (may trigger eviction)
	if err := bp.addPage(pg); err != nil {
		return nil, fmt.Errorf("failed to add page to buffer pool: %w", err)
	}

	// Pin the page
	pg.Lock()
	pg.PinCount++
	pg.Unlock()

	return pg, nil
}

// NewPage creates a new page in the buffer pool for a specific file
// NewPage asks the DiskManager for the next available page ID for the given
// file, constructs a blank Page struct entirely in RAM, marks it dirty so
// the BufferPool will eventually flush it, and pins it for the caller.
func (bp *BufferPool) NewPage(fileID uint32, pageType types.PageType) (*page.Page, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if bp.diskManager == nil {
		return nil, fmt.Errorf("disk manager not set")
	}

	// Allocate a new page on disk
	pageID, err := bp.diskManager.AllocatePage(fileID, pageType)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate page: %w", err)
	}

	// Create page in memory
	pg := diskmanager.NewPage(pageID, fileID, pageType)
	pg.IsDirty = true // New pages are dirty by default

	pg.Lock()
	pg.PinCount++
	pg.Unlock()

	// Add to buffer pool
	if err := bp.addPage(pg); err != nil {
		// Pin the page
		pg.Lock()
		pg.PinCount--
		pg.Unlock()
		return nil, fmt.Errorf("failed to add new page to buffer pool: %w", err)
	}

	return pg, nil
}

// UnpinPage decrements the pin count for a page
func (bp *BufferPool) UnpinPage(pageID int64, isDirty bool) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	page, exists := bp.pages[pageID]
	if !exists {
		return fmt.Errorf("page %d not in buffer pool", pageID)
	}

	page.Lock()
	defer page.Unlock()

	if page.PinCount > 0 {
		page.PinCount--
	}

	if isDirty {
		page.IsDirty = true
	}

	return nil
}

// FlushPage writes a specific page to disk if dirty
func (bp *BufferPool) FlushPage(pageID int64) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	page, exists := bp.pages[pageID]
	if !exists {
		return fmt.Errorf("page %d not in buffer pool", pageID)
	}

	page.Lock()
	defer page.Unlock()

	if !page.IsDirty {
		return nil // Nothing to flush
	}

	if bp.walManager != nil {
		pageLSN := page.LSN // generic, works for both heap and index
		flushedLSN := bp.walManager.GetFlushedLSN()
		if pageLSN > flushedLSN {
			fmt.Printf("[BufferPool] FLUSH BLOCKED pageID=%d pageLSN=%d flushedLSN=%d\n", pageID, pageLSN, flushedLSN)
			return fmt.Errorf("cannot flush page %d: pageLSN=%d not yet covered by WAL flushedLSN=%d", pageID, pageLSN, flushedLSN)
		}
		fmt.Printf("[BufferPool] FLUSH pageID=%d pageLSN=%d flushedLSN=%d\n", pageID, pageLSN, flushedLSN)
	}

	if err := bp.diskManager.WritePage(page); err != nil {
		return fmt.Errorf("failed to flush page %d: %w", pageID, err)
	}

	page.IsDirty = false
	return nil
}

// FlushAllPages writes all dirty pages to disk
func (bp *BufferPool) FlushAllPages() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if bp.diskManager == nil {
		return fmt.Errorf("disk manager not set")
	}

	fmt.Printf("[BufferPool] FlushAllPages — pool size=%d\n", len(bp.pages))

	for pageID, page := range bp.pages {
		page.Lock()
		if page.IsDirty {
			if bp.walManager != nil {
				if page.LSN > bp.walManager.GetFlushedLSN() {
					page.Unlock()
					continue // skip — not yet covered by WAL
				}
			}
			if err := bp.diskManager.WritePage(page); err != nil {
				page.Unlock()
				return fmt.Errorf("failed to flush page %d: %w", pageID, err)
			}
			fmt.Printf("[BufferPool]   flushing pageID=%d\n", pageID)
			page.IsDirty = false
		}
		page.Unlock()
	}

	return nil
}

// addPage adds a page to the buffer pool, evicting if necessary
// Assumes lock is already held
func (bp *BufferPool) addPage(page *page.Page) error {
	// If page already in pool, just update access order
	if _, exists := bp.pages[page.ID]; exists {
		bp.updateAccessOrder(page.ID)
		return nil
	}

	// If at capacity, evict LRU page
	if len(bp.pages) >= bp.capacity {
		if err := bp.evictLRU(); err != nil {
			return fmt.Errorf("failed to evict page: %w", err)
		}
	}

	// Add page to pool
	bp.pages[page.ID] = page
	bp.updateAccessOrder(page.ID)

	return nil
}

// evictLRU evicts the least recently used unpinned page
// Assumes lock is already held
func (bp *BufferPool) evictLRU() error {
	// Find first unpinned page in access order (LRU)
	for i := 0; i < len(bp.accessOrder); i++ {
		pageID := bp.accessOrder[i]
		page, exists := bp.pages[pageID]

		if !exists {
			// Remove from access order if page doesn't exist
			bp.accessOrder = append(bp.accessOrder[:i], bp.accessOrder[i+1:]...)
			i--
			continue
		}

		page.Lock()
		pinCount := page.PinCount
		isDirty := page.IsDirty

		// Skip pinned pages
		if pinCount > 0 {
			page.Unlock()
			continue
		}

		fmt.Printf("[BufferPool] EVICT pageID=%d dirty=%v\n", pageID, isDirty)
		// Flush if dirty
		if isDirty && bp.diskManager != nil {
			if bp.walManager != nil {
				if page.LSN > bp.walManager.GetFlushedLSN() {
					// Can't evict this page yet — WAL not durable
					page.Unlock()
					continue // skip this page, try next LRU candidate
				}
			}
			if err := bp.diskManager.WritePage(page); err != nil {
				page.Unlock()
				return fmt.Errorf("failed to write page %d during eviction: %w", pageID, err)
			}
			page.IsDirty = false
		}
		page.Unlock()

		// Evict the page
		delete(bp.pages, pageID)
		bp.accessOrder = append(bp.accessOrder[:i], bp.accessOrder[i+1:]...)
		return nil
	}

	return fmt.Errorf("all pages are pinned, cannot evict")
}

// updateAccessOrder moves a page to the end of access order (most recently used)
// Assumes lock is already held
func (bp *BufferPool) updateAccessOrder(pageID int64) {
	// Remove from current position
	for i, id := range bp.accessOrder {
		if id == pageID {
			bp.accessOrder = append(bp.accessOrder[:i], bp.accessOrder[i+1:]...)
			break
		}
	}
	// Add to end (most recently used)
	bp.accessOrder = append(bp.accessOrder, pageID)
}

// DeletePage removes a page from the buffer pool (used after deletion)
func (bp *BufferPool) DeletePage(pageID int64) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	page, exists := bp.pages[pageID]
	if !exists {
		return nil // Already not in pool
	}

	page.Lock()
	if page.PinCount > 0 {
		page.Unlock()
		return fmt.Errorf("cannot delete pinned page %d", pageID)
	}
	page.Unlock()

	// Remove from pool and access order
	delete(bp.pages, pageID)
	for i, id := range bp.accessOrder {
		if id == pageID {
			bp.accessOrder = append(bp.accessOrder[:i], bp.accessOrder[i+1:]...)
			break
		}
	}

	return nil
}
