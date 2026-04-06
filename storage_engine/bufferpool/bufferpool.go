package bufferpool

import (
	diskmanager "DaemonDB/storage_engine/disk_manager"
	"DaemonDB/storage_engine/page"
	"DaemonDB/types"
	"encoding/binary"
	"fmt"
	"time"
)

var SilenceLogs bool

// NewBufferPool creates a buffer pool with the given capacity and eviction policy.
// If policy is nil, LRU-K (K=2) is used as the default.
func NewBufferPool(capacity int, diskManager *diskmanager.DiskManager, policy EvictionPolicy) *BufferPool {
	if policy == nil {
		policy = NewLRUKPolicy(2)
	}
	return &BufferPool{
		pages:       make(map[int64]*page.Page, capacity),
		capacity:    capacity,
		diskManager: diskManager,
		policy:      policy,
	}
}

func (bp *BufferPool) SetWALManager(wal WALFlushedLSNGetter) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.walManager = wal
}

func (bp *BufferPool) SetEvictDebug(enabled bool) {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	bp.debugEvict = enabled
}

// FetchPage retrieves a page from the buffer pool, loading from disk if necessary.
func (bp *BufferPool) FetchPage(pageID int64) (*page.Page, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if pg, exists := bp.pages[pageID]; exists {
		if !SilenceLogs {
			fmt.Printf("[BufferPool] HIT  pageID=%d pinCount=%d policy=%s\n", pageID, pg.PinCount, bp.policy.Name())
		}
		bp.hits++
		bp.policy.OnAccess(pageID)
		pg.Lock()
		pg.PinCount++
		pg.Unlock()
		return pg, nil
	}

	bp.misses++

	if !SilenceLogs {
		fmt.Printf("[BufferPool] MISS pageID=%d — loading from disk\n", pageID)
	}
	if bp.diskManager == nil {
		return nil, fmt.Errorf("disk manager not set")
	}

	start := time.Now()
	pg, err := bp.diskManager.ReadPage(pageID)
	if err != nil {
		return nil, fmt.Errorf("failed to read page %d from disk: %w", pageID, err)
	}
	_ = time.Since(start)

	if pg.PageType == types.PageTypeHeapData {
		if len(pg.Data) >= 8 {
			pg.LSN = binary.LittleEndian.Uint64(pg.Data[page.PageLSNOffset:])
		}
	}

	if err := bp.addPage(pg); err != nil {
		return nil, fmt.Errorf("failed to add page to buffer pool: %w", err)
	}

	pg.Lock()
	pg.PinCount++
	pg.Unlock()

	return pg, nil
}

// NewPage allocates a new page on disk, adds it to the pool, and pins it.
func (bp *BufferPool) NewPage(fileID uint32, pageType types.PageType) (*page.Page, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if bp.diskManager == nil {
		return nil, fmt.Errorf("disk manager not set")
	}

	pageID, err := bp.diskManager.AllocatePage(fileID, pageType)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate page: %w", err)
	}

	pg := diskmanager.NewPage(pageID, fileID, pageType)
	pg.IsDirty = true

	pg.Lock()
	pg.PinCount++
	pg.Unlock()

	if err := bp.addPage(pg); err != nil {
		pg.Lock()
		pg.PinCount--
		pg.Unlock()
		return nil, fmt.Errorf("failed to add new page to buffer pool: %w", err)
	}

	return pg, nil
}

// UnpinPage decrements the pin count for a page.
func (bp *BufferPool) UnpinPage(pageID int64, isDirty bool) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	pg, exists := bp.pages[pageID]
	if !exists {
		return fmt.Errorf("page %d not in buffer pool", pageID)
	}

	pg.Lock()
	defer pg.Unlock()

	if pg.PinCount > 0 {
		pg.PinCount--
	}
	if isDirty {
		pg.IsDirty = true
	}
	return nil
}

// FlushPage writes a specific dirty page to disk, respecting the WAL rule.
func (bp *BufferPool) FlushPage(pageID int64) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	pg, exists := bp.pages[pageID]
	if !exists {
		return fmt.Errorf("page %d not in buffer pool", pageID)
	}

	pg.Lock()
	defer pg.Unlock()

	if !pg.IsDirty {
		return nil
	}

	if bp.walManager != nil {
		pageLSN := pg.LSN
		flushedLSN := bp.walManager.GetFlushedLSN()
		if pageLSN > flushedLSN {
			if !SilenceLogs {
				fmt.Printf("[BufferPool] FLUSH BLOCKED pageID=%d pageLSN=%d flushedLSN=%d\n",
					pageID, pageLSN, flushedLSN)
			}
			return fmt.Errorf(
				"cannot flush page %d: pageLSN=%d not yet covered by WAL flushedLSN=%d",
				pageID, pageLSN, flushedLSN)
		}
		if !SilenceLogs {
			fmt.Printf("[BufferPool] FLUSH pageID=%d pageLSN=%d flushedLSN=%d\n",
				pageID, pageLSN, flushedLSN)
		}
	}

	if err := bp.diskManager.WritePage(pg); err != nil {
		return fmt.Errorf("failed to flush page %d: %w", pageID, err)
	}
	pg.IsDirty = false
	return nil
}

// FlushAllPages writes all dirty pages to disk, skipping WAL-blocked ones.
func (bp *BufferPool) FlushAllPages() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	if bp.diskManager == nil {
		return fmt.Errorf("disk manager not set")
	}

	if !SilenceLogs {
		fmt.Printf("[BufferPool] FlushAllPages — pool size=%d policy=%s\n", len(bp.pages), bp.policy.Name())
	}
	for pageID, pg := range bp.pages {
		pg.Lock()
		if pg.IsDirty {
			if bp.walManager != nil && pg.LSN > bp.walManager.GetFlushedLSN() {
				pg.Unlock()
				continue
			}
			if err := bp.diskManager.WritePage(pg); err != nil {
				pg.Unlock()
				return fmt.Errorf("failed to flush page %d: %w", pageID, err)
			}
			fmt.Printf("[BufferPool]   flushing pageID=%d\n", pageID)
			pg.IsDirty = false
		}
		pg.Unlock()
	}
	return nil
}

// DeletePage removes a page from the buffer pool.
func (bp *BufferPool) DeletePage(pageID int64) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	pg, exists := bp.pages[pageID]
	if !exists {
		return nil
	}

	pg.Lock()
	if pg.PinCount > 0 {
		pg.Unlock()
		return fmt.Errorf("cannot delete pinned page %d", pageID)
	}
	pg.Unlock()

	bp.policy.OnEvict(pageID) // ← notify policy to clean up its metadata
	delete(bp.pages, pageID)
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal helpers — all assume bp.mu is held by the caller.
// ─────────────────────────────────────────────────────────────────────────────

// addPage inserts a page into the pool, evicting a victim first if full.
func (bp *BufferPool) addPage(pg *page.Page) error {
	if _, exists := bp.pages[pg.ID]; exists {
		bp.policy.OnAccess(pg.ID)
		return nil
	}

	if len(bp.pages) >= bp.capacity {
		if err := bp.evict(); err != nil {
			return fmt.Errorf("eviction failed: %w", err)
		}
	}

	bp.pages[pg.ID] = pg
	bp.policy.OnAccess(pg.ID) // record first access
	return nil
}

// evict picks a victim via the active policy, flushes it if dirty, and
// removes it from the pool.
func (bp *BufferPool) evict() error {
	// Build a WAL-aware candidate set: wrap pages map but let the policy
	// pick — it skips pinned pages. We handle WAL-blocked pages here by
	// retrying with a reduced candidate set if needed.
	victim := bp.policy.ChooseVictim(bp.pages)
	if victim == -1 {
		return fmt.Errorf("all pages are pinned, cannot evict")
	}

	pg := bp.pages[victim]
	pg.Lock()

	// WAL safety: if the chosen victim is dirty and its LSN isn't durable
	// yet, fall back — iterate remaining pages for an unblocked candidate.
	if pg.IsDirty && bp.walManager != nil && pg.LSN > bp.walManager.GetFlushedLSN() {
		pg.Unlock()
		// Linear scan for a WAL-safe unpinned page.
		victim = bp.walSafeVictim()
		if victim == -1 {
			return fmt.Errorf("all pages are pinned or WAL-blocked, cannot evict")
		}
		pg = bp.pages[victim]
		pg.Lock()
	}

	if pg.IsDirty && bp.diskManager != nil {
		if err := bp.diskManager.WritePage(pg); err != nil {
			pg.Unlock()
			return fmt.Errorf("failed to write page %d during eviction: %w", victim, err)
		}
		pg.IsDirty = false
	}

	if bp.debugEvict && !SilenceLogs {
		fmt.Printf("[BufferPool] EVICT pageID=%d policy=%s\n", victim, bp.policy.Name())
	}
	pg.Unlock()

	bp.policy.OnEvict(victim)
	delete(bp.pages, victim)
	return nil
}

// walSafeVictim finds any unpinned page whose dirty LSN is already covered
// by the WAL, or any clean unpinned page.  O(n) fallback, called rarely.
func (bp *BufferPool) walSafeVictim() int64 {
	for pageID, pg := range bp.pages {
		pg.Lock()
		pinned := pg.PinCount > 0
		walBlocked := pg.IsDirty && bp.walManager != nil && pg.LSN > bp.walManager.GetFlushedLSN()
		pg.Unlock()
		if !pinned && !walBlocked {
			return pageID
		}
	}
	return -1
}
