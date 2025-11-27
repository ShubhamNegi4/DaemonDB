package bplus

import (
	"fmt"
	"os"
	"sync"
)

// OnDiskPager implements the Pager interface for disk-based storage
type OnDiskPager struct {
	file     *os.File
	filePath string
	pageSize int
	nextPage int64 // Next available page ID
	mu       sync.RWMutex
}

// NewOnDiskPager creates a new disk-based pager for B+ tree index storage
func NewOnDiskPager(indexPath string) (*OnDiskPager, error) {
	// Open or create the index file
	file, err := os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open index file %s: %w", indexPath, err)
	}

	// Get file size to determine number of existing pages
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat index file: %w", err)
	}

	fileSize := stat.Size()
	numPages := fileSize / int64(PageSize)
	nextPageID := numPages // Next page ID is after existing pages

	// If file is empty, nextPageID should be 1 (page 0 is reserved for metadata if needed)
	if numPages == 0 {
		nextPageID = 1
	}

	pager := &OnDiskPager{
		file:     file,
		filePath: indexPath,
		pageSize: PageSize,
		nextPage: nextPageID,
	}

	return pager, nil
}

// ReadPage reads a 4KB page from disk at the given page ID
func (p *OnDiskPager) ReadPage(pageID int64) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.file == nil {
		return nil, fmt.Errorf("pager file is closed")
	}

	page := make([]byte, p.pageSize)
	offset := pageID * int64(p.pageSize)

	n, err := p.file.ReadAt(page, offset)
	if err != nil {
		// If we read less than a full page and it's not EOF, return error
		if n == 0 {
			return nil, fmt.Errorf("failed to read page %d: %w", pageID, err)
		}
		// If partial read, pad with zeros
		if n < p.pageSize {
			for i := n; i < p.pageSize; i++ {
				page[i] = 0
			}
		}
	}

	return page, nil
}

// WritePage writes a 4KB page to disk at the given page ID
func (p *OnDiskPager) WritePage(pageID int64, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.file == nil {
		return fmt.Errorf("pager file is closed")
	}

	// Ensure data is exactly pageSize bytes
	if len(data) != p.pageSize {
		return fmt.Errorf("data size %d does not match page size %d", len(data), p.pageSize)
	}

	offset := pageID * int64(p.pageSize)
	_, err := p.file.WriteAt(data, offset)
	if err != nil {
		return fmt.Errorf("failed to write page %d: %w", pageID, err)
	}

	return nil
}

// AllocatePage allocates a new page and returns its ID
func (p *OnDiskPager) AllocatePage() (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.file == nil {
		return 0, fmt.Errorf("pager file is closed")
	}

	pageID := p.nextPage
	p.nextPage++

	// Initialize the new page with zeros
	emptyPage := make([]byte, p.pageSize)
	offset := pageID * int64(p.pageSize)
	_, err := p.file.WriteAt(emptyPage, offset)
	if err != nil {
		return 0, fmt.Errorf("failed to allocate page %d: %w", pageID, err)
	}

	return pageID, nil
}

// DeallocatePage marks a page as free (for now, just a no-op)
// In a full implementation, you'd maintain a free page list
func (p *OnDiskPager) DeallocatePage(pageID int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.file == nil {
		return fmt.Errorf("pager file is closed")
	}

	// For now, we don't actually deallocate pages (they remain in the file)
	// In a full implementation, you would:
	// 1. Add pageID to a free page list
	// 2. Optionally zero out the page
	// 3. Track free pages for reuse

	return nil
}

// Sync flushes all pending writes to disk
func (p *OnDiskPager) Sync() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.file == nil {
		return fmt.Errorf("pager file is closed")
	}

	return p.file.Sync()
}

// Close closes the index file
func (p *OnDiskPager) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.file == nil {
		return nil // Already closed
	}

	err := p.file.Sync() // Flush before closing
	if err != nil {
		p.file.Close()
		return fmt.Errorf("failed to sync before close: %w", err)
	}

	err = p.file.Close()
	p.file = nil // Mark as closed
	return err
}

func (p *OnDiskPager) TotalPages() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.nextPage // if next is 20, meaning 19 pages are full
}
