package heapfile

import (
	"fmt"
	"os"
)

// NewHeapFilePager creates a new disk-based pager for heap file data storage
func NewHeapFilePager(heapPath string) (*HeapFilePager, error) {
	// Open or create the heap data file
	file, err := os.OpenFile(heapPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open heap file %s: %w", heapPath, err)
	}

	// Get file size to determine number of existing pages
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat heap file: %w", err)
	}

	fileSize := stat.Size()
	numPages := fileSize / int64(PageSize)
	nextPageID := numPages

	// If file is empty, start from page 0
	if numPages == 0 {
		nextPageID = 0
	}

	pager := &HeapFilePager{
		file:     file,
		filePath: heapPath,
		pageSize: PageSize,
		nextPage: nextPageID,
	}

	return pager, nil
}

// readPage reads a 4KB page from disk at the given page number using the pager
func (hf *HeapFile) readPage(pageNum uint32) ([]byte, error) {
	return hf.pager.ReadPage(int64(pageNum))
}

// writePage writes a 4KB page to disk at the given page number using the pager
func (hf *HeapFile) writePage(pageNum uint32, page []byte) error {
	return hf.pager.WritePage(int64(pageNum), page)
}

func (p *HeapFilePager) ReadPage(pageID int64) ([]byte, error) {
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
func (p *HeapFilePager) WritePage(pageID int64, data []byte) error {
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

	// **CRITICAL:** update nextPage so TotalPages() reflects newly created pages
	if pageID >= p.nextPage {
		p.nextPage = pageID + 1
	}

	return nil
}

// initializePage initializes a new empty page with header and empty slot directory
func (hf *HeapFile) initializePage(pageNo uint32) error {
	page := make([]byte, PageSize)

	// Initialize header
	header := PageHeader{
		FileID:      hf.fileID,
		PageNo:      pageNo,
		FreePtr:     PageHeaderSize, // Start data area right after header
		NumRows:     0,
		NumRowsFree: PageSize - PageHeaderSize,
		IsPageFull:  0,
		SlotCount:   0, // No slots initially
	}

	writePageHeader(page, &header)

	// Write page to disk using pager
	return hf.pager.WritePage(int64(pageNo), page)
}

// AllocatePage allocates a new page and returns its ID
func (p *HeapFilePager) AllocatePage() (int64, error) {
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
func (p *HeapFilePager) DeallocatePage(pageID int64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.file == nil {
		return fmt.Errorf("pager file is closed")
	}

	return nil
}

// findSuitablePage finds a page with enough space for the required row size
func (hf *HeapFile) findSuitablePage(requiredSpace uint16) (uint32, error) {
	// Try reading pages sequentially until we find a suitable one or hit the end
	// Start from page 0 and go up
	pageNum := uint32(0)
	for {
		page, err := hf.readPage(pageNum)
		if err != nil {
			// Page doesn't exist yet, create it
			if err := hf.initializePage(pageNum); err != nil {
				return 0, err
			}
			return pageNum, nil
		}

		header := readPageHeader(page)

		// Check if page is full
		if header.IsPageFull != 0 {
			pageNum++
			continue
		}

		// Calculate available space (considering slot directory)
		availableSpace := calculateFreeSpace(header)
		requiredWithSlot := requiredSpace + SlotSize // row + new slot entry

		if availableSpace >= requiredWithSlot {
			return pageNum, nil
		}

		// This page doesn't have enough space, try next
		pageNum++
	}
}

// Sync flushes all pending writes to disk
func (p *HeapFilePager) Sync() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.file == nil {
		return fmt.Errorf("pager file is closed")
	}

	return p.file.Sync()
}

// Close closes the index file
func (p *HeapFilePager) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.file == nil {
		return nil
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

// TotalPages returns the total number of pages in the heap file
func (p *HeapFilePager) TotalPages() int64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.nextPage
}
