package heapfile

import (
	"fmt"
)

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

	// Write page to disk
	offset := int64(pageNo) * PageSize
	_, err := hf.file.WriteAt(page, offset)
	return err
}

// findSuitablePage finds a page with enough space for the required row size
func (hf *HeapFile) findSuitablePage(requiredSpace uint16) (uint32, error) {
	// Get file size
	stat, err := hf.file.Stat()
	if err != nil {
		return 0, err
	}

	numPages := uint32(stat.Size() / PageSize)
	for pageNum := uint32(0); pageNum < numPages; pageNum++ {
		page, err := hf.readPage(pageNum)
		if err != nil {
			continue
		}

		header := readPageHeader(page)

		// Check if page is full
		if header.IsPageFull != 0 {
			continue
		}

		// Calculate available space (considering slot directory)
		availableSpace := calculateFreeSpace(header)
		requiredWithSlot := requiredSpace + SlotSize // row + new slot entry

		if availableSpace >= requiredWithSlot {
			return pageNum, nil
		}
	}

	// No page found, create new one
	newPageNum := numPages
	if err := hf.initializePage(newPageNum); err != nil {
		return 0, err
	}

	return newPageNum, nil
}

// insertRow inserts a row into the heap file and returns a RowPointer
func (hf *HeapFile) insertRow(rowData []byte) (*RowPointer, error) {
	hf.mu.Lock()
	defer hf.mu.Unlock()

	rowLen := uint16(len(rowData))
	maxRowSize := uint16(PageSize - PageHeaderSize - SlotSize) // Leave space for at least one slot
	if rowLen > maxRowSize {
		return nil, fmt.Errorf("row too large: %d bytes (max: %d)", rowLen, maxRowSize)
	}

	// Find a page with enough capacity
	pageNum, err := hf.findSuitablePage(rowLen)
	if err != nil {
		return nil, err
	}

	// Read the page
	page, err := hf.readPage(pageNum)
	if err != nil {
		return nil, err
	}

	header := readPageHeader(page)

	// Calculate required space (row + new slot entry)
	requiredSpace := rowLen + SlotSize
	availableSpace := calculateFreeSpace(header)

	if availableSpace < requiredSpace {
		// Page doesn't have space, try next page (recursive)
		return hf.insertRow(rowData)
	}

	// Write row data at freePtr
	rowOffset := header.FreePtr
	copy(page[rowOffset:rowOffset+rowLen], rowData)

	// Add slot entry (this updates SlotCount in the page)
	slotIndex := addSlot(page, rowOffset, rowLen)

	// Re-read header to get updated SlotCount
	header = readPageHeader(page)

	// Update header
	header.FreePtr += rowLen
	header.NumRows++
	header.NumRowsFree = calculateFreeSpace(header)

	// Mark page as full if no space left for another row
	if header.NumRowsFree < (rowLen + SlotSize) {
		header.IsPageFull = 1
	}

	writePageHeader(page, header)

	// Write page back to disk
	if err := hf.writePage(pageNum, page); err != nil {
		return nil, err
	}

	return &RowPointer{
		FileID:     hf.fileID,
		PageNumber: pageNum,
		SlotIndex:  slotIndex,
	}, nil
}

// GetRow retrieves a row from the heap file using a RowPointer
func (hf *HeapFile) GetRow(ptr *RowPointer) ([]byte, error) {
	hf.mu.RLock()
	defer hf.mu.RUnlock()

	// PageNumber is uint32, so it's always >= 0

	// Read the page
	page, err := hf.readPage(ptr.PageNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to read page %d: %w", ptr.PageNumber, err)
	}

	// Read slot entry
	slot := readSlot(page, ptr.SlotIndex)
	if slot == nil {
		return nil, fmt.Errorf("invalid slot at index %d", ptr.SlotIndex)
	}
	if slot.Offset == 0 || slot.Length == 0 {
		return nil, fmt.Errorf("invalid slot at index %d", ptr.SlotIndex)
	}

	// Extract row data
	rowData := getRowData(page, slot)
	if rowData == nil {
		return nil, fmt.Errorf("failed to read row data from slot %d", ptr.SlotIndex)
	}

	return rowData, nil
}
