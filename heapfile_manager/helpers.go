package heapfile

import (
	"fmt"
	"path/filepath"
)

func (hfm *HeapFileManager) UpdateBaseDir(dir string) {
	hfm.mu.Lock()
	defer hfm.mu.Unlock()
	hfm.baseDir = dir
}

func (hfm *HeapFileManager) LoadHeapFile(fileID uint32, tableName string) (*HeapFile, error) {

	// fmt.Printf("\nLoading Tables..... \n%s --> %d\n", tableName, fileID)
	hfm.mu.Lock()
	defer hfm.mu.Unlock()

	// Already loaded?
	if hf, exists := hfm.files[fileID]; exists {
		return hf, nil
	}

	filePath := filepath.Join(hfm.baseDir, fmt.Sprintf("%s_%d.heap", tableName, fileID))

	// Create OnDiskPager for the heap file
	pager, err := NewHeapFilePager(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open heap file %s: %w", filePath, err)
	}

	hf := &HeapFile{
		fileID:   fileID,
		pager:    pager,
		filePath: filePath,
	}

	hfm.files[fileID] = hf
	// fmt.Printf("\nLoaded heap file %d (%s)\n", fileID, filePath)
	return hf, nil
}

// InsertRow inserts a row into the specified heap file
func (hfm *HeapFileManager) InsertRow(fileID uint32, rowData []byte) (*RowPointer, error) {
	hfm.mu.RLock()
	heapFile, exists := hfm.files[fileID]
	hfm.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("heap file %d not found", fileID)
	}

	return heapFile.insertRow(rowData)
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

func (hf *HeapFile) GetAllRowPointers() []*RowPointer {
	hf.mu.RLock()
	defer hf.mu.RUnlock()

	var result []*RowPointer

	totalPages := hf.pager.TotalPages()
	// fmt.Println("total pages:", totalPages)

	for pageID := int64(0); pageID < totalPages; pageID++ {
		pageData, err := hf.pager.ReadPage(pageID)
		if err != nil {
			fmt.Printf("Error reading page %d: %v\n", pageID, err)
			continue
		}

		// page header to get slot count
		header := readPageHeader(pageData)
		if header == nil {
			continue
		}

		for slotIdx := uint16(0); slotIdx < header.SlotCount; slotIdx++ {
			slot := readSlot(pageData, slotIdx)
			// if slot is valid (not deleted/empty)
			if slot != nil && slot.Offset != 0 && slot.Length != 0 {
				result = append(result, &RowPointer{
					FileID:     hf.fileID,
					PageNumber: uint32(pageID),
					SlotIndex:  slotIdx,
				})
			}
		}
	}

	return result
}
