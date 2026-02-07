package heapfile

import (
	"fmt"
)

// insertRow inserts a row into the heap file and returns a RowPointer.
func (hf *HeapFile) insertRow(rowData []byte) (*RowPointer, error) {
	hf.mu.Lock()
	defer hf.mu.Unlock()

	rowLen := uint16(len(rowData))
	maxRowSize := uint16(PageSize - PageHeaderSize - SlotSize)
	if rowLen > maxRowSize {
		return nil, fmt.Errorf("row too large: %d bytes (max: %d)", rowLen, maxRowSize)
	}

	pageNum, err := hf.findSuitablePage(rowLen)
	if err != nil {
		return nil, err
	}

	page, err := hf.readPage(pageNum)
	if err != nil {
		return nil, err
	}

	header := readPageHeader(page)

	requiredSpace := rowLen + SlotSize
	availableSpace := calculateFreeSpace(header)

	if availableSpace < requiredSpace {
		return hf.insertRow(rowData)
	}

	rowOffset := header.FreePtr
	copy(page[rowOffset:rowOffset+rowLen], rowData)

	slotIndex := addSlot(page, rowOffset, rowLen)

	header = readPageHeader(page)

	header.FreePtr += rowLen
	header.NumRows++
	header.NumRowsFree = calculateFreeSpace(header)

	if header.NumRowsFree < (rowLen + SlotSize) {
		header.IsPageFull = 1
	}

	writePageHeader(page, header)

	if err := hf.writePage(pageNum, page); err != nil {
		return nil, err
	}

	return &RowPointer{
		FileID:     hf.fileID,
		PageNumber: pageNum,
		SlotIndex:  slotIndex,
	}, nil
}

// GetRow retrieves a row from the heap file using a RowPointer.
func (hf *HeapFile) GetRow(ptr *RowPointer) ([]byte, error) {
	hf.mu.RLock()
	defer hf.mu.RUnlock()

	page, err := hf.readPage(ptr.PageNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to read page %d: %w", ptr.PageNumber, err)
	}

	slot := readSlot(page, ptr.SlotIndex)
	if slot == nil {
		return nil, fmt.Errorf("invalid slot at index %d", ptr.SlotIndex)
	}
	if slot.Offset == 0 || slot.Length == 0 {
		return nil, fmt.Errorf("invalid slot at index %d", ptr.SlotIndex)
	}

	rowData := getRowData(page, slot)
	if rowData == nil {
		return nil, fmt.Errorf("failed to read row data from slot %d", ptr.SlotIndex)
	}

	return rowData, nil
}

// GetAllRowPointers returns all valid row pointers in the heap file (full table scan).
func (hf *HeapFile) GetAllRowPointers() []*RowPointer {
	hf.mu.RLock()
	defer hf.mu.RUnlock()

	var result []*RowPointer

	totalPages := hf.pager.TotalPages()

	for pageID := int64(0); pageID < totalPages; pageID++ {
		pageData, err := hf.pager.ReadPage(pageID)
		if err != nil {
			continue
		}

		header := readPageHeader(pageData)
		if header == nil {
			continue
		}

		for slotIdx := uint16(0); slotIdx < header.SlotCount; slotIdx++ {
			slot := readSlot(pageData, slotIdx)
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

// deleteRow tombstones a row by zeroing its slot (Offset=0, Length=0).
func (hf *HeapFile) deleteRow(ptr *RowPointer) error {
	hf.mu.Lock()
	defer hf.mu.Unlock()

	page, err := hf.readPage(ptr.PageNumber)
	if err != nil {
		return fmt.Errorf("failed to read page %d: %w", ptr.PageNumber, err)
	}

	header := readPageHeader(page)
	if header == nil {
		return fmt.Errorf("failed to read page header for page %d", ptr.PageNumber)
	}
	if ptr.SlotIndex >= header.SlotCount {
		return fmt.Errorf("invalid slot index %d (slotCount=%d)", ptr.SlotIndex, header.SlotCount)
	}

	slot := readSlot(page, ptr.SlotIndex)
	if slot == nil {
		return fmt.Errorf("invalid slot at index %d", ptr.SlotIndex)
	}

	if slot.Offset == 0 || slot.Length == 0 {
		return nil
	}

	slot.Offset = 0
	slot.Length = 0
	writeSlot(page, ptr.SlotIndex, slot)

	if header.NumRows > 0 {
		header.NumRows--
	}
	header.IsPageFull = 0
	header.NumRowsFree = calculateFreeSpace(header)
	writePageHeader(page, header)

	if err := hf.writePage(ptr.PageNumber, page); err != nil {
		return fmt.Errorf("failed to write page %d: %w", ptr.PageNumber, err)
	}

	return nil
}
