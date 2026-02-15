package heapfile

import (
	"fmt"
)

// this file contains internal functions, they do ont contain locks.
// but it is to be ensured that the external functions for each should contain locks to avoid cirtical section

// insertRow inserts a row into the heap file and returns a RowPointer.
func (hf *HeapFile) insertRow(rowData []byte, opLSN uint64) (*RowPointer, error) {

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
		return hf.insertRow(rowData, opLSN)
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

	header.LastAppliedLSN = opLSN

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

func (hf *HeapFile) getRow(ptr *RowPointer) ([]byte, error) {

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

	var result []*RowPointer

	totalPages := hf.pager.TotalPages()

	for pageID := int64(0); pageID < totalPages; pageID++ {
		pageData, err := hf.pager.readPage(pageID)
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
func (hf *HeapFile) deleteRow(ptr *RowPointer, opLSN uint64) error {

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

	header.LastAppliedLSN = opLSN
	writePageHeader(page, header)

	if err := hf.writePage(ptr.PageNumber, page); err != nil {
		return fmt.Errorf("failed to write page %d: %w", ptr.PageNumber, err)
	}

	return nil
}

func (hf *HeapFile) updateRow(ptr *RowPointer, newRowData []byte, opLSN uint64) error {

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
		return fmt.Errorf("slot %d is not occupied, nothing to update", ptr.SlotIndex)
	}

	newRowLen := uint16(len(newRowData))

	if newRowLen > slot.Length {
		// If new data is larger, we need to delete and re-insert

		// Delete the old row
		if err := hf.deleteRow(ptr, opLSN); err != nil {
			return fmt.Errorf("failed to delete old row for update: %w", err)
		}

		// Insert the new row (this may return a different RowPointer)
		newRP, err := hf.insertRow(newRowData, opLSN)
		if err != nil {
			return fmt.Errorf("failed to insert updated row: %w", err)
		}

		// Update the original row pointer to point to new location
		*ptr = *newRP
		return nil
	}

	// New data fits in existing slot (in-place update)
	// Copy new data to the slot's offset
	copy(page[slot.Offset:slot.Offset+newRowLen], newRowData)

	// Update slot length if it changed
	if newRowLen != slot.Length {
		slot.Length = newRowLen
		writeSlot(page, ptr.SlotIndex, slot)

		header.NumRowsFree = calculateFreeSpace(header)
	}

	header.LastAppliedLSN = opLSN
	writePageHeader(page, header)

	// Write the updated page back to disk
	if err := hf.writePage(ptr.PageNumber, page); err != nil {
		return fmt.Errorf("failed to write page %d: %w", ptr.PageNumber, err)
	}

	return nil
}

// checkPageLSN checks if an operation has already been applied to a page
func (hf *HeapFile) CheckPageLSN(pageNum uint32, opLSN uint64) (bool, error) {
	totalPages := uint32(hf.pager.TotalPages())
	if pageNum >= totalPages {
		// Page doesn't exist = operation not applied yet
		return false, nil
	}

	page, err := hf.readPage(pageNum)
	if err != nil {
		return false, err
	}

	header := readPageHeader(page)
	if header == nil {
		return false, fmt.Errorf("failed to read page header")
	}

	// If page LSN >= operation LSN, operation already applied
	return header.LastAppliedLSN >= opLSN, nil
}
