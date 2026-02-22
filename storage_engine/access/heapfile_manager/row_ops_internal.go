package heapfile

import (
	"DaemonDB/types"
	"fmt"
)

// this file contains internal functions, they do not contain locks.
// but it is to be ensured that the external functions for each should contain locks to avoid cirtical section

// insertRow inserts a row into the heap file and returns a RowPointer.
func (hf *HeapFile) insertRow(rowData []byte, opLSN uint64) (*types.RowPointer, error) {

	rowLen := uint16(len(rowData))
	maxRowSize := uint16(types.PageSize - types.HeapPageHeaderSize - types.SlotSize - 1) // -1 for page type
	if rowLen > maxRowSize {
		return nil, fmt.Errorf("row too large: %d bytes (max: %d)", rowLen, maxRowSize)
	}

	for {
		pg, localPageNum, err := hf.findSuitablePage(uint16(rowLen))
		if err != nil {
			return nil, fmt.Errorf("failed to find suitable page: %w", err)
		}

		pg.Lock()

		// Double-check space after acquiring lock — another goroutine may
		// have filled this page between findSuitablePage and Lock.
		if FreeSpace(pg) < int(rowLen) {
			pg.Unlock()
			hf.bufferPool.UnpinPage(pg.ID, false)
			continue // retry — findSuitablePage will allocate a new page
		}

		slotIndex, err := InsertRecord(pg, rowData)
		if err != nil {
			// InsertRecord only fails if space check is wrong — shouldn't happen
			// after FreeSpace check above, but handle it cleanly.
			pg.Unlock()
			hf.bufferPool.UnpinPage(pg.ID, false)
			return nil, fmt.Errorf("failed to insert record into page: %w", err)
		}

		SetLastAppliedLSN(pg, opLSN)
		pg.Unlock()
		hf.bufferPool.UnpinPage(pg.ID, true) // unpin but dont flush it yet

		fmt.Printf("[Heap] INSERT fileID=%d page=%d slot=%d lsn=%d\n", hf.fileID, localPageNum, slotIndex, opLSN)

		return &types.RowPointer{
			FileID:     hf.fileID,
			PageNumber: localPageNum,
			SlotIndex:  slotIndex,
		}, nil
	}
}

func (hf *HeapFile) getRow(ptr *types.RowPointer) ([]byte, error) {

	globalPageID, err := hf.diskManager.GetGlobalPageID(hf.fileID, int64(ptr.PageNumber))
	if err != nil {
		return nil, fmt.Errorf("failed to resolve page %d: %w", ptr.PageNumber, err)
	}

	pg, err := hf.bufferPool.FetchPage(globalPageID)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch page %d: %w", globalPageID, err)
	}
	defer hf.bufferPool.UnpinPage(pg.ID, false)

	pg.RLock()
	defer pg.RUnlock()

	return GetRecord(pg, ptr.SlotIndex)
}

// GetAllRowPointers returns all valid row pointers in the heap file (full table scan).
func (hf *HeapFile) GetAllRowPointers() []types.RowPointer {

	var result []types.RowPointer

	fd, err := hf.diskManager.GetFileDescriptor(hf.fileID)
	if err != nil {
		return result
	}

	totalPages := fd.NextPageID

	for localPageNum := int64(0); localPageNum < totalPages; localPageNum++ {
		globalPageID, err := hf.diskManager.GetGlobalPageID(hf.fileID, localPageNum)
		if err != nil {
			continue
		}

		pg, err := hf.bufferPool.FetchPage(globalPageID)
		if err != nil {
			continue
		}

		pg.RLock()

		// Skip non-heap pages or uninitialized pages.
		if pg.PageType != types.PageTypeHeapData {
			pg.RUnlock()
			hf.bufferPool.UnpinPage(globalPageID, false)
			continue
		}

		slotCount := GetSlotCount(pg)
		for slotIdx := uint16(0); slotIdx < slotCount; slotIdx++ {
			if IsSlotLive(pg, slotIdx) {
				result = append(result, types.RowPointer{
					FileID:     hf.fileID,
					PageNumber: uint32(localPageNum), // ← local
					SlotIndex:  slotIdx,
				})
			}
		}
		pg.RUnlock()
		hf.bufferPool.UnpinPage(globalPageID, false)
	}

	return result
}

// deleteRow tombstones a row by zeroing its slot (Offset=0, Length=0).
func (hf *HeapFile) deleteRow(ptr *types.RowPointer, opLSN uint64) error {
	globalPageID, err := hf.diskManager.GetGlobalPageID(hf.fileID, int64(ptr.PageNumber))
	if err != nil {
		return fmt.Errorf("failed to resolve local page %d: %w", ptr.PageNumber, err)
	}

	fmt.Printf("[Heap] DELETE fileID=%d page=%d slot=%d lsn=%d\n", ptr.FileID, ptr.PageNumber, ptr.SlotIndex, opLSN)

	pg, err := hf.bufferPool.FetchPage(globalPageID)
	if err != nil {
		return fmt.Errorf("failed to fetch page %d: %w", globalPageID, err)
	}
	defer hf.bufferPool.UnpinPage(pg.ID, true)

	pg.Lock()
	defer pg.Unlock()

	if err := DeleteRecord(pg, ptr.SlotIndex); err != nil {
		return err
	}
	SetLastAppliedLSN(pg, opLSN)
	return nil
}

func (hf *HeapFile) updateRow(ptr *types.RowPointer, newRowData []byte, opLSN uint64) error {

	fmt.Printf("[Heap] UPDATE fileID=%d page=%d slot=%d lsn=%d\n", ptr.FileID, ptr.PageNumber, ptr.SlotIndex, opLSN)

	globalPageID, err := hf.diskManager.GetGlobalPageID(hf.fileID, int64(ptr.PageNumber))
	if err != nil {
		return fmt.Errorf("failed to resolve page %d: %w", ptr.PageNumber, err)
	}

	pg, err := hf.bufferPool.FetchPage(globalPageID)
	if err != nil {
		return fmt.Errorf("failed to fetch page %d: %w", globalPageID, err)
	}

	pg.Lock()

	updated, err := UpdateRecord(pg, ptr.SlotIndex, newRowData)
	if err != nil {
		pg.Unlock()
		hf.bufferPool.UnpinPage(pg.ID, false)
		return fmt.Errorf("failed to update record: %w", err)
	}

	SetLastAppliedLSN(pg, opLSN)
	pg.Unlock()
	hf.bufferPool.UnpinPage(pg.ID, true)

	if !updated {
		// UpdateRecord already tombstoned the slot — just re-insert on a new page.
		newRP, err := hf.insertRow(newRowData, opLSN)
		if err != nil {
			return fmt.Errorf("failed to insert updated row: %w", err)
		}

		fmt.Printf("[Heap] UPDATE row moved — old page=%d slot=%d new page=%d slot=%d\n",
			ptr.PageNumber, ptr.SlotIndex, newRP.PageNumber, newRP.SlotIndex)

		*ptr = *newRP
	}

	return nil
}
