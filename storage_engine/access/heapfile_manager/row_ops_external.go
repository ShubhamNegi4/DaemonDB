package heapfile

import (
	"DaemonDB/types"
	"fmt"
)

/* this file contains external functions for row operations on the heapfile, they will lock the row before calling there internal function
it is to be ensured that the internal functions of these should not contain locks,
otherwise two or more dependent function (like UPDATE calling both INSERT and DELETE) will get into Deadlock
*/

// InsertRow inserts a row into the specified heap file (delegates to HeapFile.insertRow).
func (hfm *HeapFileManager) InsertRow(fileID uint32, rowData []byte, opLSN uint64) (*types.RowPointer, error) {
	hfm.mu.RLock()
	heapFile, exists := hfm.files[fileID]
	hfm.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("heap file %d not found", fileID)
	}

	heapFile.mu.Lock()
	defer heapFile.mu.Unlock()

	return heapFile.insertRow(rowData, opLSN)
}

func (hfm *HeapFileManager) InsertRowAtPointer(fileID uint32, rp *types.RowPointer, rowData []byte, lsn uint64) error {
	hfm.mu.RLock()
	hf, exists := hfm.files[fileID]
	hfm.mu.RUnlock()
	if !exists {
		return fmt.Errorf("heap file %d not found", fileID)
	}

	globalPageID, err := hf.diskManager.GetGlobalPageID(fileID, int64(rp.PageNumber))
	if err != nil {
		return fmt.Errorf("failed to resolve page %d: %w", rp.PageNumber, err)
	}

	pg, err := hf.bufferPool.FetchPage(globalPageID)
	if err != nil {
		return fmt.Errorf("failed to fetch page: %w", err)
	}

	pg.Lock()

	// Write directly to the specific slot
	if err := InsertRecordAtSlot(pg, rp.SlotIndex, rowData); err != nil {
		pg.Unlock()
		hf.bufferPool.UnpinPage(pg.ID, false)
		return fmt.Errorf("failed to insert at slot %d: %w", rp.SlotIndex, err)
	}

	SetLastAppliedLSN(pg, lsn)
	pg.Unlock()
	hf.bufferPool.UnpinPage(pg.ID, true)
	return nil
}

// // GetRow retrieves a row from the heap file using a RowPointer.
func (hfm *HeapFileManager) GetRow(rp *types.RowPointer) ([]byte, error) {
	if rp == nil {
		return nil, fmt.Errorf("row pointer is nil")
	}

	// Lock manager to get the heap file
	hfm.mu.RLock()
	heapFile, exists := hfm.files[rp.FileID]
	hfm.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("heap file not found")
	}

	// Lock the heap file for reading before calling its method
	heapFile.mu.RLock()
	defer heapFile.mu.RUnlock()

	return heapFile.getRow(rp)
}

// // UpdateRow updates an existing row in the heap file
// // It replaces the row data at the given RowPointer with new data
func (hfm *HeapFileManager) UpdateRow(rp *types.RowPointer, newRowData []byte, opLSN uint64) error {

	if rp == nil {
		return fmt.Errorf("row pointer is nil")
	}

	hfm.mu.RLock()
	heapFile, exists := hfm.files[rp.FileID]
	hfm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("heap file not found")
	}

	heapFile.mu.Lock()
	defer heapFile.mu.Unlock()

	return heapFile.updateRow(rp, newRowData, opLSN)
}

// // DeleteRow tombstones a row using its RowPointer.
// // After this, GetRow(ptr) should return "invalid slot".
func (hfm *HeapFileManager) DeleteRow(rp *types.RowPointer, opLSN uint64) error {

	if rp == nil {
		return fmt.Errorf("row pointer is nil")
	}
	hfm.mu.RLock()
	heapFile, exists := hfm.files[rp.FileID]
	hfm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("heap file %d not found", rp.FileID)
	}

	heapFile.mu.Lock()
	defer heapFile.mu.Unlock()

	return heapFile.deleteRow(rp, opLSN)
}
