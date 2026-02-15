package heapfile

import "fmt"

// this file contains external functions, they will lock the row before calling there internal function
// it is to be ensured that the internal functions of these should not contain locks, otherwise two or more dependent function (like UPDATE calling both INSERT and DELETE) will get into Deadlock

// InsertRow inserts a row into the specified heap file (delegates to HeapFile.insertRow).
func (hfm *HeapFileManager) InsertRow(fileID uint32, rowData []byte, opLSN uint64) (*RowPointer, error) {
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

// GetRow retrieves a row from the heap file using a RowPointer.
func (hfm *HeapFileManager) GetRow(rp *RowPointer) ([]byte, error) {
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

// UpdateRow updates an existing row in the heap file
// It replaces the row data at the given RowPointer with new data
func (hfm *HeapFileManager) UpdateRow(rp *RowPointer, newRowData []byte, opLSN uint64) error {

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

// DeleteRow tombstones a row using its RowPointer.
// After this, GetRow(ptr) should return "invalid slot".
func (hfm *HeapFileManager) DeleteRow(ptr *RowPointer, opLSN uint64) error {
	hfm.mu.RLock()
	heapFile, exists := hfm.files[ptr.FileID]
	hfm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("heap file %d not found", ptr.FileID)
	}

	heapFile.mu.Lock()
	defer heapFile.mu.Unlock()

	return heapFile.deleteRow(ptr, opLSN)
}
