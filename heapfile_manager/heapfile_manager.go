package heapfile

import (
	bplus "DaemonDB/bplustree"
	"fmt"
)

// NewHeapFileManager creates a new heap file manager
func NewHeapFileManager(baseDir string) (*HeapFileManager, error) {
	return &HeapFileManager{
		baseDir: baseDir,
		files:   make(map[uint32]*HeapFile),
	}, nil
}

// CreateHeapfile creates a fresh heap file for a new table or when a heap file is filled
func (hfm *HeapFileManager) CreateHeapfile(tableName string, fileID uint32) error {
	hfm.mu.Lock()
	defer hfm.mu.Unlock()
	fmt.Print("the file id for the table is: ", fileID)
	filePath := fmt.Sprintf("%s/%s_%d.heap", hfm.baseDir, tableName, fileID)

	// Create OnDiskPager for the heap file
	pager, err := bplus.NewOnDiskPager(filePath)
	if err != nil {
		return fmt.Errorf("failed to create pager for heap file: %w", err)
	}

	heapFile := &HeapFile{
		fileID:   fileID,
		pager:    pager,
		filePath: filePath,
	}

	// Initialize first page
	if err := heapFile.initializePage(0); err != nil {
		pager.Close()
		return err
	}

	hfm.files[fileID] = heapFile
	return nil
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

// GetRow retrieves a row from the heap file using a RowPointer
func (hfm *HeapFileManager) GetRow(ptr *RowPointer) ([]byte, error) {
	hfm.mu.RLock()
	heapFile, exists := hfm.files[ptr.FileID]
	hfm.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("heap file %d not found", ptr.FileID)
	}

	return heapFile.GetRow(ptr)
}
