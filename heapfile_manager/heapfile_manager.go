package heapfile

import (
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
	// fmt.Printf("the file id for the table is: %d\n", fileID)
	filePath := fmt.Sprintf("%s/%s_%d.heap", hfm.baseDir, tableName, fileID)

	// Create OnDiskPager for the heap file
	pager, err := NewHeapFilePager(filePath)
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

func (hfm *HeapFileManager) GetHeapFileByID(fileID uint32) (*HeapFile, error) {
	hfm.mu.RLock()
	hf, exists := hfm.files[fileID]
	hfm.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("heap file %d not found", fileID)
	}

	return hf, nil
}

// CloseAll closes all heap files managed by this manager
func (hfm *HeapFileManager) CloseAll() error {
	hfm.mu.Lock()
	defer hfm.mu.Unlock()

	var lastErr error
	for fileID, heapFile := range hfm.files {
		if err := heapFile.pager.Close(); err != nil {
			fmt.Printf("Error closing heap file %d: %v\n", fileID, err)
			lastErr = err
		}
	}

	return lastErr
}
