package heapfile

import (
	"fmt"
	"os"
)

// NewHeapFileManager creates a new heap file manager
func NewHeapFileManager(baseDir string) (*HeapFileManager, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, err
	}
	return &HeapFileManager{
		baseDir: baseDir,
		files:   make(map[uint32]*HeapFile),
	}, nil
}

// CreateHeapfile creates a fresh heap file for a new table or when a heap file is filled
func (hfm *HeapFileManager) CreateHeapfile(tableName string, fileID uint32) error {
	hfm.mu.Lock()
	defer hfm.mu.Unlock()

	filePath := fmt.Sprintf("%s/%s_%d.heap", hfm.baseDir, tableName, fileID)
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("failed to create heap file: %w", err)
	}

	heapFile := &HeapFile{
		fileID:   fileID,
		file:     file,
		filePath: filePath,
	}

	// Initialize first page
	if err := heapFile.initializePage(0); err != nil {
		file.Close()
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
