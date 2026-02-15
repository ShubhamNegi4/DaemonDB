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
	hfm.mu.Lock()
	defer hfm.mu.Unlock()

	if hf, exists := hfm.files[fileID]; exists {
		return hf, nil
	}

	filePath := filepath.Join(hfm.baseDir, fmt.Sprintf("%s_%d.heap", tableName, fileID))

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
	return hf, nil
}
