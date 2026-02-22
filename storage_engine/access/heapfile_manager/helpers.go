package heapfile

import (
	"DaemonDB/storage_engine/page"
	"DaemonDB/types"
	"fmt"
)

/*
This file contains helpers related to HeapFileManager and Heapfile
*/

func (hfm *HeapFileManager) UpdateBaseDir(dir string) {
	hfm.mu.Lock()
	defer hfm.mu.Unlock()
	hfm.baseDir = dir
}

func (hfm *HeapFileManager) GetHeapFileByTable(tableName string) (*HeapFile, error) {
	hfm.mu.Lock()
	defer hfm.mu.Unlock()

	fileID, exists := hfm.tableIndex[tableName]
	if !exists {
		return nil, fmt.Errorf("no heap file open for table '%s'", tableName)
	}

	hf, exists := hfm.files[fileID]
	if !exists {
		return nil, fmt.Errorf("heap file index inconsistency for table '%s'", tableName)
	}

	return hf, nil
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

func (hfm *HeapFileManager) GetPageLSN(fileID uint32, pageNumber uint32) (uint64, error) {
	hfm.mu.RLock()
	hf, exists := hfm.files[fileID]
	hfm.mu.RUnlock()
	if !exists {
		return 0, fmt.Errorf("heap file %d not found", fileID)
	}

	globalPageID, err := hf.diskManager.GetGlobalPageID(fileID, int64(pageNumber))
	if err != nil {
		return 0, err
	}

	pg, err := hf.bufferPool.FetchPage(globalPageID)
	if err != nil {
		return 0, err
	}
	defer hf.bufferPool.UnpinPage(globalPageID, false)

	return GetLastAppliedLSN(pg), nil
}

/*


Helpers related to HeapFile


*/
// findSuitablePage finds a page with enough space for the required row size
func (hf *HeapFile) findSuitablePage(requiredSpace uint16) (*page.Page, uint32, error) {
	requiredWithSlot := int(requiredSpace) + types.SlotSize

	fd, err := hf.diskManager.GetFileDescriptor(hf.fileID)
	if err != nil {
		return nil, 0, err
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

		if FreeSpace(pg) >= requiredWithSlot {
			return pg, uint32(localPageNum), nil // ← return local
		}

		hf.bufferPool.UnpinPage(globalPageID, false)
	}

	// Allocate new page.
	pg, err := hf.bufferPool.NewPage(hf.fileID, types.PageTypeHeapData)
	if err != nil {
		return nil, 0, err
	}

	InitHeapPage(pg) // ← always initialize new pages

	fd, err = hf.diskManager.GetFileDescriptor(hf.fileID)
	if err != nil {
		hf.bufferPool.UnpinPage(pg.ID, false)
		return nil, 0, err
	}

	// Register the new page in localToGlobal.
	localPageNum := uint32(fd.NextPageID - 1)
	SetPageNo(pg, localPageNum)
	if err := hf.diskManager.RegisterPage(hf.fileID, int64(localPageNum)); err != nil {
		hf.bufferPool.UnpinPage(pg.ID, false)
		return nil, 0, fmt.Errorf("failed to register new page: %w", err)
	}

	return pg, localPageNum, nil
}

// Flush flushes all dirty pages for this heap file
func (hf *HeapFile) Flush() error {
	return hf.bufferPool.FlushAllPages()
}

func getFileIDs(files map[uint32]*HeapFile) []uint32 {
	keys := make([]uint32, 0, len(files))
	for k := range files {
		keys = append(keys, k)
	}
	return keys
}
