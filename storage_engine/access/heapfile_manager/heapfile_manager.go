package heapfile

import (
	"DaemonDB/storage_engine/bufferpool"
	diskmanager "DaemonDB/storage_engine/disk_manager"
	"DaemonDB/types"
	"fmt"
	"os"
	"path/filepath"
)

/*
This file is the start of the heapfile manager
This is responsible for creation of heapfile, which is ultimately initialization of heap pages

Heapfile manager knows Disk Manager fpr file related operations like OpenFileWithID, CloseFile
and it also knows the Buffer Pool to Add the created/accessed pages to the cache
*/

// NewHeapFileManager creates a new heap file manager
func NewHeapFileManager(baseDir string, diskManager *diskmanager.DiskManager, bufferPool *bufferpool.BufferPool) (*HeapFileManager, error) {
	return &HeapFileManager{
		baseDir:     baseDir,
		files:       make(map[uint32]*HeapFile),
		tableIndex:  make(map[string]uint32),
		diskManager: diskManager,
		bufferPool:  bufferPool,
	}, nil
}

// Chain of command this function drives:
//  1. DiskManager.OpenFile  → creates the OS file, returns a fileID
//  2. BufferPool.NewPage    → allocates a page ID (RAM only, dirty)
//  3. page.InitHeapPage     → writes header fields into the in-RAM buffer
//  4. BufferPool.UnpinPage  → caller is done; pool may flush when it needs space
//  5. (later) BufferPool flush → DiskManager.WritePage → bytes hit disk
func (hfm *HeapFileManager) CreateHeapfile(tableName string, fileID int) error {

	hfm.mu.Lock()
	defer hfm.mu.Unlock()

	// Guard: refuse to create a duplicate entry for the same table.
	if _, exists := hfm.tableIndex[tableName]; exists {
		return fmt.Errorf("heap file for table '%s' already open", tableName)
	}

	catalogFileID := uint32(fileID)

	heapPath := filepath.Join(hfm.baseDir, fmt.Sprintf("%d.heap", catalogFileID))

	if _, err := os.Stat(heapPath); err == nil {
		return fmt.Errorf("heapfile %d already exists", catalogFileID)
	}

	if err := os.MkdirAll(hfm.baseDir, 0755); err != nil {
		return fmt.Errorf("failed to create heap directory: %w", err)
	}

	// Use DiskManager to create file properly
	_, err := hfm.diskManager.OpenFileWithID(heapPath, catalogFileID)
	if err != nil {
		return fmt.Errorf("failed to create heapfile: %w", err)
	}

	pg, err := hfm.bufferPool.NewPage(catalogFileID, types.PageTypeHeapData)
	if err != nil {
		_ = hfm.diskManager.CloseFile(catalogFileID)
		return fmt.Errorf("buffer pool failed to allocate first page: %w", err)
	}

	// Initialize the first heap page (header, free pointer, etc.)
	InitHeapPage(pg) // sets FreePtr=0, NumRecords=0, etc.

	// Unpin the page.  We are done setting it up; the BufferPool is free to
	// flush it to disk whenever it needs the frame for something else.
	// (PinCount goes from 1 → 0; the dirty flag ensures it will be written.)
	if err := hfm.bufferPool.UnpinPage(pg.ID, true /* isDirty */); err != nil {
		_ = hfm.diskManager.CloseFile(catalogFileID)
		return fmt.Errorf("failed to unpin first heap page: %w", err)
	}

	// Register the HeapFile object for future operations
	hf := &HeapFile{
		fileID:      catalogFileID,
		tableName:   tableName,
		filePath:    heapPath,
		diskManager: hfm.diskManager,
		bufferPool:  hfm.bufferPool,
	}

	hfm.files[catalogFileID] = hf             // runtime lookup: catalogFileID → HeapFile
	hfm.tableIndex[tableName] = catalogFileID // name lookup: tableName → catalogFileID

	return nil
}

func (hfm *HeapFileManager) LoadHeapFile(catalogFileID uint32, tableName string) (*HeapFile, error) {
	hfm.mu.Lock()
	defer hfm.mu.Unlock()

	fmt.Printf("[LoadHeapFile] table=%s catalogFileID=%d\n", tableName, catalogFileID)

	// Already loaded.
	if hf, exists := hfm.files[catalogFileID]; exists {
		fmt.Printf("[LoadHeapFile] already loaded: table=%s fileID=%d\n", tableName, catalogFileID)
		return hf, nil
	}

	heapPath := filepath.Join(hfm.baseDir, fmt.Sprintf("%d.heap", catalogFileID))

	if _, err := os.Stat(heapPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("heap file %d not found on disk", catalogFileID)
	}

	// OpenFile registers the file with DiskManager and sets NextPageID from file size.
	_, err := hfm.diskManager.OpenFileWithID(heapPath, catalogFileID)
	if err != nil {
		return nil, fmt.Errorf("failed to open heap file: %w", err)
	}

	// ... OpenFileWithID ...
	fmt.Printf("[LoadHeapFile] registered: table=%s fileID=%d heapPath=%s\n", tableName, catalogFileID, heapPath)

	// Re-register all existing pages into globalPageMap.
	fd, err := hfm.diskManager.GetFileDescriptor(catalogFileID)
	if err != nil {
		return nil, err
	}

	for localPage := int64(0); localPage < fd.NextPageID; localPage++ {
		if err := hfm.diskManager.RegisterPage(catalogFileID, localPage); err != nil {
			return nil, fmt.Errorf("failed to register page %d: %w", localPage, err)
		}
	}

	// ... after registering pages ...
	fmt.Printf("[LoadHeapFile] pages registered: fileID=%d totalPages=%d\n", catalogFileID, fd.NextPageID)

	hf := &HeapFile{
		fileID:      catalogFileID,
		tableName:   tableName,
		filePath:    heapPath,
		diskManager: hfm.diskManager,
		bufferPool:  hfm.bufferPool,
	}

	hfm.files[catalogFileID] = hf
	hfm.tableIndex[tableName] = catalogFileID

	fmt.Printf("[LoadHeapFile] tableIndex=%+v\n", hfm.tableIndex)
	fmt.Printf("[LoadHeapFile] files keys=%v\n", getFileIDs(hfm.files))

	return hf, nil
}
