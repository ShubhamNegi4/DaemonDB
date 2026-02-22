package diskmanager

import (
	"DaemonDB/storage_engine/page"
	"DaemonDB/types"
	"encoding/binary"
	"fmt"
	"os"
)

/*
This is main file for disk manager
It owns:
File descriptors (os.File)
Reading/writing raw bytes at specific offsets (ReadAt, WriteAt)
Page allocation (tracking NextPageID per file)
The globalPageID ↔ (fileID, localPage) mapping

DiskManager (storage_engine/disk_manager/)
Owns OS file handles and the global page ID space.
Page ID encoding:
globalPageID = int64(fileID) << 32 | localPageNum
This makes global IDs deterministic — no counter needed, same result on every restart regardless of file load order.

Bufferpool on Page hits return the pages, but if page miss occurs then it is disk manager which creates/writes the page at the offset
*/

func NewDiskManager() *DiskManager {
	return &DiskManager{
		files:         make(map[uint32]*FileDescriptor),
		globalPageMap: make(map[int64]uint32),
		localToGlobal: make(map[PageKey]int64),
		nextFileID:    1,
	}
}

func NewPage(pageID int64, fileID uint32, pageType types.PageType) *page.Page {
	return &page.Page{
		ID:       pageID,
		FileID:   fileID,
		Data:     make([]byte, page.PageSize),
		IsDirty:  false,
		PinCount: 0,
		PageType: pageType,
	}
}

/*
Why two OpenFile variants:
OpenFileWithID: Used for Heap files, index files and maintained by CatalogManager (stable across restarts)
OpenFile: Used for WALsegments and maintained by DiskManager counter (session-scoped)
*/
func (dm *DiskManager) OpenFileWithID(filePath string, catalogFileID uint32) (uint32, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Already open — return existing.
	for id, fd := range dm.files {
		if fd.FilePath == filePath {
			return id, nil
		}
	}

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return 0, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return 0, err
	}

	numPages := stat.Size() / int64(page.PageSize)

	fd := &FileDescriptor{
		FileID:     catalogFileID, // ← forced, not dm.nextFileID
		FilePath:   filePath,
		File:       file,
		NextPageID: numPages,
	}

	dm.files[catalogFileID] = fd
	if catalogFileID >= dm.nextFileID {
		dm.nextFileID = catalogFileID + 1
	}

	return catalogFileID, nil
}

// OpenFile opens or creates a file and returns its file ID
func (dm *DiskManager) OpenFile(filePath string) (uint32, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// Check if file is already open
	for id, fd := range dm.files {
		if fd.FilePath == filePath {
			return id, nil
		}
	}

	// Open or create the file
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return 0, fmt.Errorf("failed to open file %s: %w", filePath, err)
	}

	// Get file size to determine existing pages
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return 0, fmt.Errorf("failed to stat file: %w", err)
	}

	fileSize := stat.Size()
	numPages := fileSize / int64(page.PageSize)

	fileID := dm.nextFileID
	dm.nextFileID++

	fmt.Printf("OpenFile: path=%s assigned fileID=%d\n", filePath, fileID)

	fd := &FileDescriptor{
		FileID:     fileID,
		FilePath:   filePath,
		File:       file,
		NextPageID: numPages,
	}

	dm.files[fileID] = fd

	return fileID, nil
}

// ReadPage reads a page from disk
func (dm *DiskManager) ReadPage(globalPageID int64) (*page.Page, error) {
	dm.mu.RLock()
	fileID, exists := dm.globalPageMap[globalPageID]
	dm.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("page %d not found in global page map", globalPageID)
	}

	dm.mu.RLock()
	fd, exists := dm.files[fileID]
	dm.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("file %d not found", fileID)
	}

	fd.mu.RLock()
	defer fd.mu.RUnlock()

	if fd.File == nil {
		return nil, fmt.Errorf("file %d is closed", fileID)
	}

	// Calculate local page offset within the file
	localPageID := dm.getLocalPageID(globalPageID, fileID)
	offset := localPageID * int64(page.PageSize)

	pg := NewPage(globalPageID, fileID, types.PageTypeUnknown)
	n, err := fd.File.ReadAt(pg.Data, offset)
	if err != nil && n == 0 {
		return nil, fmt.Errorf("failed to read page %d from file %d: %w", localPageID, fileID, err)
	}

	// Pad with zeros if partial read
	if n < page.PageSize {
		for i := n; i < page.PageSize; i++ {
			pg.Data[i] = 0
		}
	}

	// Detect page type from data (first byte convention)
	if len(pg.Data) > 8 {
		pg.PageType = types.PageType(pg.Data[8])
	}

	return pg, nil
}

// WritePage writes a page to disk
func (dm *DiskManager) WritePage(pg *page.Page) error {
	dm.mu.RLock()
	fd, exists := dm.files[pg.FileID]
	dm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("file %d not found", pg.FileID)
	}

	fd.mu.Lock()
	defer fd.mu.Unlock()

	if fd.File == nil {
		return fmt.Errorf("file %d is closed", pg.FileID)
	}

	if len(pg.Data) != page.PageSize {
		return fmt.Errorf("page data size %d does not match page size %d", len(pg.Data), page.PageSize)
	}

	// Mark page type in first byte
	pg.Data[8] = byte(pg.PageType)

	// Calculate local page offset within the file
	localPageID := dm.getLocalPageID(pg.ID, pg.FileID)
	offset := localPageID * int64(page.PageSize)

	_, err := fd.File.WriteAt(pg.Data, offset)
	if err != nil {
		return fmt.Errorf("failed to write page %d to file %d: %w", localPageID, pg.FileID, err)
	}

	// Update next page ID if we wrote beyond current end
	if localPageID >= fd.NextPageID {
		fd.NextPageID = localPageID + 1
	}

	pg.IsDirty = false
	return nil
}

// AllocatePage reserves the next available page ID for a file and updates
// internal counters. It does NOT write anything to disk — that is the
// BufferPool's responsibility when it later flushes the dirty page.
func (dm *DiskManager) AllocatePage(fileID uint32, pageType types.PageType) (int64, error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	fd, exists := dm.files[fileID]
	if !exists {
		return 0, fmt.Errorf("file %d not found", fileID)
	}

	fd.mu.Lock()
	defer fd.mu.Unlock()

	if fd.File == nil {
		return 0, fmt.Errorf("file %d is closed", fileID)
	}

	// Allocate global page ID
	localPageNum := fd.NextPageID
	fd.NextPageID++

	globalPageID := int64(fileID)<<32 | localPageNum
	dm.globalPageMap[globalPageID] = fileID
	dm.localToGlobal[PageKey{FileID: fileID, LocalNum: localPageNum}] = globalPageID

	return globalPageID, nil
}

// getLocalPageID converts a global page ID to a local page ID within a file
func (dm *DiskManager) getLocalPageID(globalPageID int64, fileID uint32) int64 {
	return globalPageID & 0xFFFFFFFF
}

func (dm *DiskManager) GetGlobalPageID(fileID uint32, localPageNum int64) (int64, error) {
	return int64(fileID)<<32 | localPageNum, nil
}

func (dm *DiskManager) GetLocalPageID(fileID uint32, globalPageID int64) (int64, error) {
	local := globalPageID & 0xFFFFFFFF
	return local, nil
}

// RegisterPage adds an existing local page into the globalPageMap.
// Called when reopening existing files on database load.
func (dm *DiskManager) RegisterPage(fileID uint32, localPageNum int64) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	key := PageKey{FileID: fileID, LocalNum: localPageNum}
	if _, exists := dm.localToGlobal[key]; exists {
		return nil // already registered
	}

	// Deterministic: no counter, always same result regardless of load order
	globalPageID := int64(fileID)<<32 | localPageNum
	dm.globalPageMap[globalPageID] = fileID
	dm.localToGlobal[key] = globalPageID

	return nil
}

// Sync flushes all file buffers to disk
func (dm *DiskManager) Sync() error {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	for _, fd := range dm.files {
		fd.mu.Lock()
		if fd.File != nil {
			if err := fd.File.Sync(); err != nil {
				fd.mu.Unlock()
				return fmt.Errorf("failed to sync file %d: %w", fd.FileID, err)
			}
		}
		fd.mu.Unlock()
	}

	return nil
}

// Close closes a specific file
func (dm *DiskManager) CloseFile(fileID uint32) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	fd, exists := dm.files[fileID]
	if !exists {
		return fmt.Errorf("file %d not found", fileID)
	}

	fd.mu.Lock()
	defer fd.mu.Unlock()

	if fd.File == nil {
		return nil // Already closed
	}

	if err := fd.File.Sync(); err != nil {
		return fmt.Errorf("failed to sync before close: %w", err)
	}

	if err := fd.File.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}

	fd.File = nil
	delete(dm.files, fileID)

	return nil
}

// CloseAll closes all open files
func (dm *DiskManager) CloseAll() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	var lastErr error
	for fileID, fd := range dm.files {
		fd.mu.Lock()
		if fd.File != nil {
			if err := fd.File.Sync(); err != nil {
				lastErr = err
			}
			if err := fd.File.Close(); err != nil {
				lastErr = err
			}
			fd.File = nil
		}
		fd.mu.Unlock()
		delete(dm.files, fileID)
	}

	return lastErr
}

// GetFileDescriptor returns the file descriptor for a given file ID
func (dm *DiskManager) GetFileDescriptor(fileID uint32) (*FileDescriptor, error) {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	fd, exists := dm.files[fileID]
	if !exists {
		return nil, fmt.Errorf("file %d not found", fileID)
	}

	return fd, nil
}

// TotalPages returns the total number of pages across all files
func (dm *DiskManager) TotalPages() int64 {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	total := int64(0)
	for _, fd := range dm.files {
		total += fd.NextPageID
	}
	return total
}

// WriteMetadata writes metadata to page 0 of a file (e.g., root page ID for B+ tree)
// WriteMetadata writes directly to disk at offset 0, bypassing the buffer pool.
// Metadata pages are always at a fixed location and don't benefit from caching.
func (dm *DiskManager) WriteMetadata(fileID uint32, metadata []byte) error {
	dm.mu.RLock()
	fd, exists := dm.files[fileID]
	dm.mu.RUnlock()

	if !exists {
		return fmt.Errorf("file %d not found", fileID)
	}

	fd.mu.Lock()
	defer fd.mu.Unlock()

	if fd.File == nil {
		return fmt.Errorf("file %d is closed", fileID)
	}

	metaPage := make([]byte, page.PageSize)
	metaPage[8] = byte(types.PageTypeMetadata)
	copy(metaPage[9:], metadata)

	_, err := fd.File.WriteAt(metaPage, 0)
	if err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	return nil
}

// ReadMetadata reads metadata from page 0 of a file
func (dm *DiskManager) ReadMetadata(fileID uint32) ([]byte, error) {
	dm.mu.RLock()
	fd, exists := dm.files[fileID]
	dm.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("file %d not found", fileID)
	}

	fd.mu.RLock()
	defer fd.mu.RUnlock()

	if fd.File == nil {
		return nil, fmt.Errorf("file %d is closed", fileID)
	}

	metaPage := make([]byte, page.PageSize)
	_, err := fd.File.ReadAt(metaPage, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	return metaPage[9:], nil
}

// Helper function to write root ID for B+ tree
func (dm *DiskManager) WriteRootID(fileID uint32, rootID int64) error {
	metadata := make([]byte, 8)
	binary.LittleEndian.PutUint64(metadata, uint64(rootID))
	return dm.WriteMetadata(fileID, metadata)
}

// Helper function to read root ID for B+ tree
func (dm *DiskManager) ReadRootID(fileID uint32) (int64, error) {
	metadata, err := dm.ReadMetadata(fileID)
	if err != nil {
		return 0, err
	}
	if len(metadata) < 8 {
		return 0, fmt.Errorf("invalid metadata size")
	}
	return int64(binary.LittleEndian.Uint64(metadata[:8])), nil
}

func (dm *DiskManager) GetTotalPages(filePath string) (int64, error) {
	info, err := os.Stat(filePath)
	if err != nil {
		return 0, err
	}

	size := info.Size()
	return int64(size / types.PageSize), nil
}
