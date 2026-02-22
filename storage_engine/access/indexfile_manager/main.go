package indexfile

import (
	bplus "DaemonDB/storage_engine/access/indexfile_manager/bplustree"
	"DaemonDB/storage_engine/bufferpool"
	diskmanager "DaemonDB/storage_engine/disk_manager"
	"fmt"
	"os"
	"path/filepath"
)

/*
This file is the main file for Index File Manager that deals with the Index pages
Similar to HeapFileManager this also have access to disk manager and buffer pool

It has a B+ Tree which helps in indexing for the nodes
a new node creates a new page thats sole purpose is to hold the child, parent, next nodes
*/

func NewIndexFileManager(baseDir string, diskManager *diskmanager.DiskManager, bufferPool *bufferpool.BufferPool) (*IndexFileManager, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create indexes directory: %w", err)
	}

	return &IndexFileManager{
		baseDir:     baseDir,
		indexes:     make(map[string]*bplus.BPlusTree),
		bufferPool:  bufferPool,
		diskManager: diskManager,
	}, nil
}

// GetOrCreateIndex returns the B+ tree primary index for the given table.
// It is used to: map primary key → row pointer (file, page, slot) for point lookups,
// INSERT (add new key→rowPtr), SELECT WHERE pk=... (lookup), and ROLLBACK (remove key).
// Indexes are cached per table; cache is cleared and file handles closed on USE <other_db> or VM shutdown.
func (ifm *IndexFileManager) GetOrCreateIndex(tableName string, indexFileID uint32) (*bplus.BPlusTree, error) {

	ifm.mu.RLock()
	btree, exists := ifm.indexes[tableName]
	ifm.mu.RUnlock()

	if exists && btree != nil {
		return btree, nil
	}

	// Slow path: open or create the index file.
	ifm.mu.Lock()
	defer ifm.mu.Unlock()

	// Double-check after acquiring write lock (another goroutine may have
	// opened it while we were waiting for the lock).
	if btree, exists := ifm.indexes[tableName]; exists && btree != nil {
		return btree, nil
	}

	// Build the index file path: indexes/tableName_primary.idx
	indexKey := fmt.Sprintf("%s_primary", tableName)
	indexPath := filepath.Join(ifm.baseDir, indexKey+".idx")

	// OpenBPlusTree creates the file if it doesn't exist.
	btree, err := bplus.OpenBPlusTree(indexPath, indexFileID, ifm.bufferPool, ifm.diskManager)
	if err != nil {
		return nil, fmt.Errorf("failed to open B+ tree for table '%s': %w", tableName, err)
	}

	// Cache it so subsequent calls are O(1).
	ifm.indexes[tableName] = btree
	return btree, nil
}

// CloseIndex closes the B+ tree for a specific table and removes it from cache.
// The index is flushed to disk before closing.
func (ifm *IndexFileManager) CloseIndex(tableName string) error {
	ifm.mu.Lock()
	defer ifm.mu.Unlock()

	btree, exists := ifm.indexes[tableName]
	if !exists {
		return nil // not open, nothing to do
	}

	if err := btree.Close(); err != nil {
		return fmt.Errorf("failed to close index for table '%s': %w", tableName, err)
	}

	delete(ifm.indexes, tableName)
	return nil
}

// CloseAll closes all cached indexes and clears the cache.
// Called when switching databases or shutting down the storage engine.
func (ifm *IndexFileManager) CloseAll() error {
	ifm.mu.Lock()
	defer ifm.mu.Unlock()

	var lastErr error
	for tableName, btree := range ifm.indexes {
		if err := btree.Close(); err != nil {
			lastErr = fmt.Errorf("failed to close index for table '%s': %w", tableName, err)
		}
		delete(ifm.indexes, tableName)
	}

	return lastErr
}

// LoadIndex opens an existing index file and caches it.
// Used during database initialization to preload all indexes for open tables.
func (ifm *IndexFileManager) LoadIndex(tableName string, IndexFileID uint32) error {
	ifm.mu.Lock()
	defer ifm.mu.Unlock()

	// Already cached — nothing to do.
	if _, exists := ifm.indexes[tableName]; exists {
		return nil
	}

	indexKey := fmt.Sprintf("%s_primary", tableName)
	indexPath := filepath.Join(ifm.baseDir, indexKey+".idx")

	// Verify the file exists before opening.
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		return fmt.Errorf("index file for table '%s' not found at %s", tableName, indexPath)
	}

	btree, err := bplus.OpenBPlusTree(indexPath, IndexFileID, ifm.bufferPool, ifm.diskManager)
	if err != nil {
		return fmt.Errorf("failed to load index for table '%s': %w", tableName, err)
	}

	ifm.indexes[tableName] = btree
	return nil
}
