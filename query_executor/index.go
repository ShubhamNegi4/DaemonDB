package executor

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	bplus "DaemonDB/bplustree"
	heapfile "DaemonDB/heapfile_manager"
	"DaemonDB/types"
)

func (vm *VM) ExtractPrimaryKey(schema types.TableSchema, values []any, rowPtr *heapfile.RowPointer) ([]byte, string, error) {
	for i, col := range schema.Columns {
		if col.IsPrimaryKey {
			keyBytes, err := ValueToBytes(values[i], col.Type)
			if err != nil {
				return nil, "", err
			}
			return keyBytes, col.Name, nil
		}
	}

	return vm.GenerateImplicitKey(rowPtr), "__rowid__", nil
}

func (vm *VM) GenerateImplicitKey(rowPtr *heapfile.RowPointer) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[0:4], rowPtr.FileID)
	binary.BigEndian.PutUint32(buf[4:8], rowPtr.PageNumber)
	return buf
}

// GetOrCreateIndex returns the B+ tree primary index for the given table.
// It is used to: map primary key → row pointer (file, page, slot) for point lookups,
// INSERT (add new key→rowPtr), SELECT WHERE pk=... (lookup), and ROLLBACK (remove key).
// Indexes are cached per table; cache is cleared and file handles closed on USE <other_db> or VM shutdown.
func (vm *VM) GetOrCreateIndex(tableName string) (*bplus.BPlusTree, error) {
	if vm.currDb == "" {
		return nil, fmt.Errorf("no database selected")
	}

	if _, exists := vm.tableToFileId[tableName]; !exists {
		return nil, fmt.Errorf("table '%s' does not exist", tableName)
	}

	vm.indexCacheMu.RLock()
	btree, ok := vm.tableIndexCache[tableName]
	vm.indexCacheMu.RUnlock()

	if ok && btree != nil {
		return btree, nil
	}

	vm.indexCacheMu.Lock()
	defer vm.indexCacheMu.Unlock()

	// Double-check after acquiring write lock
	if btree, ok := vm.tableIndexCache[tableName]; ok && btree != nil {
		return btree, nil
	}

	indexKey := fmt.Sprintf("%s_primary", tableName)
	indexDir := filepath.Join(DB_ROOT, vm.currDb, "indexes")
	if err := os.MkdirAll(indexDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create indexes directory: %w", err)
	}

	indexPath := filepath.Join(indexDir, indexKey+".idx")

	btree, err := OpenBPlusTree(indexPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open B+ tree: %w", err)
	}

	vm.tableIndexCache[tableName] = btree
	return btree, nil
}

// closeOpenIndexes flushes and closes all open B+ tree index files for the current DB.
// Call on USE <other_db> or before VM shutdown to avoid leaking file descriptors.
func (vm *VM) closeOpenIndexes() {
	vm.indexCacheMu.Lock()
	defer vm.indexCacheMu.Unlock()

	for tableName, btree := range vm.tableIndexCache {
		if btree != nil {
			_ = btree.Close()
		}
		delete(vm.tableIndexCache, tableName)
	}
}

// CloseIndexCache is the public name for closing all open indexes (e.g. from main on shutdown).
func (vm *VM) CloseIndexCache() {
	vm.closeOpenIndexes()
}

// OpenBPlusTree opens or creates a B+ tree index file.
func OpenBPlusTree(indexPath string) (*bplus.BPlusTree, error) {
	pager, err := bplus.NewOnDiskPager(indexPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create pager: %w", err)
	}

	cache := bplus.NewBufferPool(10)
	btree := bplus.NewBPlusTree(pager, cache, bytes.Compare)

	return btree, nil
}
