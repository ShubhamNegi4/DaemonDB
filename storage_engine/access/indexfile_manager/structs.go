package indexfile

import (
	bplus "DaemonDB/storage_engine/access/indexfile_manager/bplustree"
	"DaemonDB/storage_engine/bufferpool"
	diskmanager "DaemonDB/storage_engine/disk_manager"
	"sync"
)

type IndexFileManager struct {
	baseDir     string                      // e.g., /data/mydb/indexes
	indexes     map[string]*bplus.BPlusTree // tableName → cached B+ tree
	bufferPool  *bufferpool.BufferPool      // ← shared with heap files
	diskManager *diskmanager.DiskManager    // ← shared with heap files
	mu          sync.RWMutex
}
