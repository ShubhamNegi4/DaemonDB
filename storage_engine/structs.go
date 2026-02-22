package storageengine

import (
	heapfile "DaemonDB/storage_engine/access/heapfile_manager"
	indexfile "DaemonDB/storage_engine/access/indexfile_manager"
	bplus "DaemonDB/storage_engine/access/indexfile_manager/bplustree"
	"DaemonDB/storage_engine/bufferpool"
	"DaemonDB/storage_engine/catalog"
	checkpoint "DaemonDB/storage_engine/checkpoint_manager"
	diskmanager "DaemonDB/storage_engine/disk_manager"
	txn "DaemonDB/storage_engine/transaction_manager"
	"DaemonDB/storage_engine/wal_manager"
	"sync"
)

type StorageEngine struct {
	BufferPool *bufferpool.BufferPool

	DiskManager       *diskmanager.DiskManager
	CatalogManager    *catalog.CatalogManager
	IndexManager      *indexfile.IndexFileManager
	HeapManager       *heapfile.HeapFileManager
	WalManager        *wal_manager.WALManager
	TxnManager        *txn.TxnManager
	CheckpointManager *checkpoint.CheckpointManager

	DbRoot          string
	currDb          string
	heapFileCounter uint32

	// Per-table B+ tree index cache (avoids reopening the same .idx file).
	// Cleared and closed when switching DB or on VM shutdown.
	indexCacheMu    sync.RWMutex
	tableIndexCache map[string]*bplus.BPlusTree
}
