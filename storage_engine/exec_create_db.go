package storageengine

import (
	heapfile "DaemonDB/storage_engine/access/heapfile_manager"
	indexfile "DaemonDB/storage_engine/access/indexfile_manager"
	"DaemonDB/storage_engine/bufferpool"
	checkpoint "DaemonDB/storage_engine/checkpoint_manager"
	diskmanager "DaemonDB/storage_engine/disk_manager"
	txn "DaemonDB/storage_engine/transaction_manager"
	"DaemonDB/storage_engine/wal_manager"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

/*
This file contains Database related commands
Create Database is just a simple directory initialization process
Use Database is where storage engine (& catalog manager), disk manager, bufferpool,
heapfile manager, indexfile manager, transaction manager, checkpoint manager, wal manager all are initialized

It is the Use command that loads table to file mapping, loads table schema from disk
It also starts the WAL recovery based on the LSN checkpoint that was succesfully saved in checkpoint.json

*/

func (se *StorageEngine) CreateDatabase(dbName string) error {
	if dbName == "" {
		return fmt.Errorf("database name cannot be empty")
	}
	if err := os.MkdirAll(se.DbRoot, 0755); err != nil {
		return fmt.Errorf("failed to create DB directory: %w", err)
	}

	dbPath := filepath.Join(se.DbRoot, dbName)

	if _, err := os.Stat(dbPath); err == nil {
		return fmt.Errorf("database %s already exists", dbName)
	}
	if err := os.Mkdir(dbPath, 0755); err != nil {
		return fmt.Errorf("failed to create database %s: %w", dbName, err)
	}

	// logs directory for WAL
	logsPath := filepath.Join(dbPath, "logs")
	if err := os.MkdirAll(logsPath, 0755); err != nil {
		return fmt.Errorf("failed to create logs directory: %w", err)
	}

	// tables folder
	if err := os.MkdirAll(filepath.Join(dbPath, "tables"), 0755); err != nil {
		return err
	}

	fmt.Printf("Created database directory: %s\n", dbPath)
	return nil
}

func (se *StorageEngine) ExecuteShowDatabases() ([]string, error) {
	entries, err := os.ReadDir(se.DbRoot)
	if err != nil {
		return nil, fmt.Errorf("failed to read DB root directory: %w", err)
	}

	var databases []string
	for _, entry := range entries {
		if entry.IsDir() {
			databases = append(databases, entry.Name())
		}
	}
	return databases, nil
}

func (se *StorageEngine) UseDatabase(name string) error {

	if name == "" {
		return fmt.Errorf("database name cannot be empty")
	}

	fmt.Printf("[DB] Switching to database: %s\n", name)

	dbDir := filepath.Join(se.DbRoot, name)

	if _, err := os.Stat(dbDir); os.IsNotExist(err) {
		return fmt.Errorf("database '%s' does not exist", name)
	}

	se.closeCurrentDatabase()

	// Initialize DiskManager
	diskManager := diskmanager.NewDiskManager()

	// Initialize BufferPool
	bufferPool := bufferpool.NewBufferPool(100, diskManager)

	// Initialize HeapFileManager
	/*
		BufferPool already holds a reference to DiskManager internally
		but we still pass diskManager seperately to the heap and index file manager
		because it is disk_manager that allocates page, open files, closes file and get file descriptors
	*/
	tablesDir := filepath.Join(dbDir, "tables")
	heapManager, err := heapfile.NewHeapFileManager(tablesDir, diskManager, bufferPool)
	if err != nil {
		return err
	}

	// Initialize IndexFileManager
	indexDir := filepath.Join(dbDir, "indexes")
	indexManager, err := indexfile.NewIndexFileManager(indexDir, diskManager, bufferPool)
	if err != nil {
		return fmt.Errorf("failed to init index manager: %w", err)
	}

	// Open WAL
	logDir := filepath.Join(dbDir, "logs")
	walManager, err := wal_manager.OpenWAL(logDir)
	if err != nil {
		return err
	}

	// Initialize TransactionManager
	txnManager, err := txn.NewTxnManager()
	if err != nil {
		return err
	}

	// Initialize CheckPointManager
	checkpointManager, err := checkpoint.NewCheckpointManager(dbDir)
	if err != nil {
		return err
	}

	se.DiskManager = diskManager
	se.BufferPool = bufferPool
	se.HeapManager = heapManager
	se.IndexManager = indexManager
	se.WalManager = walManager
	se.TxnManager = txnManager
	se.CheckpointManager = checkpointManager
	se.currDb = name

	// load catalog metadata
	se.CatalogManager.SetCurrentDatabase(name)

	se.BufferPool.SetWALManager(se.WalManager)

	fmt.Printf("[DB] DiskManager initialized\n")
	fmt.Printf("[DB] BufferPool initialized capacity=%d\n", 100)
	fmt.Printf("[DB] HeapManager initialized dir=%s\n", tablesDir)
	fmt.Printf("[DB] IndexManager initialized dir=%s\n", indexDir)
	fmt.Printf("[DB] WALManager initialized dir=%s\n", logDir)
	fmt.Printf("[DB] WAL wired to BufferPool\n")

	if err := se.CatalogManager.LoadTableFileMapping(); err != nil {
		return fmt.Errorf("failed to load table mappings: %w", err)
	}
	if err := se.CatalogManager.LoadAllTableSchemas(); err != nil {
		return err
	}

	fmt.Printf("[DB] CatalogManager loaded table schemas and table to file mapping\n")

	for tableName, mapping := range se.CatalogManager.GetAllTableMappings() {
		if _, err := se.HeapManager.LoadHeapFile(mapping.HeapFileID, tableName); err != nil {
			return fmt.Errorf("failed to load heapfile for %s: %w", tableName, err)
		}
		if _, err := se.IndexManager.GetOrCreateIndex(tableName, mapping.IndexFileID); err != nil {
			return fmt.Errorf("failed to load index for %s: %w", tableName, err)
		}
	}

	if err := se.RecoverFromWAL(); err != nil {
		log.Printf("Warning: recovery failed: %v", err)
	}

	fmt.Printf("Switched to database: %s\n", name)
	return nil

}

func (se *StorageEngine) closeCurrentDatabase() {

	if se.WalManager != nil {
		se.WalManager.Sync() // ensure WAL is durable first
		se.WalManager.Close()
	}

	if se.BufferPool != nil {
		se.BufferPool.FlushAllPages() // now safe to flush
	}

	if se.DiskManager != nil {
		se.DiskManager.CloseAll()
	}

	se.DiskManager = nil
	se.BufferPool = nil
	se.HeapManager = nil
	se.WalManager = nil
	se.currDb = ""
}
