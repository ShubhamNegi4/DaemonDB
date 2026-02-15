package executor

import (
	heapfile "DaemonDB/heapfile_manager"
	"DaemonDB/wal_manager"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

func (vm *VM) ExecuteCreateDatabase(dbName string) error {
	if dbName == "" {
		return fmt.Errorf("database name cannot be empty")
	}
	if err := os.MkdirAll(DB_ROOT, 0755); err != nil {
		return fmt.Errorf("failed to create DB directory: %w", err)
	}

	dbPath := filepath.Join(DB_ROOT, dbName)

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

	fmt.Printf("Created database directory: %s\n", dbPath)
	return nil
}

func (vm *VM) ExecuteShowDatabases() ([]string, error) {
	entries, err := os.ReadDir(DB_ROOT)
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

func (vm *VM) ExecuteUseDatabase(name string) error {
	if name == "" {
		return fmt.Errorf("database name cannot be empty")
	}

	// Close all open index files for the current DB before switching (avoids handle leak).
	vm.closeOpenIndexes()

	dbDir := filepath.Join(DB_ROOT, name)

	// Check if database exists
	if _, err := os.Stat(dbDir); os.IsNotExist(err) {
		return fmt.Errorf("database '%s' does not exist. Use CREATE DATABASE first", name)
	}

	// Close existing resources if switching databases
	if vm.currDb != "" {
		vm.closeOpenIndexes()
		if vm.WalManager != nil {
			vm.WalManager.Close()
		}
	}

	// Initialize WAL Manager for this database
	logsPath := filepath.Join(dbDir, "logs")
	if err := os.MkdirAll(logsPath, 0755); err != nil {
		return fmt.Errorf("failed to create logs directory: %w", err)
	}

	walManager, err := wal_manager.OpenWAL(logsPath)
	if err != nil {
		return fmt.Errorf("failed to open WAL: %w", err)
	}
	vm.WalManager = walManager

	// Initialize Heap File Manager for this database
	tablesDir := filepath.Join(dbDir, "tables")

	if err := os.MkdirAll(tablesDir, 0755); err != nil {
		return err
	}

	heapfileManager, err := heapfile.NewHeapFileManager(tablesDir)
	if err != nil {
		walManager.Close()
		return fmt.Errorf("failed to create heapfile manager: %w", err)
	}

	vm.heapfileManager = heapfileManager

	vm.currDb = name
	vm.heapfileManager.UpdateBaseDir(tablesDir)

	vm.CheckpointManager = NewCheckpointManager(dbDir)

	if err := vm.LoadTableFileMapping(); err != nil {
		return err
	}
	if err := vm.LoadAllTableSchemas(); err != nil {
		return err
	}

	for tableName, fileID := range vm.tableToFileId {
		if _, err := vm.heapfileManager.LoadHeapFile(fileID, tableName); err != nil {
			return fmt.Errorf("failed to load heapfile for %s: %w", tableName, err)
		}
	}

	if err := vm.RecoverAndReplayFromWAL(); err != nil {
		log.Printf("Warning: WAL recovery failed: %v", err)
	}

	fmt.Printf("Switched to database: %s\n", name)
	return nil
}

func (vm *VM) RequireDatabase() error {
	if vm.currDb == "" || vm.heapfileManager == nil || vm.WalManager == nil {
		return fmt.Errorf("no database selected. Use 'USE <database>' command first")
	}
	return nil
}
