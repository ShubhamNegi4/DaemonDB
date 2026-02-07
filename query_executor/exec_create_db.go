package executor

import (
	"fmt"
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
	tablesDir := filepath.Join(dbDir, "tables")

	if err := os.MkdirAll(tablesDir, 0755); err != nil {
		return err
	}

	vm.currDb = name
	vm.heapfileManager.UpdateBaseDir(tablesDir)

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

	return nil
}
