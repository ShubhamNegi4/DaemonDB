package executor

import "fmt"

/*
ExecTruncate handles TRUNCATE TABLE execution.
Delegates to the storage engine.
*/

func (vm *VM) ExecTruncate(tableName string) error {
	if vm.storageEngine == nil {
		return fmt.Errorf("storage engine not initialized")
	}

	if err := vm.storageEngine.RequireDatabase(); err != nil {
		return fmt.Errorf("no database selected. Run: USE <dbname>")
	}

	if tableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	if !vm.storageEngine.CatalogManager.TableExists(tableName) {
		return fmt.Errorf("table '%s' does not exist", tableName)
	}

	fmt.Printf("[VM] Truncating table: %s\n", tableName)

	if err := vm.storageEngine.TruncateTable(tableName); err != nil {
		return fmt.Errorf("truncate failed: %w", err)
	}

	fmt.Printf("[VM] Table '%s' truncated successfully\n", tableName)
	return nil
}
