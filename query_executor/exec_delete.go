package executor

import (
	"fmt"
	"strings"
)

// ExecDelete executes DELETE through the storage engine
func (vm *VM) ExecDelete(table string, whereCol string, whereVal string) error {

	if vm.storageEngine == nil {
		return fmt.Errorf("storage engine not initialized")
	}

	// Ensure database selected
	if err := vm.storageEngine.RequireDatabase(); err != nil {
		return fmt.Errorf("no database selected. Run: USE <dbname>")
	}

	if table == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	// Validate table existence
	if !vm.storageEngine.CatalogManager.TableExists(table) {
		return fmt.Errorf("table '%s' does not exist", table)
	}

	// Optional column validation
	if whereCol != "" {
		schema, err := vm.storageEngine.CatalogManager.GetTableSchema(table)
		if err != nil {
			return fmt.Errorf("failed to fetch schema: %w", err)
		}

		found := false
		for _, col := range schema.Columns {
			if strings.EqualFold(col.Name, whereCol) {
				found = true
				break
			}
		}

		if !found {
			return fmt.Errorf("column '%s' not found in table '%s'", whereCol, table)
		}
	}

	fmt.Printf("[VM] Deleting rows from table: %s\n", table)

	if err := vm.storageEngine.DeleteRows(table, whereCol, whereVal); err != nil {
		return fmt.Errorf("delete failed: %w", err)
	}

	return nil
}
