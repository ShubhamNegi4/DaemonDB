package executor

import (
	"fmt"
)

/*
This file contains command related to inserting value into the table,
the vm function does the pre processing like getting schema from catalog manager and validation over number of columns passed in the query
vm also calls auto transaction handlers for the insert operation
*/

func (vm *VM) ExecuteInsert(tableName string) error {
	if err := vm.storageEngine.RequireDatabase(); err != nil {
		return fmt.Errorf("no database selected. Run: USE <dbname>")
	}

	schema, err := vm.storageEngine.CatalogManager.GetTableSchema(tableName)

	fmt.Print("schema: %+w", schema)
	if err != nil {
		return fmt.Errorf("table '%s' not found: %w", tableName, err)
	}

	if len(vm.stack) < len(schema.Columns) {
		return fmt.Errorf("stack underflow: need %d values, have %d",
			len(schema.Columns), len(vm.stack))
	}

	values := make([]any, len(schema.Columns))
	for i := len(schema.Columns) - 1; i >= 0; i-- {
		values[i] = vm.stack[len(vm.stack)-1]
		vm.stack = vm.stack[:len(vm.stack)-1]
	}

	// Auto Transaction Begin
	if vm.currentTxn == nil {
		err := vm.autoTransactionBegin()
		if err != nil {
			return fmt.Errorf("failed to auto-begin transaction: %w", err)
		}
	}

	err = vm.storageEngine.InsertRow(vm.currentTxn, tableName, values)

	if err != nil {
		// Statement failed — if we auto-began, we must abort
		if vm.autoTxn {
			_ = vm.autoTransactionAbort()
		}
		return fmt.Errorf("failed to insert row: %w", err)
	}

	if vm.autoTxn {
		if err := vm.autoTransactionCommit(); err != nil {
			return fmt.Errorf("failed to auto-commit: %w", err)
		}
	}

	return nil
}
