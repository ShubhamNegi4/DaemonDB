package executor

import (
	"DaemonDB/types"
	"encoding/json"
	"fmt"
)

/*
This file contains command related to create table,
the vm function does the pre processing like building schema and validation foreign keys before sending it to the storage engine
*/

func (vm *VM) ExecuteCreateTable(tableName string) error {
	if err := vm.storageEngine.RequireDatabase(); err != nil {
		return fmt.Errorf("no database selected. Run: USE <dbname>")
	}

	schemaPayload := string(vm.stack[len(vm.stack)-1])
	vm.stack = vm.stack[:len(vm.stack)-1]

	var payload struct {
		Columns     string                `json:"columns"`
		ForeignKeys []types.ForeignKeyDef `json:"foreign_keys"`
	}

	if err := json.Unmarshal([]byte(schemaPayload), &payload); err != nil {
		return fmt.Errorf("invalid table schema payload: %w", err)
	}

	// Build column definitions
	columnDefs, err := vm.buildColumnDefs(payload.Columns)
	if err != nil {
		return err
	}

	// Build schema object
	schema := types.TableSchema{
		TableName:   tableName,
		Columns:     columnDefs,
		ForeignKeys: payload.ForeignKeys,
	}

	// Validate foreign keys (semantic validation only)
	if err := vm.validateForeignKeys(schema); err != nil {
		return err
	}

	// Delegate full persistence to storage engine
	if err := vm.storageEngine.CreateTable(schema); err != nil {
		return err
	}

	fmt.Printf("Table %s created successfully\n", tableName)
	return nil
}
