package executor

import (
	"DaemonDB/types"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func (vm *VM) ExecuteInsert(tableName string) error {
	if vm.currDb == "" {
		return fmt.Errorf("no database selected. Run: USE <dbname>")
	}

	schemaPath := filepath.Join(DB_ROOT, vm.currDb, "tables", tableName+"_schema.json")
	data, err := os.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("table not found %s: %w", tableName, err)
	}

	var schema types.TableSchema
	if err := json.Unmarshal(data, &schema); err != nil {
		return fmt.Errorf("invalid schema: %w", err)
	}

	fileID, ok := vm.tableToFileId[tableName]
	if !ok {
		return fmt.Errorf("heap file not found for table '%s'", tableName)
	}

	columnNames := []types.ColumnDef{}
	if len(columnNames) == 0 {
		for _, col := range schema.Columns {
			columnNames = append(columnNames, types.ColumnDef{Name: col.Name, Type: col.Type, IsPrimaryKey: col.IsPrimaryKey})
		}
	}

	if len(vm.stack) < len(schema.Columns) {
		return fmt.Errorf("table schema doesnt match the given query")
	}

	values := make([]any, len(schema.Columns))
	for i := len(schema.Columns) - 1; i >= 0; i-- {
		if len(vm.stack) == 0 {
			return fmt.Errorf("stack underflow at column %d", i)
		}
		v := vm.stack[len(vm.stack)-1]
		vm.stack = vm.stack[:len(vm.stack)-1]
		values[i] = v
	}

	if len(columnNames) != len(values) {
		return fmt.Errorf("column count (%d) doesn't match value count (%d)",
			len(columnNames), len(values))
	}

	for _, fk := range schema.ForeignKeys {
		fkColIdx := -1
		var fkCol types.ColumnDef

		for i, c := range schema.Columns {
			if strings.EqualFold(c.Name, fk.Column) {
				fkColIdx = i
				fkCol = c
				break
			}
		}

		if fkColIdx == -1 {
			return fmt.Errorf("foreign key column %s not found", fk.Column)
		}

		fkValueBytes, err := ValueToBytes(values[fkColIdx], fkCol.Type)
		if err != nil {
			return fmt.Errorf("foreign key value error: %w", err)
		}

		refIndex, err := vm.GetOrCreateIndex(fk.RefTable)
		if err != nil {
			return fmt.Errorf("referenced table %s not found", fk.RefTable)
		}

		refRowPtr, err := refIndex.Search(fkValueBytes)
		if err != nil || refRowPtr == nil {
			return fmt.Errorf(
				"foreign key constraint failed: %s.%s → %s.%s",
				tableName, fk.Column, fk.RefTable, fk.RefColumn,
			)
		}
	}

	row, err := vm.SerializeRow(columnNames, values)
	if err != nil {
		return err
	}

	// Auto Transaction
	if vm.currentTxn == nil {
		err := vm.autoTransactionBegin()
		if err != nil {
			return fmt.Errorf("failed to auto-begin transaction: %w", err)
		}
	}

	op := &types.Operation{
		Type:    types.OpInsert,
		TxnID:   vm.currentTxn.ID,
		Table:   tableName,
		RowData: row,
	}

	lsn, err := vm.WalManager.AppendOperation(op)
	if err != nil {
		return fmt.Errorf("wal append failed: %w", err)
	}

	op.LSN = lsn

	if err := vm.WalManager.Sync(); err != nil {
		return fmt.Errorf("wal sync failed: %w", err)
	}

	rowPtr, err := vm.heapfileManager.InsertRow(fileID, row, lsn)
	if err != nil {
		return fmt.Errorf("heap insert failed: %w", err)
	}

	op.RowPtr = rowPtr

	primaryKeyBytes, _, err := vm.ExtractPrimaryKey(schema, values, rowPtr)
	if err != nil {
		return fmt.Errorf("failed to extract primary key: %w", err)
	}

	if primaryKeyBytes != nil {
		rowPtrBytes := vm.SerializeRowPointer(rowPtr)
		btree, err := vm.GetOrCreateIndex(tableName)
		if err != nil {
			return fmt.Errorf("failed to get index: %w", err)
		}
		btree.Insertion(primaryKeyBytes, rowPtrBytes)
	}

	if vm.autoTxn {
		err := vm.autoTransactionEnd()
		if err != nil {
			return fmt.Errorf("failed to auto-commit: %w", err)
		}
	}

	return nil
}
