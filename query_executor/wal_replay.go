package executor

import (
	"DaemonDB/types"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

func (vm *VM) RecoverAndReplayFromWAL() error {
	fmt.Println("Starting WAL recovery...")

	// -------- PASS 0: read all WAL ops into memory --------

	var ops []*types.Operation

	err := vm.WalManager.ReplayFromLSN(0, func(op *types.Operation) error {
		ops = append(ops, op)
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to read WAL: %w", err)
	}

	// -------- PASS 1: find committed transactions --------

	committed := make(map[uint64]bool)

	for _, op := range ops {
		if op.Type == types.OpTxnCommit {
			committed[op.TxnID] = true
		}
	}

	// -------- PASS 2: replay only committed ops --------

	replayed := 0

	for _, op := range ops {

		// Skip uncommitted transactional ops
		if op.TxnID != 0 && !committed[op.TxnID] {
			continue
		}

		switch op.Type {

		case types.OpCreateTable:
			if err := vm.replayCreateTable(op); err != nil {
				return err
			}
			replayed++

		case types.OpInsert:
			if err := vm.replayInsert(op); err != nil {
				return err
			}
			replayed++

		case types.OpDrop:
			if err := vm.replayDrop(op); err != nil {
				return err
			}
			replayed++

		case types.OpDelete:
			if err := vm.replayDelete(op); err != nil {
				return err
			}
			replayed++

		case types.OpUpdate:
			if err := vm.replayUpdate(op); err != nil {
				return err
			}
			replayed++
		}
	}

	fmt.Printf("WAL recovery completed. Replayed %d operations.\n", replayed)
	return nil
}

func (vm *VM) replayCreateTable(op *types.Operation) error {
	if op.Schema == nil {
		return fmt.Errorf("create table operation missing schema")
	}

	tableName := op.Table
	schema := *op.Schema

	// Check if table already exists in memory
	if _, exists := vm.tableSchemas[tableName]; exists {
		fmt.Printf("  Table '%s' already exists, skipping...\n", tableName)
		return nil
	}
	// Add schema to in-memory map
	vm.tableSchemas[tableName] = schema
	fmt.Printf("  Restored schema for table '%s' with %d columns\n",
		tableName, len(schema.Columns))

	// Recreate schema file
	schemaPath := filepath.Join(DB_ROOT, vm.currDb, "tables", tableName+"_schema.json")

	if err := os.MkdirAll(filepath.Dir(schemaPath), 0755); err != nil {
		return fmt.Errorf("failed to create tables directory: %w", err)
	}

	schemaJson, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal schema: %w", err)
	}
	if err := os.WriteFile(schemaPath, schemaJson, 0644); err != nil {
		return fmt.Errorf("failed to write schema file: %w", err)
	}

	// Register heap file mapping if not exists
	if _, exists := vm.tableToFileId[tableName]; !exists {
		fileID := vm.heapFileCounter
		vm.heapFileCounter++
		vm.tableToFileId[tableName] = fileID
		fmt.Printf("  Assigned file ID %d to table '%s'\n", fileID, tableName)
	}

	fileID := vm.tableToFileId[tableName]

	// Recreate heap file
	if err := vm.heapfileManager.CreateHeapfile(tableName, fileID); err != nil {
		// If heap file already exists, just load it
		if _, loadErr := vm.heapfileManager.LoadHeapFile(fileID, tableName); loadErr != nil {
			return fmt.Errorf("failed to create/load heapfile: %w", err)
		}
	}

	// Save the table-to-fileID mapping
	if err := vm.SaveTableFileMapping(); err != nil {
		return fmt.Errorf("failed to save table file mapping: %w", err)
	}

	return nil
}

func (vm *VM) replayInsert(op *types.Operation) error {
	if op.RowData == nil {
		return fmt.Errorf("insert operation missing row data")
	}

	tableName := op.Table

	fileID, exists := vm.tableToFileId[tableName]
	if !exists {
		return fmt.Errorf("no file ID mapping for table '%s'", tableName)
	}

	_, err := vm.heapfileManager.InsertRow(fileID, op.RowData)
	if err != nil {
		return fmt.Errorf("failed to replay insert into '%s': %w", tableName, err)
	}

	fmt.Printf("Replayed insert into table '%s'\n", tableName)
	return nil
}

// replayDrop removes a table from in-memory state, closes its index, and deletes heap + index files.
func (vm *VM) replayDrop(op *types.Operation) error {
	tableName := op.Table
	if tableName == "" {
		return fmt.Errorf("drop operation missing table name")
	}

	fileID, exists := vm.tableToFileId[tableName]
	if !exists {
		fmt.Printf("  Table '%s' already dropped or missing, skipping replay\n", tableName)
		return nil
	}

	vm.closeIndexForTable(tableName)
	_ = vm.heapfileManager.CloseAndRemoveFile(fileID)
	delete(vm.tableSchemas, tableName)
	delete(vm.tableToFileId, tableName)

	indexPath := filepath.Join(DB_ROOT, vm.currDb, "indexes", tableName+"_primary.idx")
	_ = os.Remove(indexPath)
	schemaPath := filepath.Join(DB_ROOT, vm.currDb, "tables", tableName+"_schema.json")
	_ = os.Remove(schemaPath)

	if err := vm.SaveTableFileMapping(); err != nil {
		return fmt.Errorf("save table mapping after drop: %w", err)
	}
	fmt.Printf("  Replayed DROP table '%s'\n", tableName)
	return nil
}

// replayDelete tombstones the row at RowPtr and removes its key from the primary index.
func (vm *VM) replayDelete(op *types.Operation) error {
	if op.RowPtr == nil {
		return fmt.Errorf("delete operation missing row pointer")
	}
	tableName := op.Table
	schema, ok := vm.tableSchemas[tableName]
	if !ok {
		return fmt.Errorf("replay delete: table '%s' not found", tableName)
	}

	raw, err := vm.heapfileManager.GetRow(op.RowPtr)
	if err != nil {
		return fmt.Errorf("replay delete: get row: %w", err)
	}
	values, err := vm.DeserializeRow(raw, schema.Columns)
	if err != nil {
		return fmt.Errorf("replay delete: deserialize row: %w", err)
	}
	pkBytes, _, err := vm.ExtractPrimaryKey(schema, values, op.RowPtr)
	if err != nil {
		return fmt.Errorf("replay delete: extract PK: %w", err)
	}

	btree, err := vm.GetOrCreateIndex(tableName)
	if err != nil {
		return fmt.Errorf("replay delete: get index: %w", err)
	}
	btree.Delete(pkBytes)
	if err := vm.heapfileManager.DeleteRow(op.RowPtr); err != nil {
		return fmt.Errorf("replay delete: tombstone row: %w", err)
	}
	fmt.Printf("  Replayed DELETE from '%s'\n", tableName)
	return nil
}

// replayUpdate applies new row data: remove old key from index, tombstone old row, insert new row, add new key to index.
func (vm *VM) replayUpdate(op *types.Operation) error {
	if op.RowPtr == nil || op.RowData == nil {
		return fmt.Errorf("update operation missing row pointer or row data")
	}
	tableName := op.Table
	fileID, ok := vm.tableToFileId[tableName]
	if !ok {
		return fmt.Errorf("replay update: table '%s' not found", tableName)
	}
	schema, ok := vm.tableSchemas[tableName]
	if !ok {
		return fmt.Errorf("replay update: schema for '%s' not found", tableName)
	}

	raw, err := vm.heapfileManager.GetRow(op.RowPtr)
	if err != nil {
		return fmt.Errorf("replay update: get old row: %w", err)
	}
	oldValues, err := vm.DeserializeRow(raw, schema.Columns)
	if err != nil {
		return fmt.Errorf("replay update: deserialize old row: %w", err)
	}
	oldPkBytes, _, err := vm.ExtractPrimaryKey(schema, oldValues, op.RowPtr)
	if err != nil {
		return fmt.Errorf("replay update: extract old PK: %w", err)
	}

	btree, err := vm.GetOrCreateIndex(tableName)
	if err != nil {
		return fmt.Errorf("replay update: get index: %w", err)
	}
	btree.Delete(oldPkBytes)
	_ = vm.heapfileManager.DeleteRow(op.RowPtr)

	newPtr, err := vm.heapfileManager.InsertRow(fileID, op.RowData)
	if err != nil {
		return fmt.Errorf("replay update: insert new row: %w", err)
	}
	newValues, err := vm.DeserializeRow(op.RowData, schema.Columns)
	if err != nil {
		return fmt.Errorf("replay update: deserialize new row: %w", err)
	}
	newPkBytes, _, err := vm.ExtractPrimaryKey(schema, newValues, newPtr)
	if err != nil {
		return fmt.Errorf("replay update: extract new PK: %w", err)
	}
	rowPtrBytes := vm.SerializeRowPointer(newPtr)
	btree.Insertion(newPkBytes, rowPtrBytes)
	fmt.Printf("  Replayed UPDATE on '%s'\n", tableName)
	return nil
}

func opTypeToString(opType types.OperationType) string {
	switch opType {
	case types.OpInsert:
		return "INSERT"
	case types.OpCreateTable:
		return "CREATE TABLE"
	case types.OpDrop:
		return "DROP"
	case types.OpDelete:
		return "DELETE"
	case types.OpUpdate:
		return "UPDATE"
	default:
		return "UNKNOWN"
	}
}
