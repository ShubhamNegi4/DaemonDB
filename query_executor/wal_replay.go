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

func opTypeToString(opType types.OperationType) string {
	switch opType {
	case types.OpInsert:
		return "INSERT"
	case types.OpCreateTable:
		return "CREATE TABLE"
	default:
		return "UNKNOWN"
	}
}
