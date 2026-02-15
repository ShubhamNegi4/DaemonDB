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

	// -------- STEP 1: Load checkpoint to find starting LSN --------

	var startLSN uint64 = 0

	if vm.CheckpointManager != nil {
		checkpoint, err := vm.CheckpointManager.LoadCheckpoint()
		if err != nil {
			fmt.Printf("Warning: Failed to load checkpoint: %v\n", err)
			fmt.Println("Starting recovery from LSN 0")
		} else {
			startLSN = checkpoint.LSN
			if startLSN > 0 {
				fmt.Printf("Loaded checkpoint at LSN %d, replaying from there...\n", startLSN)
			} else {
				fmt.Println("No checkpoint found, replaying from LSN 0")
			}
		}
	}

	// -------- STEP 2: Read WAL operations starting from checkpoint --------

	var ops []*types.Operation

	err := vm.WalManager.ReplayFromLSN(startLSN, func(op *types.Operation) error {
		ops = append(ops, op)
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to read WAL: %w", err)
	}

	if len(ops) == 0 {
		fmt.Println("WAL recovery completed. No operations to replay.")
		return nil
	}

	fmt.Printf("Found %d operations in WAL after checkpoint\n", len(ops))

	// -------- STEP 3: find committed transactions --------

	committed := make(map[uint64]bool)

	for _, op := range ops {
		if op.Type == types.OpTxnCommit {
			committed[op.TxnID] = true
		}
	}

	// -------- STEP 4: Replay only committed operations (idempotently) --------

	replayed, ran := 0, 0

	for _, op := range ops {

		// Skip uncommitted transactional ops
		if op.TxnID != 0 && !committed[op.TxnID] {
			continue
		}

		switch op.Type {

		case types.OpCreateTable:
			if ran, err = vm.replayCreateTable(op); err != nil {
				return err
			}
			replayed += ran

		case types.OpInsert:
			if ran, err = vm.replayInsert(op); err != nil {
				return err
			}
			replayed += ran

		case types.OpDrop:
			if ran, err = vm.replayDrop(op); err != nil {
				return err
			}
			replayed += ran

		case types.OpDelete:
			if ran, err = vm.replayDelete(op); err != nil {
				return err
			}
			replayed += ran

		case types.OpUpdate:
			if ran, err = vm.replayUpdate(op); err != nil {
				return err
			}
			replayed += ran
		}
	}

	fmt.Printf("WAL recovery completed. Replayed %d operations.\n", replayed)
	return nil
}

func (vm *VM) replayCreateTable(op *types.Operation) (int, error) {
	if op.Schema == nil {
		return 0, fmt.Errorf("create table operation missing schema")
	}

	tableName := op.Table
	schema := *op.Schema

	// Check if table already exists in memory
	if _, exists := vm.tableSchemas[tableName]; exists {
		fmt.Printf("  Table '%s' already exists, skipping...\n", tableName)
		return 0, nil
	}
	// Add schema to in-memory map
	vm.tableSchemas[tableName] = schema
	fmt.Printf("  Restored schema for table '%s' with %d columns\n",
		tableName, len(schema.Columns))

	// Recreate schema file
	schemaPath := filepath.Join(DB_ROOT, vm.currDb, "tables", tableName+"_schema.json")

	if err := os.MkdirAll(filepath.Dir(schemaPath), 0755); err != nil {
		return 0, fmt.Errorf("failed to create tables directory: %w", err)
	}

	schemaJson, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return 0, fmt.Errorf("failed to marshal schema: %w", err)
	}
	if err := os.WriteFile(schemaPath, schemaJson, 0644); err != nil {
		return 0, fmt.Errorf("failed to write schema file: %w", err)
	}

	// Register heap file mapping if not exists
	if _, exists := vm.tableToFileId[tableName]; !exists {
		fileID := vm.heapFileCounter
		vm.heapFileCounter++
		vm.tableToFileId[tableName] = fileID
		fmt.Printf("  Assigned file ID %d to table '%s'\n", fileID, tableName)
	}

	fileID := vm.tableToFileId[tableName]

	// Don't recreate heap file - just load existing one
	heapPath := filepath.Join(DB_ROOT, vm.currDb, "tables",
		fmt.Sprintf("%s_%d.heap", tableName, fileID))

	if _, err := os.Stat(heapPath); os.IsNotExist(err) {
		fmt.Printf("  Creating heap file for '%s'\n", tableName)
		vm.heapfileManager.CreateHeapfile(tableName, fileID)
	} else {
		fmt.Printf("  Loading existing heap file for '%s'\n", tableName)
		vm.heapfileManager.LoadHeapFile(fileID, tableName)
	}

	vm.SaveTableFileMapping()

	return 1, nil
}

func (vm *VM) replayInsert(op *types.Operation) (int, error) {
	if op.RowData == nil {
		return 0, fmt.Errorf("insert operation missing row data")
	}

	tableName := op.Table

	schema, ok := vm.tableSchemas[tableName]
	if !ok {
		return 0, fmt.Errorf("no schema for table '%s'", tableName)
	}

	fileID, exists := vm.tableToFileId[tableName]
	if !exists {
		return 0, fmt.Errorf("no file ID mapping for table '%s'", tableName)
	}

	hf, err := vm.heapfileManager.GetHeapFileByID(fileID)
	if err != nil {
		return 0, fmt.Errorf("failed to get heap file: %w", err)
	}

	targetPage := uint32(0)
	if op.RowPtr != nil {
		targetPage = op.RowPtr.PageNumber
	}

	alreadyApplied, err := hf.CheckPageLSN(targetPage, op.LSN)

	if err != nil {
		return 0, fmt.Errorf("error checking lsn: %w", err)
	}

	if alreadyApplied {
		fmt.Printf("  Page LSN >= %d, insert already applied, skipping\n", op.LSN)
		return 0, nil
	}

	rowPtr, err := vm.heapfileManager.InsertRow(fileID, op.RowData, op.LSN)
	if err != nil {
		return 0, fmt.Errorf("failed to replay insert: %w", err)
	}

	// Update index
	values, _ := vm.DeserializeRow(op.RowData, schema.Columns)
	pkBytes, _, _ := vm.ExtractPrimaryKey(schema, values, rowPtr)

	if pkBytes != nil {
		rowPtrBytes := vm.SerializeRowPointer(rowPtr)
		btree, _ := vm.GetOrCreateIndex(tableName)
		btree.Insertion(pkBytes, rowPtrBytes)
	}

	fmt.Printf("Replayed insert (LSN %d) into '%s'\n", op.LSN, tableName)
	return 1, nil
}

// replayDrop removes a table from in-memory state, closes its index, and deletes heap + index files.
func (vm *VM) replayDrop(op *types.Operation) (int, error) {
	tableName := op.Table
	if tableName == "" {
		return 0, fmt.Errorf("drop operation missing table name")
	}

	fileID, exists := vm.tableToFileId[tableName]
	if !exists {
		fmt.Printf("  Table '%s' already dropped or missing, skipping replay\n", tableName)
		return 0, nil
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
		return 0, fmt.Errorf("save table mapping after drop: %w", err)
	}
	fmt.Printf("  Replayed DROP table '%s'\n", tableName)
	return 1, nil
}

// replayDelete tombstones the row at RowPtr and removes its key from the primary index.
func (vm *VM) replayDelete(op *types.Operation) (int, error) {
	if op.RowPtr == nil {
		return 0, fmt.Errorf("delete operation missing row pointer")
	}
	tableName := op.Table
	schema, ok := vm.tableSchemas[tableName]
	if !ok {
		return 0, fmt.Errorf("replay delete: table '%s' not found", tableName)
	}

	hf, err := vm.heapfileManager.GetHeapFileByID(vm.tableToFileId[tableName])
	if err != nil {
		return 0, fmt.Errorf("failed to get heap file: %w", err)
	}

	alreadyApplied, err := hf.CheckPageLSN(op.RowPtr.PageNumber, op.LSN)
	if err != nil {
		return 0, fmt.Errorf("error checking lsn: %w", err)
	}

	if alreadyApplied {
		fmt.Printf("  Page LSN >= %d, delete already applied, skipping\n", op.LSN)
		return 0, nil
	}

	raw, err := vm.heapfileManager.GetRow(op.RowPtr)
	if err != nil {
		return 0, fmt.Errorf("replay delete: get row: %w", err)
	}
	values, err := vm.DeserializeRow(raw, schema.Columns)
	if err != nil {
		return 0, fmt.Errorf("replay delete: deserialize row: %w", err)
	}
	pkBytes, _, err := vm.ExtractPrimaryKey(schema, values, op.RowPtr)
	if err != nil {
		return 0, fmt.Errorf("replay delete: extract PK: %w", err)
	}

	btree, err := vm.GetOrCreateIndex(tableName)
	if err != nil {
		return 0, fmt.Errorf("replay delete: get index: %w", err)
	}
	btree.Delete(pkBytes)
	if err := vm.heapfileManager.DeleteRow(op.RowPtr, op.LSN); err != nil {
		return 0, fmt.Errorf("replay delete: tombstone row: %w", err)
	}
	fmt.Printf("  Replayed DELETE from '%s'\n", tableName)
	return 1, nil
}

// replayUpdate applies new row data: remove old key from index, tombstone old row, insert new row, add new key to index.
func (vm *VM) replayUpdate(op *types.Operation) (int, error) {
	if op.RowPtr == nil || op.RowData == nil {
		return 0, fmt.Errorf("update operation missing row pointer or row data")
	}
	tableName := op.Table
	fileID, ok := vm.tableToFileId[tableName]
	if !ok {
		return 0, fmt.Errorf("replay update: table '%s' not found", tableName)
	}
	schema, ok := vm.tableSchemas[tableName]
	if !ok {
		return 0, fmt.Errorf("replay update: schema for '%s' not found", tableName)
	}

	hf, err := vm.heapfileManager.GetHeapFileByID(fileID)
	if err != nil {
		return 0, fmt.Errorf("failed to get heap file: %w", err)
	}

	alreadyApplied, err := hf.CheckPageLSN(op.RowPtr.PageNumber, op.LSN)

	if err != nil {
		return 0, fmt.Errorf("error checking lsn: %w", err)
	}

	if alreadyApplied {
		fmt.Printf("  Page LSN >= %d, update already applied, skipping\n", op.LSN)
		return 0, nil
	}

	raw, err := vm.heapfileManager.GetRow(op.RowPtr)
	if err != nil {
		return 0, fmt.Errorf("replay update: get old row: %w", err)
	}
	oldValues, err := vm.DeserializeRow(raw, schema.Columns)
	if err != nil {
		return 0, fmt.Errorf("replay update: deserialize old row: %w", err)
	}
	oldPkBytes, _, err := vm.ExtractPrimaryKey(schema, oldValues, op.RowPtr)
	if err != nil {
		return 0, fmt.Errorf("replay update: extract old PK: %w", err)
	}

	btree, err := vm.GetOrCreateIndex(tableName)
	if err != nil {
		return 0, fmt.Errorf("replay update: get index: %w", err)
	}
	btree.Delete(oldPkBytes)
	_ = vm.heapfileManager.DeleteRow(op.RowPtr, op.LSN)

	newPtr, err := vm.heapfileManager.InsertRow(fileID, op.RowData, op.LSN)
	if err != nil {
		return 0, fmt.Errorf("replay update: insert new row: %w", err)
	}
	newValues, err := vm.DeserializeRow(op.RowData, schema.Columns)
	if err != nil {
		return 0, fmt.Errorf("replay update: deserialize new row: %w", err)
	}
	newPkBytes, _, err := vm.ExtractPrimaryKey(schema, newValues, newPtr)
	if err != nil {
		return 0, fmt.Errorf("replay update: extract new PK: %w", err)
	}
	if newPkBytes != nil {
		rowPtrBytes := vm.SerializeRowPointer(newPtr)
		btree.Insertion(newPkBytes, rowPtrBytes)
	}
	fmt.Printf("  Replayed UPDATE on '%s'\n", tableName)
	return 1, nil
}

/*
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
*/

func (vm *VM) SaveCheckpoint() error {
	if vm.CheckpointManager == nil {
		return fmt.Errorf("checkpoint manager not initialized")
	}

	// Get current LSN from WAL manager
	currentLSN := vm.WalManager.CurrentLSN

	// Save checkpoint
	return vm.CheckpointManager.SaveCheckpoint(currentLSN, vm.currDb)
}
