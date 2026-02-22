package storageengine

import (
	txn "DaemonDB/storage_engine/transaction_manager"
	"DaemonDB/types"
	"fmt"
	"strings"
)

/*
This file contains the insert row operations
schema saved by catalog manager ensure that the foreign keys exist on the references table
call to index manager is made for creating a index and then inserting it in the Bplus Tree

During the Insert (The "Append" Phase)
Pre-allocate LSN: Get the next LSN from the WAL Manager.

	Heap Insert: Insert the row into the Buffer Pool (RAM). This marks the page as "dirty" and assigns it the LSN.

	WAL Append: Call AppendToBuffer. This writes the record to the WAL file (OS Page Cache), but does not call fsync.

	Status: If the power cuts now, the change is lost. This is okay because the transaction hasn't "committed" yet.

During the Commit (The "Durability" Phase)
This is where the magic happens. A transaction is only "Real" once the WAL is synced.

	Call WalManager.Sync(): This triggers file.Sync() (fsync).

	Update FlushedLSN: Now you know for a fact that all operations up to this LSN are safe on the physical platter.

	Acknowledge to User: Tell the user "Success."


	SQL: INSERT INTO mytable VALUES (5)
         ↓
    Parser → AST → Compiler → Bytecode
         ↓
    VM.ExecuteInsert
         ↓
    StorageEngine.InsertRow(txn, "mytable", [5])
         ├── CatalogManager.GetTableSchema("mytable")
         ├── SerializeRow([5], schema) → rowBytes
         ├── WAL.AllocateLSN()
         ├── HeapManager.InsertRow(heapFileID, rowBytes, lsn)
         │       └── findSuitablePage → InsertRecord → RowPointer{file=1, page=0, slot=0}
         ├── WAL.AppendToBuffer(OpInsert, rowBytes, rowPtr)
         ├── BTree.Insertion(pkBytes, rowPtrBytes)
         └── txn.RecordInsert(table, rowPtr, pkBytes)
*/

func (se *StorageEngine) InsertRow(txn *txn.Transaction, tableName string, values []any) error {
	fmt.Printf("values :%+v", values)
	// ── Step 1: Load schema ───────────────────────────────────────────────────
	schema, err := se.CatalogManager.GetTableSchema(tableName)
	if err != nil {
		return fmt.Errorf("table '%s' not found: %w", tableName, err)
	}

	if len(values) != len(schema.Columns) {
		return fmt.Errorf("column count mismatch: expected %d, got %d",
			len(schema.Columns), len(values))
	}

	// ── Step 2: Validate foreign key constraints ─────────────────────────────
	for _, fk := range schema.ForeignKeys {
		// Find the FK column in the schema.
		fkColIdx := -1
		var fkCol types.ColumnDef
		for i, col := range schema.Columns {
			if strings.EqualFold(col.Name, fk.Column) {
				fkColIdx = i
				fkCol = col
				break
			}
		}
		if fkColIdx == -1 {
			return fmt.Errorf("foreign key column '%s' not found in schema", fk.Column)
		}

		// Serialize the FK value to bytes for index lookup.
		fkValueBytes, err := ValueToBytes(values[fkColIdx], fkCol.Type)
		if err != nil {
			return fmt.Errorf("failed to serialize FK value: %w", err)
		}

		// Check that the referenced row exists in the parent table's index.
		refTree, err := se.getIndex(fk.RefTable)
		if err != nil {
			return fmt.Errorf("referenced table '%s' index not found: %w", fk.RefTable, err)
		}

		refRowPtr, err := refTree.Search(fkValueBytes)
		if err != nil || refRowPtr == nil {
			return fmt.Errorf(
				"foreign key constraint violation: %s.%s → %s.%s (value not found in parent)",
				tableName, fk.Column, fk.RefTable, fk.RefColumn,
			)
		}
	}

	// ── Step 3: Serialize row to binary format ───────────────────────────────
	row, err := se.SerializeRow(schema.Columns, values)
	if err != nil {
		return fmt.Errorf("failed to serialize row: %w", err)
	}

	// ── Step 4: Write to WAL ──────────────────────────────────────────────────
	var txnID uint64 = 0
	if txn != nil {
		txnID = txn.ID
	}

	fmt.Printf(" txid: %d", txnID)

	lsn := se.WalManager.AllocateLSN(len(row))

	// ── Step 5: Write to heap file ────────────────────────────────────────────
	fileID, err := se.CatalogManager.GetTableFileID(tableName)
	if err != nil {
		return fmt.Errorf("no heap file registered for table '%s': %w", tableName, err)
	}
	fmt.Printf("hehehehehe %d %+v %d", fileID, row, lsn)
	rowPtr, err := se.HeapManager.InsertRow(fileID, row, lsn)
	if err != nil {
		return fmt.Errorf("heap insert failed: %w", err)
	}

	op := &types.Operation{
		Type:    types.OpInsert,
		TxnID:   txnID,
		Table:   tableName,
		RowData: row,
		RowPtr:  *rowPtr,
	}

	if err := se.WalManager.AppendToBuffer(op, lsn); err != nil {
		_ = se.HeapManager.DeleteRow(rowPtr, lsn)
		return fmt.Errorf("WAL buffer append failed: %w", err)
	}

	// ── Step 6: Update primary key index ──────────────────────────────────────
	primaryKeyBytes, _, err := se.ExtractPrimaryKey(schema, values, rowPtr)
	if err != nil {
		_ = se.HeapManager.DeleteRow(rowPtr, lsn) // compensate
		return fmt.Errorf("failed to extract primary key: %w", err)
	}

	rowPtrBytes := se.SerializeRowPointer(*rowPtr)
	btree, err := se.getIndex(tableName)
	if err != nil {
		_ = se.HeapManager.DeleteRow(rowPtr, lsn)
		return fmt.Errorf("failed to get index for '%s': %w", tableName, err)
	}
	if err := btree.Insertion(primaryKeyBytes, rowPtrBytes); err != nil {
		_ = se.HeapManager.DeleteRow(rowPtr, lsn)
		return fmt.Errorf("index insert failed: %w", err)
	}

	// Record for rollback — only after both heap and index succeed
	txn.RecordInsert(tableName, *rowPtr, primaryKeyBytes)

	return nil
}
