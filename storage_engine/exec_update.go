package storageengine

import (
	txn "DaemonDB/storage_engine/transaction_manager"
	types "DaemonDB/types"
	"fmt"
)

/*
This file contains the Update row functionality
It searches for the row pointer

this is similar to Insert Row

*/

func (se *StorageEngine) UpdateRow(txn *txn.Transaction, tableName string, ptr types.RowPointer, newRow types.Row) error {
	schema, err := se.CatalogManager.GetTableSchema(tableName)
	if err != nil {
		return fmt.Errorf("table '%s' not found: %w", tableName, err)
	}

	// Read old row BEFORE overwriting — needed for rollback
	oldRowData, err := se.HeapManager.GetRow(&ptr)
	if err != nil {
		return fmt.Errorf("failed to read old row for undo log: %w", err)
	}

	oldValues, err := se.DeserializeRow(oldRowData, schema.Columns)
	if err != nil {
		return fmt.Errorf("failed to deserialize old row: %w", err)
	}
	oldPKBytes, _, err := se.ExtractPrimaryKey(schema, oldValues, &ptr)
	if err != nil {
		return fmt.Errorf("failed to extract old PK: %w", err)
	}

	// Serialize row to bytes
	serialized, err := se.SerializeRowFromMap(schema.Columns, newRow)
	if err != nil {
		return err
	}

	var txnID uint64
	if txn != nil {
		txnID = txn.ID
	}

	lsn := se.WalManager.AllocateLSN(len(serialized))

	oldPtr := ptr
	if err := se.HeapManager.UpdateRow(&ptr, serialized, lsn); err != nil {
		return fmt.Errorf("heap update failed: %w", err)
	}

	op := &types.Operation{
		Type:    types.OpUpdate,
		TxnID:   txnID,
		Table:   tableName,
		RowPtr:  ptr,
		OldPtr:  oldPtr,
		RowData: serialized,
	}

	if err := se.WalManager.AppendToBuffer(op, lsn); err != nil {
		_ = se.HeapManager.DeleteRow(&ptr, lsn)
		return fmt.Errorf("WAL buffer append failed: %w", err)
	}

	newValues, err := se.DeserializeRow(serialized, schema.Columns)
	if err != nil {
		return fmt.Errorf("failed to deserialize updated row: %w", err)
	}

	newPKBytes, _, err := se.ExtractPrimaryKey(schema, newValues, &ptr)
	if err != nil {
		return fmt.Errorf("failed to extract new PK: %w", err)
	}

	btree, err := se.getIndex(tableName)
	if err != nil {
		return fmt.Errorf("failed to get index: %w", err)
	}

	// If row moved (delete+reinsert path), old index entry is now stale.
	if oldPtr != ptr {
		oldValues, err := se.DeserializeRow(oldRowData, schema.Columns)
		if err == nil {
			oldPKBytes, _, _ := se.ExtractPrimaryKey(schema, oldValues, &oldPtr)
			_ = btree.Delete(oldPKBytes)
		}
	}

	// Insert updated index entry.
	rowPtrBytes := se.SerializeRowPointer(ptr)
	if err := btree.Insertion(newPKBytes, rowPtrBytes); err != nil {
		return fmt.Errorf("index update failed: %w", err)
	}

	// Record for rollback — only after both heap and index succeed
	txn.RecordUpdate(tableName, oldPtr, ptr, oldRowData, oldPKBytes)

	return nil
}
