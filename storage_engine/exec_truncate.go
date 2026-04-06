package storageengine

import (
	"DaemonDB/types"
	"fmt"
)

/*
TRUNCATE TABLE implementation.

Flow:
1. WAL log the truncate operation
2. Scan all rows in the heap file
3. Delete each row
4. Remove corresponding index entries
*/

func (se *StorageEngine) TruncateTable(tableName string) error {

	if err := se.RequireDatabase(); err != nil {
		return err
	}

	if !se.CatalogManager.TableExists(tableName) {
		return fmt.Errorf("table '%s' does not exist", tableName)
	}

	// ---------------------------
	// WAL log
	// ---------------------------
	op := &types.Operation{
		Type:  types.OpTruncateTable,
		Table: tableName,
	}

	lsn, err := se.WalManager.AppendOperation(op)
	if err != nil {
		return fmt.Errorf("wal append failed: %w", err)
	}

	if err := se.WalManager.Sync(); err != nil {
		return fmt.Errorf("wal sync failed: %w", err)
	}

	// ---------------------------
	// Load schema
	// ---------------------------
	schema, err := se.CatalogManager.GetTableSchema(tableName)
	if err != nil {
		return err
	}

	// ---------------------------
	// Get heap file
	// ---------------------------
	hf, err := se.HeapManager.GetHeapFileByTable(tableName)
	if err != nil {
		return err
	}

	rowPtrs := hf.GetAllRowPointers()

	if len(rowPtrs) == 0 {
		fmt.Printf("Table '%s' already empty\n", tableName)
		return nil
	}

	// ---------------------------
	// Get index
	// ---------------------------
	index, err := se.GetIndex(tableName)

	// ---------------------------
	// Delete rows
	// ---------------------------
	for _, rp := range rowPtrs {

		rawRow, err := se.HeapManager.GetRow(&rp)
		if err != nil {
			continue
		}

		values, err := se.DeserializeRow(rawRow, schema.Columns)
		if err != nil {
			continue
		}

		pkBytes, _, err := se.ExtractPrimaryKey(schema, values, &rp)
		if err == nil && index != nil {
			index.Delete(pkBytes)
		}

		if err := se.HeapManager.DeleteRow(&rp, lsn); err != nil {
			return err
		}
	}

	fmt.Printf("Table '%s' truncated (%d rows removed)\n", tableName, len(rowPtrs))

	return nil
}
