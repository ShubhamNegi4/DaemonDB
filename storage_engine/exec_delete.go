package storageengine

import (
	"DaemonDB/types"
	"fmt"
)

func (se *StorageEngine) DeleteRows(tableName string) error {

	// Ensure DB selected
	if err := se.RequireDatabase(); err != nil {
		return err
	}

	// Validate table
	if !se.CatalogManager.TableExists(tableName) {
		return fmt.Errorf("table '%s' does not exist", tableName)
	}

	// Load schema
	schema, err := se.CatalogManager.GetTableSchema(tableName)
	if err != nil {
		return err
	}

	// WAL record
	op := &types.Operation{
		Type:  types.OpDelete,
		Table: tableName,
	}

	lsn, err := se.WalManager.AppendOperation(op)
	if err != nil {
		return err
	}

	if err := se.WalManager.Sync(); err != nil {
		return err
	}

	// Get heap file
	hf, err := se.HeapManager.GetHeapFileByTable(tableName)
	if err != nil {
		return err
	}

	rowPtrs := hf.GetAllRowPointers()

	if len(rowPtrs) == 0 {
		fmt.Printf("Table '%s' already empty\n", tableName)
		return nil
	}

	// Get index
	index, _ := se.getIndex(tableName)

	deleted := 0

	for _, rp := range rowPtrs {

		rawRow, err := se.HeapManager.GetRow(&rp)
		if err != nil {
			continue
		}

		values, err := se.DeserializeRow(rawRow, schema.Columns)
		if err != nil {
			continue
		}

		// Remove index entry
		pkBytes, _, err := se.ExtractPrimaryKey(schema, values, &rp)
		if err == nil && index != nil {
			index.Delete(pkBytes)
		}

		// Delete from heap
		if err := se.HeapManager.DeleteRow(&rp, lsn); err != nil {
			return err
		}

		deleted++
	}

	fmt.Printf("Deleted %d rows from '%s'\n", deleted, tableName)

	return nil
}
