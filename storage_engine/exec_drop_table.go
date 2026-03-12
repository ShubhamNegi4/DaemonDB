package storageengine

import (
	"DaemonDB/types"
	"fmt"
	"os"
	"path/filepath"
)

func (se *StorageEngine) DropTable(tableName string) error {

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
		Type:  types.OpDrop,
		Table: tableName,
	}

	_, err := se.WalManager.AppendOperation(op)
	if err != nil {
		return fmt.Errorf("wal append failed: %w", err)
	}

	if err := se.WalManager.Sync(); err != nil {
		return fmt.Errorf("wal sync failed: %w", err)
	}

	// ---------------------------
	// Remove heap file
	// ---------------------------
	fileID, err := se.CatalogManager.GetTableFileID(tableName)
	if err == nil {

		heapPath := filepath.Join(
			se.DbRoot,
			se.currDb,
			"tables",
			fmt.Sprintf("%d.heap", fileID),
		)

		_ = os.Remove(heapPath)
	}

	// ---------------------------
	// Remove index file
	// ---------------------------
	indexFileID, err := se.CatalogManager.GetIndexFileID(tableName)
	if err == nil {

		indexPath := filepath.Join(
			se.DbRoot,
			se.currDb,
			"indexes",
			fmt.Sprintf("%d.idx", indexFileID),
		)

		_ = os.Remove(indexPath)
	}

	// ---------------------------
	// Remove catalog metadata
	// ---------------------------
	if err := se.CatalogManager.UnregisterTable(tableName); err != nil {
		return err
	}

	fmt.Printf("Table '%s' dropped\n", tableName)

	return nil
}
