package storageengine

import (
	"DaemonDB/types"
	"fmt"
)

/*
This file contains the Create Table process
The table schema and mapping to the fileId is made persisted by the catalog manager
catalog manager writes the table schema to table_schema.json
and also manages the meta data like heap file counter and table_file_mapping for the table
*/

func (se *StorageEngine) CreateTable(schema types.TableSchema) error {
	if err := se.RequireDatabase(); err != nil {
		return fmt.Errorf("no database selected. Run: USE <dbname>")
	}

	tableName := schema.TableName

	if se.CatalogManager.TableExists(tableName) {
		return fmt.Errorf("table '%s' already exists", tableName)
	}

	op := &types.Operation{
		Type:   types.OpCreateTable,
		Table:  tableName,
		Schema: &schema,
	}
	lsn, err := se.WalManager.AppendOperation(op)
	if err != nil {
		return fmt.Errorf("wal append failed: %w", err)
	}
	if err := se.WalManager.Sync(); err != nil {
		return fmt.Errorf("wal sync failed: %w", err)
	}

	// compensate appends an OpAbort record referencing lsn so the WAL
	// replayer knows to skip the original CREATE TABLE on recovery.
	compensate := func(original error) error {
		abortOp := &types.Operation{
			Type:      types.OpAbort,
			Table:     tableName,
			TargetLSN: lsn, // points at the CREATE TABLE record above
		}
		if _, werr := se.WalManager.AppendOperation(abortOp); werr != nil {
			return fmt.Errorf(
				"CRITICAL: error [%w]; also failed to write WAL abort record: %v",
				original, werr,
			)
		}
		if werr := se.WalManager.Sync(); werr != nil {
			return fmt.Errorf(
				"CRITICAL: error [%w]; also failed to sync WAL abort record: %v",
				original, werr,
			)
		}
		return original
	}

	// ── Step 2: Register in catalog ─────────────────────────────────────────
	fileID, indexFileID, err := se.CatalogManager.RegisterNewTable(schema)
	if err != nil {
		return compensate(fmt.Errorf("failed to register table in catalog: %w", err))
	}

	if err := se.HeapManager.CreateHeapfile(tableName, int(fileID)); err != nil {
		if rerr := se.CatalogManager.UnregisterTable(tableName); rerr != nil {
			return compensate(fmt.Errorf(
				"failed to create heap file [%w]; also failed to roll back catalog entry: %v",
				err, rerr,
			))
		}
		return compensate(fmt.Errorf("failed to create heap file: %w", err))
	}

	// pre-create index file with its catalog-assigned ID
	if _, err := se.IndexManager.GetOrCreateIndex(tableName, indexFileID); err != nil {
		if rerr := se.CatalogManager.UnregisterTable(tableName); rerr != nil {
			return compensate(fmt.Errorf(
				"failed to create index [%w]; also failed to roll back catalog entry: %v",
				err, rerr,
			))
		}
		return compensate(fmt.Errorf("failed to create index: %w", err))
	}

	return nil
}
