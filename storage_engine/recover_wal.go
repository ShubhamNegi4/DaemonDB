package storageengine

import (
	"fmt"

	"DaemonDB/types"
)

// RecoverFromWAL is called once at startup before the engine accepts any
// queries.  It loads the last checkpoint, replays all WAL operations that
// follow it, and skips anything that was explicitly aborted or that belongs
// to an uncommitted transaction.
func (se *StorageEngine) RecoverFromWAL() error {

	// find the starting LSN from the last checkpoint

	var startLSN uint64 = 0

	if se.CheckpointManager != nil {
		checkpoint, err := se.CheckpointManager.LoadCheckpoint()
		if err != nil {
			fmt.Printf("Warning: failed to load checkpoint: %v — replaying from LSN 0\n", err)
		} else {
			startLSN = checkpoint.LSN
			// if startLSN > 0 {
			// 	// fmt.Printf("Loaded checkpoint at LSN %d\n", startLSN)
			// }
		}
	}

	//  collect all WAL operations from startLSN onwards

	var ops []*types.Operation

	err := se.WalManager.ReplayFromLSN(startLSN, func(op *types.Operation) error {
		ops = append(ops, op)
		return nil
	})
	if err != nil {
		return fmt.Errorf("failed to read WAL: %w", err)
	}

	if len(ops) == 0 {
		fmt.Println("WAL recovery complete — no operations to replay.")
		return nil
	}

	fmt.Printf("Found %d WAL operations after checkpoint LSN %d\n", len(ops), startLSN)

	//  single pass to build committed and aborted sets
	//
	// committed:   txnID → true  for transactions that reached OpTxnCommit.
	//              DML ops whose TxnID is not in this set are skipped.
	// aborted:   txnID → true  for transactions that reached OpTxnAbort.
	//              Ops that failed to run, aborted is run to stop that on ongoing commit
	// abortedLSN: original-op-LSN → true  for DDL ops followed by an
	//              OpAbort compensation record.  These are skipped even
	//              though they carry no TxnID.

	committed := make(map[uint64]bool)
	aborted := make(map[uint64]bool)
	abortedLSN := make(map[uint64]bool)

	for _, op := range ops {
		switch op.Type {
		case types.OpTxnCommit:
			committed[op.TxnID] = true
		case types.OpTxnAbort:
			aborted[op.TxnID] = true
		case types.OpAbort:
			abortedLSN[op.TargetLSN] = true
		}
	}

	fmt.Println("[Recovery] Starting WAL recovery")
	fmt.Printf("[Recovery] Checkpoint LSN=%d\n", startLSN)
	fmt.Printf("[Recovery] Found %d ops after checkpoint\n", len(ops))
	fmt.Printf("[Recovery] Committed txns: %v\n", printIds(committed))
	fmt.Printf("[Recovery] Aborted txns:   %v\n", printIds(aborted))

	// replay in WAL order, skipping aborted / uncommitted ops

	replayed := 0

	for _, op := range ops {
		// Control records are never replayed as state changes.
		switch op.Type {
		case types.OpTxnBegin, types.OpTxnCommit, types.OpTxnAbort, types.OpAbort:
			continue
		}

		// Skip DML that belongs to an uncommitted transaction.
		if op.TxnID != 0 && !committed[op.TxnID] {
			continue
		}

		// Skip DDL ops that were cancelled by a compensation record.
		if abortedLSN[op.LSN] {
			fmt.Printf("  Skipping aborted op LSN=%d table=%s\n", op.LSN, op.Table)
			continue
		}

		fmt.Printf("[Recovery] REDO op=%d lsn=%d table=%s txnID=%d\n", op.Type, op.LSN, op.Table, op.TxnID)

		var err error
		switch op.Type {
		case types.OpCreateTable:
			err = se.replayCreateTable(op)
		case types.OpInsert:
			err = se.replayInsert(op)
		// case types.OpDrop:
		// 	err = se.replayDrop(op)
		case types.OpDelete:
			err = se.replayDelete(op)
		case types.OpUpdate:
			err = se.replayUpdate(op)
		}

		if err != nil {
			return fmt.Errorf("replay failed at LSN %d (op=%d table=%s): %w",
				op.LSN, op.Type, op.Table, err)
		}
		replayed++
	}

	// UNDO — physically remove writes from uncommitted transactions ─
	// Iterate in REVERSE order — last write first.
	undone := 0
	for i := len(ops) - 1; i >= 0; i-- {
		op := ops[i]

		// Only undo DML from transactions that never committed
		if op.TxnID == 0 || committed[op.TxnID] {
			continue
		}

		fmt.Printf("[Recovery] UNDO op=%d lsn=%d table=%s txnID=%d\n", op.Type, op.LSN, op.Table, op.TxnID)

		switch op.Type {
		case types.OpInsert:
			rp := op.RowPtr
			if err := se.HeapManager.DeleteRow(&rp, op.LSN); err != nil {
				fmt.Printf("  Warning: undo insert failed at LSN %d (table=%s): %v\n",
					op.LSN, op.Table, err)
				continue
			}
			// Remove from index
			schema, err := se.CatalogManager.GetTableSchema(op.Table)
			if err == nil {
				values, err := se.DeserializeRow(op.RowData, schema.Columns)
				if err == nil {
					pkBytes, _, _ := se.ExtractPrimaryKey(schema, values, &rp)
					btree, err := se.getIndex(op.Table)
					if err == nil {
						btree.Delete(pkBytes)
					}
				}
			}
			undone++

		case types.OpUpdate:
			// op.RowData is the NEW data, we need to restore old data
			// Old data isn't stored in WAL currently — this is a limitation.
			// For now, log a warning. Full MVCC would store before-image in WAL.
			fmt.Printf("  Warning: cannot undo update at LSN %d (table=%s) — before-image not in WAL\n",
				op.LSN, op.Table)
		}
	}

	fmt.Printf("[Recovery] Complete — redone=%d undone=%d\n", replayed, undone)
	return nil
}

// individual replay handlers
// Each handler is intentionally thin: it delegates to the same public methods
// that normal query execution uses, so compensation, locking, and state
// updates are never duplicated.

func (se *StorageEngine) replayCreateTable(op *types.Operation) error {
	if op.Schema == nil {
		return fmt.Errorf("replayCreateTable: op at LSN %d has nil schema", op.LSN)
	}

	// Idempotent: if the table already exists we crashed after the heap file
	// was created — nothing to do.
	if se.CatalogManager.TableExists(op.Table) {
		fmt.Printf("  replayCreateTable: '%s' already exists, skipping\n", op.Table)
		return nil
	}

	// Reuse the exact same path as the normal CREATE TABLE execution.
	// This means WAL compensation logic, catalog rollback, etc. all apply
	// automatically — no duplication.
	return se.CreateTable(*op.Schema)
}

func (se *StorageEngine) replayInsert(op *types.Operation) error {
	if op.RowData == nil {
		return fmt.Errorf("replayInsert: nil row data at LSN %d", op.LSN)
	}
	if !se.CatalogManager.TableExists(op.Table) {
		return fmt.Errorf("replayInsert: table '%s' does not exist", op.Table)
	}

	fileID, err := se.CatalogManager.GetTableFileID(op.Table)
	if err != nil {
		return err
	}

	// Check if page already has this write — page was flushed before crash
	if op.RowPtr.PageNumber != 0 {
		pageLSN, err := se.HeapManager.GetPageLSN(fileID, op.RowPtr.PageNumber)
		if err == nil && pageLSN >= op.LSN {
			fmt.Printf("  replayInsert: skipping LSN %d — page %d already up to date (pageLSN=%d)\n",
				op.LSN, op.RowPtr.PageNumber, pageLSN)
			return nil
		}
	}

	return se.HeapManager.InsertRowAtPointer(fileID, &op.RowPtr, op.RowData, op.LSN)
}

// func (se *StorageEngine) replayDrop(op *types.Operation) error {
// 	if !se.CatalogManager.TableExists(op.Table) {
// 		fmt.Printf("  replayDrop: '%s' does not exist, skipping\n", op.Table)
// 		return nil
// 	}

// 	return se.DropTable(op.Table)
// }

func (se *StorageEngine) replayDelete(op *types.Operation) error {

	if !se.CatalogManager.TableExists(op.Table) {
		return fmt.Errorf("replayDelete: table '%s' does not exist at LSN %d", op.Table, op.LSN)
	}
	// Skip if page already has this write
	fileID, err := se.CatalogManager.GetTableFileID(op.Table)
	if err != nil {
		return err
	}
	pageLSN, err := se.HeapManager.GetPageLSN(fileID, op.RowPtr.PageNumber)
	if err == nil && pageLSN >= op.LSN {
		fmt.Printf("  replayDelete: skipping LSN %d — page already up to date\n", op.LSN)
		return nil
	}

	rp := op.RowPtr
	return se.HeapManager.DeleteRow(&rp, op.LSN)
}

func (se *StorageEngine) replayUpdate(op *types.Operation) error {
	if op.RowData == nil {
		return fmt.Errorf("replayUpdate: nil row data at LSN %d", op.LSN)
	}

	if !se.CatalogManager.TableExists(op.Table) {
		return fmt.Errorf("replayUpdate: table '%s' does not exist at LSN %d", op.Table, op.LSN)
	}

	fileID, err := se.CatalogManager.GetTableFileID(op.Table)
	if err != nil {
		return err
	}
	pageLSN, err := se.HeapManager.GetPageLSN(fileID, op.RowPtr.PageNumber)
	if err == nil && pageLSN >= op.LSN {
		fmt.Printf("  replayUpdate: skipping LSN %d — page already up to date\n", op.LSN)
		return nil
	}

	rp := op.RowPtr
	return se.HeapManager.UpdateRow(&rp, op.RowData, op.LSN)
}

// helpers used only during recovery

func printIds(m map[uint64]bool) []uint64 {
	ids := make([]uint64, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	return ids
}
