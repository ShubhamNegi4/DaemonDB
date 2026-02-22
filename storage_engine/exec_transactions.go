package storageengine

import (
	txn "DaemonDB/storage_engine/transaction_manager"
	"DaemonDB/types"
	"fmt"
)

// Transaction WAL logging methods
/* These are called by the VM during auto-transaction boundaries.
They write OpTxnBegin/Commit/Abort records to the WAL and sync.
*/

// LogTransactionBegin writes an OpTxnBegin record to the WAL.
// Called by VM.autoTransactionBegin after TxnManager.Begin().
func (se *StorageEngine) LogTransactionBegin(txnID uint64) error {
	op := &types.Operation{
		Type:  types.OpTxnBegin,
		TxnID: txnID,
	}
	lsn := se.WalManager.AllocateLSN(0)
	return se.WalManager.AppendToBuffer(op, lsn)
}

// LogTransactionCommit writes an OpTxnCommit record to the WAL.
// Called by VM.autoTransactionCommit before TxnManager.Commit().
//
// CRITICAL: The WAL record must be synced to disk BEFORE calling
// TxnManager.Commit, so recovery knows which transactions succeeded.
func (se *StorageEngine) LogTransactionCommit(txnID uint64) error {
	op := &types.Operation{
		Type:  types.OpTxnCommit,
		TxnID: txnID,
	}
	lsn := se.WalManager.AllocateLSN(0)
	return se.WalManager.AppendToBuffer(op, lsn)
}

// LogTransactionAbort writes an OpTxnAbort record to the WAL.
// Called by VM.autoTransactionAbort before TxnManager.Abort().
func (se *StorageEngine) LogTransactionAbort(txnID uint64) error {
	op := &types.Operation{
		Type:  types.OpTxnAbort,
		TxnID: txnID,
	}
	if _, err := se.WalManager.AppendOperation(op); err != nil {
		return fmt.Errorf("failed to log transaction abort: %w", err)
	}
	return se.WalManager.Sync()
}

// SaveCheckpoint triggers a checkpoint after a transaction commits.
// This is optional — checkpoints can also be triggered periodically by a
// background thread, or after N transactions, or after N bytes written to WAL.
//
// For now, we checkpoint after every auto-committed transaction.
func (se *StorageEngine) SaveCheckpoint() error {
	if se.CheckpointManager == nil || se.WalManager == nil {
		return nil // checkpointing not enabled
	}

	currentLSN := se.WalManager.GetCurrentLSN()
	fmt.Printf("[Checkpoint] Saving at LSN=%d db=%s\n", currentLSN, se.currDb)

	return se.CheckpointManager.SaveCheckpoint(currentLSN, se.currDb)
}

// ── Transaction state management wrappers ─────────────────────────────────────
// These delegate to TxnManager so the VM doesn't need direct access to it.

// BeginTransaction starts a new transaction and returns it.
func (se *StorageEngine) BeginTransaction() (*txn.Transaction, error) {
	t := se.TxnManager.Begin()

	fmt.Printf("[TXN] BEGIN txnID=%d\n", t.ID)
	if err := se.LogTransactionBegin(t.ID); err != nil {
		return nil, fmt.Errorf("failed to log transaction begin: %w", err)
	}
	return t, nil
}

// CommitTransaction marks a transaction as committed in the TxnManager.
// Called AFTER LogTransactionCommit has synced the WAL record.
func (se *StorageEngine) CommitTransaction(txnID uint64) error {

	fmt.Printf("[TXN] COMMIT txnID=%d\n", txnID)
	if err := se.LogTransactionCommit(txnID); err != nil {
		return err
	}

	// fsync WAL — this is the durability boundary
	// After this, all buffered WAL records including DML ops are durable
	if err := se.WalManager.Sync(); err != nil {
		return err
	}
	fmt.Printf("[TXN] COMMIT WAL synced, flushing pages\n")

	// safe to flush dirty heap pages to disk
	// Buffer pool flush guard allows this because FlushedLSN is now up to date
	if err := se.BufferPool.FlushAllPages(); err != nil {
		// non-fatal — WAL can recover these pages on restart
		fmt.Printf("warning: buffer pool flush failed after commit: %v\n", err)
	}

	return se.TxnManager.Commit(txnID)
}

// AbortTransaction marks a transaction as aborted in the TxnManager.
func (se *StorageEngine) AbortTransaction(t *txn.Transaction) error {

	if t == nil {
		return fmt.Errorf("AbortTransaction: nil transaction")
	}

	fmt.Printf("[TXN] ABORT txnID=%d insertedRows=%d updatedRows=%d\n", t.ID, len(t.InsertedRows), len(t.UpdatedRows))

	if err := se.LogTransactionAbort(t.ID); err != nil {
		return err
	}

	abortLSN := se.WalManager.GetCurrentLSN()

	// Undo updates first (reverse order — last write first)
	for i := len(t.UpdatedRows) - 1; i >= 0; i-- {
		u := t.UpdatedRows[i]
		rp := u.NewRowPtr

		if err := se.HeapManager.UpdateRow(&rp, u.OldRowData, abortLSN); err != nil {
			return fmt.Errorf("rollback: restore updated row failed (table=%s page=%d slot=%d): %w",
				u.Table, rp.PageNumber, rp.SlotIndex, err)
		}

		idx, err := se.getIndex(u.Table)
		if err != nil {
			return fmt.Errorf("rollback: index open failed (table=%s): %w", u.Table, err)
		}
		idx.Delete(u.PrimaryKey)
		oldPtrBytes := se.SerializeRowPointer(u.OldRowPtr)
		if err := idx.Insertion(u.PrimaryKey, oldPtrBytes); err != nil {
			return fmt.Errorf("rollback: index reinsert failed (table=%s): %w", u.Table, err)
		}
	}

	// Undo inserts (reverse order)
	for i := len(t.InsertedRows) - 1; i >= 0; i-- {
		ins := t.InsertedRows[i]
		rp := ins.RowPtr

		if err := se.HeapManager.DeleteRow(&rp, abortLSN); err != nil {
			return fmt.Errorf("rollback: delete inserted row failed (table=%s page=%d slot=%d): %w",
				ins.Table, rp.PageNumber, rp.SlotIndex, err)
		}

		idx, err := se.getIndex(ins.Table)
		if err != nil {
			return fmt.Errorf("rollback: index open failed (table=%s): %w", ins.Table, err)
		}
		idx.Delete(ins.PrimaryKey)

		fmt.Printf("[TXN] ABORT undid insert table=%s page=%d slot=%d\n", ins.Table, rp.PageNumber, rp.SlotIndex)
	}

	if err := se.BufferPool.FlushAllPages(); err != nil {
		fmt.Printf("warning: buffer pool flush failed after abort: %v\n", err)
	}

	return se.TxnManager.Abort(t.ID)
}
