package txn

import (
	"fmt"
	"sync/atomic"
)

/*
Transaction manager manages the BEGIN, COMMIT, ABORT state of quries that are to be made Atomically
(either all queries should run or none)
*/

func NewTxnManager() (*TxnManager, error) {
	return &TxnManager{
		nextID:     1,
		activeTxns: make(map[uint64]*Transaction),
	}, nil
}

// Begin starts a new transaction and registers it as active.
func (tm *TxnManager) Begin() *Transaction {
	// Use atomic increment to safely issue txn IDs from multiple goroutines.
	txnID := atomic.AddUint64(&tm.nextID, 1) - 1

	txn := &Transaction{
		ID:           txnID,
		State:        TxnActive,
		InsertedRows: make([]InsertedRow, 0),
	}

	tm.mu.Lock()
	tm.activeTxns[txnID] = txn
	tm.mu.Unlock()

	return txn
}

// Commit marks a transaction as committed and removes it from the active set.
// Called AFTER OpTxnCommit has been written to WAL and synced.
func (tm *TxnManager) Commit(txnID uint64) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	txn, exists := tm.activeTxns[txnID]
	if !exists {
		// Already committed/aborted or never existed — idempotent.
		return nil
	}

	if txn.State == TxnAborted {
		return fmt.Errorf("transaction %d was already aborted", txnID)
	}

	txn.State = TxnCommitted
	delete(tm.activeTxns, txnID)

	fmt.Printf("[TXN] COMMIT complete txnID=%d\n", txnID)
	return nil
}

// Abort marks a transaction as aborted and removes it from the active set.
// Called AFTER OpTxnAbort has been written to WAL and synced.
//
// In a full implementation, this would also roll back all writes:
//   - Delete inserted rows from heap file
//   - Remove inserted keys from indexes
//   - Restore old versions for updated rows (MVCC)
//
// For now, rollback is implicit: during recovery, uncommitted ops are skipped
// because their TxnID never appears in an OpTxnCommit record.
func (tm *TxnManager) Abort(txnID uint64) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	txn, exists := tm.activeTxns[txnID]
	if !exists {
		// Already committed/aborted or never existed — idempotent.
		return nil
	}

	if txn.State == TxnCommitted {
		return fmt.Errorf("transaction %d was already committed", txnID)
	}

	txn.State = TxnAborted
	delete(tm.activeTxns, txnID)

	return nil
}

// GetTransaction returns the transaction with the given ID, or nil if not found.
func (tm *TxnManager) GetTransaction(txnID uint64) *Transaction {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.activeTxns[txnID]
}

// IsActive returns true if the given txnID is currently active.
func (tm *TxnManager) IsActive(txnID uint64) bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	_, exists := tm.activeTxns[txnID]
	return exists
}

// ActiveTransactions returns a snapshot of all currently active transactions.
// Used by checkpoint to know which transactions are in-flight.
func (tm *TxnManager) ActiveTransactions() []*Transaction {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	txns := make([]*Transaction, 0, len(tm.activeTxns))
	for _, txn := range tm.activeTxns {
		txns = append(txns, txn)
	}
	return txns
}
