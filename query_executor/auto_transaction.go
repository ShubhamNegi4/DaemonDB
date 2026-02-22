package executor

import (
	"fmt"
)

/*
This file contains functions that are required for starting automatic transactions
Called when the user executes INSERT/UPDATE/DELETE without an explicit BEGIN/COMMIT/ABORT.
*/

// autoTransactionBegin starts an implicit transaction for a single statement.
func (vm *VM) autoTransactionBegin() error {
	txn, err := vm.storageEngine.BeginTransaction()

	if err != nil {
		return fmt.Errorf("failed to begin txn: %w", err)
	}

	vm.currentTxn = txn
	vm.autoTxn = true

	return nil
}

// autoTransactionCommit commits an implicit transaction.
// Called after a single statement succeeds.
func (vm *VM) autoTransactionCommit() error {
	if !vm.autoTxn || vm.currentTxn == nil {
		return fmt.Errorf("autoTransactionCommit: no active transaction")
	}

	txnID := vm.currentTxn.ID

	// Mark transaction as committed.
	// This removes it from the TxnManager's active set.
	if err := vm.storageEngine.CommitTransaction(txnID); err != nil {
		return err
	}

	// Clear VM transaction state.
	vm.currentTxn = nil
	vm.autoTxn = false

	// Optionally trigger a checkpoint.
	// In production, checkpoint periodically (every N txns or N bytes),
	// not after every single transaction.
	_ = vm.storageEngine.SaveCheckpoint()

	return nil
}

// autoTransactionAbort aborts an implicit transaction.
// Called when a single statement fails.
func (vm *VM) autoTransactionAbort() error {
	if vm.currentTxn == nil {
		vm.autoTxn = false
		return nil
	}

	// Physically undoes all writes (heap + index) then marks aborted in TxnManager
	if err := vm.storageEngine.AbortTransaction(vm.currentTxn); err != nil {
		return err
	}

	vm.currentTxn = nil
	vm.autoTxn = false
	return nil
}
