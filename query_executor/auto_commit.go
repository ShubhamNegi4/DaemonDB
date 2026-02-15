package executor

import (
	"DaemonDB/types"
	"fmt"
)

func (vm *VM) autoTransactionBegin() error {
	// Auto Transaction for Update Command
	vm.currentTxn = vm.TxnManager.Begin()
	vm.autoTxn = true
	// Log transaction begin to WAL
	op := &types.Operation{
		Type:  types.OpTxnBegin,
		TxnID: vm.currentTxn.ID,
	}
	if _, err := vm.WalManager.AppendOperation(op); err != nil {
		return fmt.Errorf("failed to log transaction begin: %w", err)
	}
	if err := vm.WalManager.Sync(); err != nil {
		return fmt.Errorf("failed to sync transaction begin: %w", err)
	}
	return nil
}

func (vm *VM) autoTransactionEnd() error {
	op := &types.Operation{
		Type:  types.OpTxnCommit,
		TxnID: vm.currentTxn.ID,
	}
	if _, err := vm.WalManager.AppendOperation(op); err != nil {
		return fmt.Errorf("auto-commit failed: %w", err)
	}
	if err := vm.WalManager.Sync(); err != nil {
		return fmt.Errorf("auto-commit sync failed: %w", err)
	}

	vm.SaveCheckpoint() // save checkpoint for this auto commit

	vm.autoTxn = false
	vm.currentTxn = nil // Clear transaction
	return nil
}
