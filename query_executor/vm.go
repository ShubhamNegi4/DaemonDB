package executor

/*
VM (VDBE) - Orchestrates operations, does NOT write to disk
    ↓
    ├─→ HeapFileManager - Writes ROW DATA to disk
    ├─→ B+ Tree - Writes INDEX DATA to disk
    └─→ WAL - fsync operations to Disk → Replay Logs
*/

import (
	bplus "DaemonDB/bplustree"
	heapfile "DaemonDB/heapfile_manager"
	"DaemonDB/types"
	"DaemonDB/wal_manager"
	"fmt"
)

func NewVM(tree *bplus.BPlusTree, heapFileManager *heapfile.HeapFileManager, walManager *wal_manager.WALManager) *VM {
	return &VM{
		tree:              tree,
		WalManager:        walManager,
		heapfileManager:   heapFileManager,
		TxnManager:        NewTxnManager(),
		CheckpointManager: nil,
		stack:             make([][]byte, 0),
		currDb:            "demoDB",
		tableToFileId:     make(map[string]uint32),
		heapFileCounter:   1,
		tableSchemas:      make(map[string]types.TableSchema),
		tableIndexCache:   make(map[string]*bplus.BPlusTree),
	}
}

func (vm *VM) Execute(instructions []Instruction) error {
	vm.stack = nil

	for _, instr := range instructions {
		switch instr.Op {
		case OP_PUSH_VAL:
			vm.stack = append(vm.stack, []byte(instr.Value))

		case OP_PUSH_KEY:
			vm.stack = append(vm.stack, []byte(instr.Value))

		case OP_CREATE_DB:
			return vm.ExecuteCreateDatabase(instr.Value)

		case OP_SHOW_DB:
			databases, err := vm.ExecuteShowDatabases()
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("Databases:")
				for _, db := range databases {
					fmt.Printf("  - %s\n", db)
				}
			}
			return nil

		case OP_USE_DB:
			return vm.ExecuteUseDatabase(instr.Value)

		case OP_CREATE_TABLE:
			return vm.ExecuteCreateTable(instr.Value)

		case OP_INSERT:
			return vm.ExecuteInsert(instr.Value)

		case OP_SELECT:
			return vm.ExecuteSelect(instr.Value)

		case OP_UPDATE:
			return vm.ExecuteUpdate(instr.Value)

		case OP_TXN_BEGIN:
			vm.currentTxn = vm.TxnManager.Begin()
			op := &types.Operation{
				Type:  types.OpTxnBegin,
				TxnID: vm.currentTxn.ID,
			}
			_, err := vm.WalManager.AppendOperation(op)
			if err != nil {
				return err
			}
			if err := vm.WalManager.Sync(); err != nil {
				return err
			}
			return nil

		case OP_TXN_COMMIT:
			if vm.currentTxn == nil {
				return fmt.Errorf("no active transaction")
			}
			op := &types.Operation{
				Type:  types.OpTxnCommit,
				TxnID: vm.currentTxn.ID,
			}
			_, err := vm.WalManager.AppendOperation(op)
			if err != nil {
				return err
			}
			if err := vm.WalManager.Sync(); err != nil {
				return err
			}
			vm.currentTxn = nil
			return nil

		case OP_TXN_ROLLBACK:
			if vm.currentTxn == nil {
				return fmt.Errorf("no active transaction")
			}
			op := &types.Operation{
				Type:  types.OpTxnAbort,
				TxnID: vm.currentTxn.ID,
			}
			if _, err := vm.WalManager.AppendOperation(op); err != nil {
				return err
			}
			if err := vm.WalManager.Sync(); err != nil {
				return err
			}
			for i := len(vm.currentTxn.InsertedRows) - 1; i >= 0; i-- {
				ins := vm.currentTxn.InsertedRows[i]
				rp := ins.RowPtr
				if err := vm.heapfileManager.DeleteRow(&rp, op.LSN); err != nil {
					return fmt.Errorf("rollback heap delete failed (table=%s file=%d page=%d slot=%d): %w",
						ins.Table, rp.FileID, rp.PageNumber, rp.SlotIndex, err)
				}
				btree, err := vm.GetOrCreateIndex(ins.Table)
				if err != nil {
					return fmt.Errorf("rollback index open failed (table=%s): %w", ins.Table, err)
				}
				btree.Delete(ins.PrimaryKey)
			}
			vm.currentTxn.State = TxnAborted
			vm.currentTxn = nil
			return nil

		case OP_END:
			return nil

		default:
			return fmt.Errorf("unknown opcode: %d", instr.Op)
		}
	}
	return nil
}
