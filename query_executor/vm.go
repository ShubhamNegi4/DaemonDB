package executor

/*
 VM (VDBE) - Orchestrates operations, does NOT write to disk
    ↓
    ├─→ StorageEngine - Coordinates heap, index, WAL, catalog, txn
    │       ├─→ HeapFileManager  - Row storage (insert/get/update/delete)
    │       ├─→ IndexFileManager - B+ tree primary key index
    │       ├─→ WALManager       - Write-ahead log (crash recovery)
    │       ├─→ CatalogManager   - Schema + file ID metadata
    │       └─→ TxnManager       - Transaction lifecycle
    ↓
    DiskManager  - OS file handles, global↔local page ID mapping
    BufferPool   - Page cache, pinning, LRU eviction, dirty flushing


	The virtual machine executes bytecode instructions compiled from parsed SQL.
	It does not touch disk directly — all persistence goes through the StorageEngine.

*/

import (
	storageengine "DaemonDB/storage_engine"
	"fmt"
)

/*
This file is the main start of the VM
It has the Execute function which has a Switch Case based on which the Instruction are decided to be executed
*/

func NewVM(engine *storageengine.StorageEngine) *VM {
	return &VM{
		storageEngine: engine,
		stack:         make([][]byte, 0),
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
			t, err := vm.storageEngine.BeginTransaction()
			if err != nil {
				return fmt.Errorf("BEGIN failed: %w", err)
			}
			vm.currentTxn = t
			return nil

		case OP_TXN_COMMIT:
			if vm.currentTxn == nil {
				return fmt.Errorf("no active transaction")
			}
			if err := vm.storageEngine.CommitTransaction(vm.currentTxn.ID); err != nil {
				return fmt.Errorf("COMMIT failed: %w", err)
			}
			vm.currentTxn = nil
			return nil

		case OP_TXN_ROLLBACK:
			if vm.currentTxn == nil {
				return fmt.Errorf("no active transaction")
			}
			if err := vm.storageEngine.AbortTransaction(vm.currentTxn); err != nil {
				return fmt.Errorf("ROLLBACK failed: %w", err)
			}
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
