package txn

import (
	"DaemonDB/types"
	"sync"
)

type TxnState uint8

const (
	TxnActive TxnState = iota
	TxnCommitted
	TxnAborted
)

type Transaction struct {
	ID    uint64
	State TxnState

	// Logical UNDO support
	InsertedRows []InsertedRow
	UpdatedRows  []UpdatedRow
}

type InsertedRow struct {
	Table      string
	RowPtr     types.RowPointer
	PrimaryKey []byte
}

type UpdatedRow struct {
	Table      string
	OldRowPtr  types.RowPointer // location before update (may move on delete+reinsert)
	NewRowPtr  types.RowPointer // location after update
	OldRowData []byte           // serialized old row, used to restore on rollback
	PrimaryKey []byte
}

type TxnManager struct {
	nextID     uint64
	activeTxns map[uint64]*Transaction // all currently active transactions
	mu         sync.RWMutex
}
