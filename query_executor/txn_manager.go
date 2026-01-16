package executor

import heapfile "DaemonDB/heapfile_manager"

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
}

type InsertedRow struct {
	Table      string
	RowPtr     heapfile.RowPointer
	PrimaryKey []byte
}

type TxnManager struct {
	nextID uint64
}

func NewTxnManager() *TxnManager {
	return &TxnManager{
		nextID: 1,
	}
}

func (tm *TxnManager) Begin() *Transaction {
	txn := &Transaction{
		ID:    tm.nextID,
		State: TxnActive,
	}
	tm.nextID++
	return txn
}
