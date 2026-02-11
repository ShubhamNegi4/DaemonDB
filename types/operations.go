package types

import (
	heapfile "DaemonDB/heapfile_manager"
	"encoding/json"
)

type OperationType byte

const (
	// Existing operations (UNCHANGED VALUES)
	OpInsert      OperationType = 1
	OpUpdate      OperationType = 2
	OpDelete      OperationType = 3
	OpCreateTable OperationType = 4

	// Transaction operations (NEW)
	OpTxnBegin  OperationType = 5
	OpTxnCommit OperationType = 6
	OpTxnAbort  OperationType = 7

	// DDL (for WAL replay; VM will log these when DROP is implemented)
	OpDrop OperationType = 8
)

type Operation struct {
	Type  OperationType `json:"type"`
	TxnID uint64        `json:"txn_id,omitempty"`

	// DML
	Table   string               `json:"table,omitempty"`
	RowData []byte               `json:"row_data,omitempty"`
	RowPtr  *heapfile.RowPointer `json:"row_ptr,omitempty"` // ✅ ADD THIS

	// DDL
	Schema *TableSchema `json:"schema,omitempty"`
}

func (op *Operation) Encode() []byte {
	data, _ := json.Marshal(op)
	return data
}
