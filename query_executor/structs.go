package executor

import (
	storageengine "DaemonDB/storage_engine"
	txn "DaemonDB/storage_engine/transaction_manager"
)

type OpCode byte

const (
	// stack
	OP_PUSH_VAL OpCode = iota
	OP_PUSH_KEY

	// sql command
	OP_CREATE_DB
	OP_SHOW_DB
	OP_USE_DB
	OP_CREATE_TABLE
	OP_INSERT
	OP_SELECT
	OP_UPDATE
	OP_ADD
	OP_SUB
	OP_MUL
	OP_DIV

	//  TRANSACTIONS (NEW)
	OP_TXN_BEGIN
	OP_TXN_COMMIT
	OP_TXN_ROLLBACK

	OP_END
)

type Instruction struct {
	Op    OpCode
	Value string
}

type VM struct {
	storageEngine *storageengine.StorageEngine

	currentTxn *txn.Transaction
	autoTxn    bool

	stack [][]byte
}
