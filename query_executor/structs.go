package executor

import (
	bplus "DaemonDB/bplustree"
	heapfile "DaemonDB/heapfile_manager"
	"DaemonDB/types"
	"DaemonDB/wal_manager"
)

const DB_ROOT = "./databases" // all databases stored here

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
	tree            *bplus.BPlusTree
	WalManager      *wal_manager.WALManager
	heapfileManager *heapfile.HeapFileManager

	// 🔹 TRANSACTIONS (NEW)
	TxnManager *TxnManager
	currentTxn *Transaction

	// existing fields
	stack           [][]byte
	currDb          string
	tableToFileId   map[string]uint32 // table name to heap file id
	heapFileCounter uint32            // for current db, whats the heap file counter
	tableSchemas    map[string]types.TableSchema
}

type SelectPayload struct {
	Table     string   `json:"table"`
	Columns   []string `json:"columns"`
	WhereCol  string   `json:"where_col,omitempty"`
	WhereVal  string   `json:"where_val,omitempty"`
	JoinTable string   `json:"join_table,omitempty"`
	JoinType  string   `json:"join_type,omitempty"`
	LeftCol   string   `json:"left_col,omitempty"`
	RightCol  string   `json:"right_col,omitempty"`
}
