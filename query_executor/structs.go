package executor

import (
	bplus "DaemonDB/bplustree"
	heapfile "DaemonDB/heapfile_manager"
	"DaemonDB/types"
	"DaemonDB/wal_manager"
	"sync"
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

	TxnManager *TxnManager
	currentTxn *Transaction

	stack           [][]byte
	currDb          string
	tableToFileId   map[string]uint32 // table name to heap file id
	heapFileCounter uint32
	tableSchemas    map[string]types.TableSchema

	// Per-table B+ tree index cache (avoids reopening the same .idx file).
	// Cleared and closed when switching DB or on VM shutdown.
	indexCacheMu   sync.RWMutex
	tableIndexCache map[string]*bplus.BPlusTree
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
