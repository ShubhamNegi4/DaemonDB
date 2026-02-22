package types

import (
	"encoding/json"
)

type OperationType byte

const (
	OpInsert      OperationType = 1
	OpUpdate      OperationType = 2
	OpDelete      OperationType = 3
	OpCreateTable OperationType = 4

	OpTxnBegin  OperationType = 5
	OpTxnCommit OperationType = 6
	OpTxnAbort  OperationType = 7
	OpAbort     OperationType = 8

	// DDL (for WAL replay; VM will log these when DROP is implemented)
	OpDrop OperationType = 9
)

type Operation struct {
	Type  OperationType `json:"type"`
	TxnID uint64        `json:"txn_id,omitempty"`

	LSN       uint64
	TargetLSN uint64

	// DML
	Table   string     `json:"table,omitempty"`
	RowData []byte     `json:"row_data,omitempty"`
	RowPtr  RowPointer `json:"row_ptr,omitempty"`
	OldPtr  RowPointer `json:"old_ptr,omitempty"`

	// DDL
	Schema *TableSchema `json:"schema,omitempty"`
}

func (op *Operation) Encode() []byte {
	data, _ := json.Marshal(op)
	return data
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

type UpdatePayload struct {
	Table     string                    `json:"table"`
	SetExprs  map[string]ExpressionNode `json:"set_exprs"`
	WhereExpr *ExpressionNode           `json:"where_expr,omitempty"`
}

// ExpressionNode represents an expression tree for evaluation
type ExpressionNode struct {
	Type    int             `json:"type"` // 0=LITERAL, 1=COLUMN, 2=BINARY, 3=COMPARISON
	Literal interface{}     `json:"literal,omitempty"`
	Column  string          `json:"column,omitempty"`
	Op      string          `json:"op,omitempty"`
	Left    *ExpressionNode `json:"left,omitempty"`
	Right   *ExpressionNode `json:"right,omitempty"`
}
