package types

import "encoding/json"

type OperationType byte

const (
	OpInsert      OperationType = 1
	OpUpdate      OperationType = 2
	OpDelete      OperationType = 3
	OpCreateTable OperationType = 4
)

type Operation struct {
	Type    OperationType `json:"type"`
	Table   string        `json:"table"`
	RowData []byte        `json:"row_data,omitempty"`
	Schema  *TableSchema  `json:"schema,omitempty"`
}

func (op *Operation) Encode() []byte {
	data, _ := json.Marshal(op)
	return data
}
