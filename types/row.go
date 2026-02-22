package types

import "strings"

type Row struct {
	Values map[string]interface{}
}

// RowPointer points to a specific row in a heap file
type RowPointer struct {
	FileID     uint32 `json:"file_id"`
	PageNumber uint32 `json:"page_number"`
	SlotIndex  uint16 `json:"slot_index"` // Index in the slot directory
}

type RowWithPointer struct {
	Pointer RowPointer
	Row     Row
}

func (r *Row) Set(column string, value interface{}) {
	r.Values[strings.ToLower(column)] = value
}

func (r *Row) ToMap() map[string]interface{} {
	return r.Values
}

func (r *RowWithPointer) ToMap() map[string]interface{} {
	return r.Row.Values
}

func (r *Row) Clone() Row {
	newMap := make(map[string]interface{})
	for k, v := range r.Values {
		newMap[k] = v
	}
	return Row{Values: newMap}
}
