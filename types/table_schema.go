package types

type ColumnDef struct {
	Name         string `json:"name"`
	Type         string `json:"type"`
	IsPrimaryKey bool   `json:"is_primary_key"`
}

type ForeignKeyDef struct {
	Column    string `json:"column"`
	RefTable  string `json:"ref_table"`
	RefColumn string `json:"ref_column"`
}

type TableSchema struct {
	TableName   string          `json:"table_name"`
	Columns     []ColumnDef     `json:"columns"`
	ForeignKeys []ForeignKeyDef `json:"foreign_keys,omitempty"`
}
