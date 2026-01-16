package parser

// Statement is a generic interface for all statements
type Statement interface{}

// CREATE DATABASE statement
type CreateDatabaseStmt struct {
	DbName string
}

// SHOW DATABASE statement
type ShowDatabasesStmt struct {
}

// USE DATABASE statement
type UseDatabaseStatement struct {
	DbName string
}

// SELECT statement
type SelectStmt struct {
	Columns    []string
	Table      string
	WhereCol   string
	WhereValue string

	// join
	JoinType  string
	JoinTable string
	LeftCol   string
	Rightcol  string
}

// CREATE TABLE statement
type CreateTableStmt struct {
	TableName   string
	Columns     []ColumnDef
	ForeignKeys []ForeignKeyDef
}

type ColumnDef struct {
	Name         string `json:"name"`
	Type         string `json:"type"`
	IsPrimaryKey bool   `json:"is_primary_key"`
}

// For foreign key
type ForeignKeyDef struct {
	Column    string `json:"column"`     // child column
	RefTable  string `json:"ref_table"`  // parent table
	RefColumn string `json:"ref_column"` // parent PK column
}

// INSERT statement
type InsertStmt struct {
	Table  string
	Values []string
}

// DROP statement
type DropStmt struct {
	Table string
}

// UPDATE statement
type UpdateStmt struct {
	Table       string
	Assignments map[string]string
}

// TRANSACTION statements

type BeginTxnStmt struct{}

type CommitTxnStmt struct{}

type RollbackTxnStmt struct{}
