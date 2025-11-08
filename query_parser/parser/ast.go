package parser

// Statement is a generic interface for all statements
type Statement interface{}

type CreateDatabaseStmt struct {
	DbName string
}

type ShowDatabasesStmt struct {
}

// SELECT statement
type SelectStmt struct {
	Columns []string
	Table   string
}

// CREATE TABLE statement
type CreateTableStmt struct {
	TableName string
	Columns   []ColumnDef
}

type ColumnDef struct {
	Name string
	Type string
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
