package executor

/*
this files executes the code generator output (process the sql statements) based on a large switch case statement on the OpCode in the Execute function
the vdbe like vm is currently a stack based apporach


 ============================================================================
 ARCHITECTURE OVERVIEW
 ============================================================================

 VM (VDBE) - Orchestrates operations, does NOT write to disk
     ↓
     ├─→ HeapFileManager - Writes ROW DATA to disk
     │       ↓
     │   HeapFile.writePage() - Disk I/O for data
     │
     └─→ B+ Tree - Writes INDEX DATA to disk
             ↓
         Pager.WritePage() - Disk I/O for index

*/

import (
	bplus "DaemonDB/bplustree"
	heapfile "DaemonDB/heapfile_manager"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	OP_END
)

type Instruction struct {
	Op    OpCode
	Value string
}

type VM struct {
	tree            *bplus.BPlusTree
	stack           [][]byte
	currDb          string
	heapfileManager *heapfile.HeapFileManager
	tableToFileId   map[string]uint32 // table name to heap file id
	heapFileCounter uint32            // for current db, whats the heap file counter
	tableSchemas    map[string]TableSchema
}

type ColumnDef struct {
	Name         string `json:"name"`
	Type         string `json:"type"`
	IsPrimaryKey bool   `json:"is_primary_key"`
}
type TableSchema struct {
	TableName string      `json:"table_name"`
	Columns   []ColumnDef `json:"columns"`
}

func NewVM(tree *bplus.BPlusTree, heapFileManager *heapfile.HeapFileManager) *VM {
	return &VM{
		tree:            tree,
		stack:           make([][]byte, 0),
		currDb:          "demoDB",
		heapfileManager: heapFileManager,
		tableToFileId:   make(map[string]uint32),
		heapFileCounter: 1,
	}
}

func (vm *VM) Execute(instructions []Instruction) error {

	vm.stack = nil

	for _, instr := range instructions {
		// fmt.Printf("%v --> %v\n", instr.Op, instr.Value)
		switch instr.Op {
		case OP_PUSH_VAL:
			// Push value onto stack
			vm.stack = append(vm.stack, []byte(instr.Value))
			// fmt.Printf("Pushed value: %s (stack size: %d)\n", instr.Value, len(vm.stack))

		case OP_PUSH_KEY:
			vm.stack = append(vm.stack, []byte(instr.Value))
			// fmt.Printf("Pushed key: %s\n", instr.Value)

		case OP_CREATE_DB:
			return vm.ExecuteCreateDatabase(instr.Value)

		case OP_SHOW_DB:
			databases, err := vm.ExecuteShowDatabases()
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("Databases:")
				for _, db := range databases {
					fmt.Printf("  - %s\n", db)
				}
			}
			return nil

		case OP_USE_DB:
			return vm.ExecuteUseDatabase(instr.Value)

		case OP_CREATE_TABLE:
			return vm.ExecuteCreateTable(instr.Value)

		case OP_INSERT:
			return vm.ExecuteInsert(instr.Value)

		case OP_SELECT:
			return vm.ExecuteSelect(instr.Value)

		case OP_END:
			return nil

		default:
			return fmt.Errorf("unknown opcode: %d", instr.Op)
		}
	}
	return nil
}

/*


implementation of functions that vm will execute based on the instruction OpCode


*/

func (vm *VM) ExecuteCreateDatabase(dbName string) error {
	println("make a db with: ", dbName)
	if dbName == "" {
		return fmt.Errorf("database name cannot be empty")
	}
	if err := os.MkdirAll(DB_ROOT, 0755); err != nil {
		return fmt.Errorf("failed to create DB directory: %w", err)
	}

	dbPath := filepath.Join(DB_ROOT, dbName)

	if _, err := os.Stat(dbPath); err == nil {
		return fmt.Errorf("database %s already exists", dbName)
	}
	if err := os.Mkdir(dbPath, 0755); err != nil {
		return fmt.Errorf("failed to create database %s: %w", dbName, err)
	}

	fmt.Printf("Created database directory: %s\n", dbPath)
	return nil
}

func (vm *VM) ExecuteShowDatabases() ([]string, error) {
	entries, err := os.ReadDir(DB_ROOT)
	if err != nil {
		return nil, fmt.Errorf("failed to read DB root directory: %w", err)
	}

	var databases []string

	for _, entry := range entries {
		if entry.IsDir() {
			databases = append(databases, entry.Name())
		}
	}

	return databases, nil
}

func (vm *VM) ExecuteUseDatabase(name string) error {
	if name == "" {
		return fmt.Errorf("database name cannot be empty")
	}

	// DB directory
	dbDir := filepath.Join(DB_ROOT, name)
	tablesDir := filepath.Join(dbDir, "tables")

	// Create it if first time
	if err := os.MkdirAll(tablesDir, 0755); err != nil {
		return err
	}

	// Set current DB in VM
	vm.currDb = name

	// Update heap file base directory
	vm.heapfileManager.UpdateBaseDir(tablesDir)

	// Load table-file-id mapping
	if err := vm.LoadTableFileMapping(); err != nil {
		return err
	}

	// Load table schemas
	if err := vm.LoadAllTableSchemas(); err != nil {
		return err
	}

	// Load heap files for each table
	for tableName, fileID := range vm.tableToFileId {
		if _, err := vm.heapfileManager.LoadHeapFile(fileID, tableName); err != nil {
			return fmt.Errorf("failed to load heapfile for %s: %w", tableName, err)
		}
	}

	return nil
}

func (vm *VM) ExecuteCreateTable(tableName string) error {

	if vm.currDb == "" {
		return fmt.Errorf("no database selected. Run: USE <dbname>")
	}

	// the stack contains the schema of the table seperate by :
	encodedSchema := string(vm.stack[len(vm.stack)-1])
	vm.stack = vm.stack[:len(vm.stack)-1]

	println("schema of table: ", encodedSchema)
	cols := strings.Split(encodedSchema, ",")

	// store the column defination so that it can be converted to JSON later
	columnDefs := make([]ColumnDef, 0, len(cols))
	for _, col := range cols {
		colItr := strings.Split(col, ":")
		if len(colItr) != 2 {
			return fmt.Errorf("invalid column format: %s", col)
		}
		columnDefs = append(columnDefs, ColumnDef{Name: colItr[1], Type: colItr[0]})
	}

	schema := TableSchema{
		TableName: tableName,
		Columns:   columnDefs,
	}

	// inside the current selected db, create a table file

	// TODO: check if the table already exists

	// schema.json stores the schema of the table, data.path will store the data
	schemaPath := filepath.Join(DB_ROOT, vm.currDb, "tables", tableName+"_schema.json")

	// writing on disk in json format for debugging purpose
	// also writing to disk should be atomic, this write can be half done if a power loss occurs in the process
	// TODO: two phase commit protocol + journaling (Redo and Undo logging) will make the writes more secure

	schemaJson, _ := json.MarshalIndent(schema, "", "  ")
	fmt.Println(string(schemaJson))
	if err := os.WriteFile(schemaPath, schemaJson, 0644); err != nil {
		return fmt.Errorf("cannot write schema: %w", err)
	}

	// register the table to the heapfile fileID
	fileID := vm.heapFileCounter
	vm.heapFileCounter++
	vm.tableToFileId[tableName] = fileID

	if err := vm.heapfileManager.CreateHeapfile(tableName, fileID); err != nil {
		// TODO: drop the table
		return fmt.Errorf("failed to create heap file: %w", err)
	}

	if err := vm.SaveTableFileMapping(); err != nil {
		return fmt.Errorf("failed to save table-fileID mapping: %w", err)
	}

	vm.LoadTableFileMapping() // loading the mapping for just created table

	fmt.Printf("Table %s created successfully\n", tableName)

	return nil
}

func (vm *VM) ExecuteInsert(tableName string) error {

	if vm.currDb == "" {
		return fmt.Errorf("no database selected. Run: USE <dbname>")
	}

	// load schema of the table
	schemaPath := filepath.Join(DB_ROOT, vm.currDb, "tables", tableName+"_schema.json")
	data, err := os.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("table not found %s: %w", tableName, err)
	}

	var schema TableSchema

	if err := json.Unmarshal(data, &schema); err != nil {
		return fmt.Errorf("invalid schema: %w", err)
	}
	// get the file id, that is reserved for this table
	fileID, ok := vm.tableToFileId[tableName]
	if !ok {
		return fmt.Errorf("heap file not found for table '%s'", tableName)
	}

	columnNames := []ColumnDef{}
	// If no column names provided, use all columns from schema
	// for now our insert query doesnt take columns as a token
	if len(columnNames) == 0 {
		for _, col := range schema.Columns {
			columnNames = append(columnNames, ColumnDef{col.Name, col.Type, col.IsPrimaryKey})
		}
	}

	if len(vm.stack) < len(schema.Columns) {
		return fmt.Errorf("table schema doesnt match the given query")
	}

	// take all the values that are given in the query
	values := make([]any, len(schema.Columns))
	for i := len(schema.Columns) - 1; i >= 0; i-- {
		if len(vm.stack) == 0 {
			return fmt.Errorf("stack underflow at column %d", i)
		}
		v := vm.stack[len(vm.stack)-1]
		vm.stack = vm.stack[:len(vm.stack)-1]
		values[i] = v
	}

	if len(columnNames) != len(values) {
		return fmt.Errorf("column count (%d) doesn't match value count (%d)",
			len(columnNames), len(values))
	}

	// fmt.Println("Schema columns:", schema.Columns)

	/*

		#### HeapFileManager writes ROW DATA to disk ####

	*/

	row, err := vm.SerializeRow(columnNames, values)
	// fmt.Print("the row after serailization is: ", row)

	if err != nil {
		return err
	}

	// fmt.Print("file Id: ", fileID)
	// fmt.Print("row:  ", string(row))

	rowPtr, err := vm.heapfileManager.InsertRow(fileID, row)
	if err != nil {
		return fmt.Errorf("heap insert failed: %w", err)
	}

	// fmt.Printf("Inserted into heap (File:%d, Page:%d)\n", rowPtr.FileID, rowPtr.PageNumber)

	/*

		#### B+ Tree writes INDEX DATA to disk (via its pager) ####

	*/

	primaryKeyBytes, _, err := vm.ExtractPrimaryKey(schema, values, rowPtr)
	if err != nil {
		return fmt.Errorf("failed to extract primary key: %w", err)
	}

	rowPtrBytes := vm.SerializeRowPointer(rowPtr)

	// Get B+ tree index
	// VM will read the exisiting bplus tree created for this current table
	btree, err := vm.GetOrCreateIndex(tableName)
	if err != nil {
		return fmt.Errorf("failed to get index: %w", err)
	}

	// B+ tree insertion - B+ TREE'S PAGER WRITES TO DISK, NOT VM
	btree.Insertion(primaryKeyBytes, rowPtrBytes)

	// fmt.Printf("Indexed in B+ tree (key_column: %s)\n", pkColumnName)

	return nil
}

func (vm *VM) ExecuteSelect(tableName string) error {
	if vm.currDb == "" {
		return fmt.Errorf("no database selected. Run: USE <dbname>")
	}

	// fmt.Printf("SELECT %s ", vm.stack)
	// fmt.Println("FROM", tableName)

	schemaPath := filepath.Join(DB_ROOT, vm.currDb, "tables", tableName+"_schema.json")
	data, err := os.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("table not found: %s", tableName)
	}

	var schema TableSchema
	if err := json.Unmarshal(data, &schema); err != nil {
		return fmt.Errorf("invalid schema: %w", err)
	}

	fileID, ok := vm.tableToFileId[tableName]
	if !ok {
		return fmt.Errorf("table '%s' not found", tableName)
	}

	// fmt.Print("fileID: ", fileID)

	heapFile, err := vm.heapfileManager.GetHeapFileByID(fileID)
	if err != nil {
		return err
	}
	rowPtrs := heapFile.GetAllRowPointers()
	if len(rowPtrs) == 0 {
		fmt.Println("table is empty")
		return nil
	}

	// fmt.Printf("scanning %d rows...\n", len(rowPtrs))

	fmt.Printf("\n\n                Table: %s\n\n", tableName)
	vm.PrintTableHeader(schema.Columns)

	for _, ptr := range rowPtrs {
		rowBytes, err := vm.heapfileManager.GetRow(ptr)
		if err != nil {
			fmt.Println("error reading row:", err)
			continue
		}

		rowValues, err := vm.DeserializeRow(rowBytes, schema.Columns)
		if err != nil {
			fmt.Println("error deserializing row:", err)
			continue
		}

		vm.PrintTableRow(rowValues, schema.Columns)
	}

	return nil
}
