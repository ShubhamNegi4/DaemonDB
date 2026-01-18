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
     ├─→ B+ Tree - Writes INDEX DATA to disk
     │       ↓
	 │   Pager.WritePage() - Disk I/O for index
	 |
	 └─→ WAL - fsync operations to Disk
             ↓
         Replay Logs

*/

import (
	bplus "DaemonDB/bplustree"
	heapfile "DaemonDB/heapfile_manager"
	"DaemonDB/types"
	"DaemonDB/wal_manager"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func NewVM(tree *bplus.BPlusTree, heapFileManager *heapfile.HeapFileManager, walManager *wal_manager.WALManager) *VM {
	return &VM{
		tree:            tree,
		WalManager:      walManager,
		heapfileManager: heapFileManager,

		// 🔹 TRANSACTIONS (NEW)
		TxnManager: NewTxnManager(),

		// existing fields
		stack:           make([][]byte, 0),
		currDb:          "demoDB",
		tableToFileId:   make(map[string]uint32),
		heapFileCounter: 1,
		tableSchemas:    make(map[string]types.TableSchema),
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

			//  TRANSACTIONS (NEW)
		case OP_TXN_BEGIN:
			vm.currentTxn = vm.TxnManager.Begin()

			op := &types.Operation{
				Type:  types.OpTxnBegin,
				TxnID: vm.currentTxn.ID,
			}
			_, err := vm.WalManager.AppendOperation(op)
			if err != nil {
				return err
			}
			if err := vm.WalManager.Sync(); err != nil {
				return err
			}
			return nil

		case OP_TXN_COMMIT:
			if vm.currentTxn == nil {
				return fmt.Errorf("no active transaction")
			}

			op := &types.Operation{
				Type:  types.OpTxnCommit,
				TxnID: vm.currentTxn.ID,
			}
			_, err := vm.WalManager.AppendOperation(op)
			if err != nil {
				return err
			}
			if err := vm.WalManager.Sync(); err != nil {
				return err
			}

			vm.currentTxn = nil
			return nil

		case OP_TXN_ROLLBACK:
			if vm.currentTxn == nil {
				return fmt.Errorf("no active transaction")
			}

			// 1) Log txn abort (same as before)
			op := &types.Operation{
				Type:  types.OpTxnAbort,
				TxnID: vm.currentTxn.ID,
			}
			if _, err := vm.WalManager.AppendOperation(op); err != nil {
				return err
			}
			if err := vm.WalManager.Sync(); err != nil {
				return err
			}

			// 2) Runtime UNDO: undo inserted rows in reverse order
			for i := len(vm.currentTxn.InsertedRows) - 1; i >= 0; i-- {
				ins := vm.currentTxn.InsertedRows[i]

				// (a) delete from heapfile
				rp := ins.RowPtr
				if err := vm.heapfileManager.DeleteRow(&rp); err != nil {
					return fmt.Errorf("rollback heap delete failed (table=%s file=%d page=%d slot=%d): %w",
						ins.Table, rp.FileID, rp.PageNumber, rp.SlotIndex, err)
				}

				// (b) delete from primary index
				btree, err := vm.GetOrCreateIndex(ins.Table)
				if err != nil {
					return fmt.Errorf("rollback index open failed (table=%s): %w", ins.Table, err)
				}
				btree.Delete(ins.PrimaryKey) // Phase 2 made this durable
			}

			// 3) mark txn aborted + clear active txn
			vm.currentTxn.State = TxnAborted
			vm.currentTxn = nil
			return nil

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

	// Pop schema payload (JSON)
	schemaPayload := string(vm.stack[len(vm.stack)-1])
	vm.stack = vm.stack[:len(vm.stack)-1]

	var payload struct {
		Columns     string                `json:"columns"`
		ForeignKeys []types.ForeignKeyDef `json:"foreign_keys"`
	}

	if err := json.Unmarshal([]byte(schemaPayload), &payload); err != nil {
		return fmt.Errorf("invalid table schema payload: %w", err)
	}

	// Parse columns
	colParts := strings.Split(payload.Columns, ",")
	columnDefs := make([]types.ColumnDef, 0, len(colParts))

	for _, col := range colParts {
		colItr := strings.Split(col, ":")
		if len(colItr) < 2 {
			return fmt.Errorf("invalid column format: %s", col)
		}
		isPK := len(colItr) >= 3 && strings.EqualFold(colItr[2], "pk")
		colType := strings.ToUpper(colItr[0])
		if colType == "STRING" {
			colType = "VARCHAR"
		}
		columnDefs = append(columnDefs, types.ColumnDef{
			Name:         colItr[1],
			Type:         colType,
			IsPrimaryKey: isPK,
		})
	}
	// ================= FOREIGN KEY VALIDATION (CREATE TABLE) =================

	// Load already existing table schemas (needed when CREATE TABLE is first command)
	if vm.tableSchemas == nil {
		vm.tableSchemas = make(map[string]types.TableSchema)
	}

	// Validate each foreign key
	for _, fk := range payload.ForeignKeys {

		// 1. Referenced table must exist
		refSchema, ok := vm.tableSchemas[fk.RefTable]
		if !ok {
			return fmt.Errorf(
				"foreign key error: referenced table '%s' does not exist",
				fk.RefTable,
			)
		}

		// 2. FK column must exist in current table
		var fkCol types.ColumnDef
		foundFKCol := false
		for _, c := range columnDefs {
			if strings.EqualFold(c.Name, fk.Column) {
				fkCol = c
				foundFKCol = true
				break
			}
		}
		if !foundFKCol {
			return fmt.Errorf(
				"foreign key error: column '%s' does not exist in table '%s'",
				fk.Column, tableName,
			)
		}

		// 3. Referenced column must exist AND be PRIMARY KEY
		var refPKCol types.ColumnDef
		foundRefPK := false
		for _, c := range refSchema.Columns {
			if strings.EqualFold(c.Name, fk.RefColumn) {
				if !c.IsPrimaryKey {
					return fmt.Errorf(
						"foreign key error: referenced column '%s.%s' is not a PRIMARY KEY",
						fk.RefTable, fk.RefColumn,
					)
				}
				refPKCol = c
				foundRefPK = true
				break
			}
		}
		if !foundRefPK {
			return fmt.Errorf(
				"foreign key error: referenced column '%s.%s' does not exist",
				fk.RefTable, fk.RefColumn,
			)
		}

		// 4. Column types must match
		if !strings.EqualFold(fkCol.Type, refPKCol.Type) {
			return fmt.Errorf(
				"foreign key error: type mismatch (%s.%s is %s, %s.%s is %s)",
				tableName, fk.Column, fkCol.Type,
				fk.RefTable, fk.RefColumn, refPKCol.Type,
			)
		}
	}

	schema := types.TableSchema{
		TableName:   tableName,
		Columns:     columnDefs,
		ForeignKeys: payload.ForeignKeys,
	}
	vm.tableSchemas[tableName] = schema

	// =============== WRITING TO WAL ===============

	op := &types.Operation{
		Type:   types.OpCreateTable,
		Table:  tableName,
		Schema: &schema,
	}
	// fmt.Printf("Created a Create Opertion for WAL %+v", op)

	_, err := vm.WalManager.AppendOperation(op)
	if err != nil {
		return fmt.Errorf("wal append failed: %w", err)
	}

	// WAL is fsynced before applying changes
	if err := vm.WalManager.Sync(); err != nil {
		return fmt.Errorf("wal sync failed: %w", err)
	}

	// =============== WRITING SCHEMA TO DISK ===============

	// Persist schema
	schemaPath := filepath.Join(DB_ROOT, vm.currDb, "tables", tableName+"_schema.json")
	schemaJson, _ := json.MarshalIndent(schema, "", "  ")
	if err := os.WriteFile(schemaPath, schemaJson, 0644); err != nil {
		return fmt.Errorf("cannot write schema: %w", err)
	}

	// Register heap file
	fileID := vm.heapFileCounter
	vm.heapFileCounter++
	vm.tableToFileId[tableName] = fileID

	if err := vm.heapfileManager.CreateHeapfile(tableName, fileID); err != nil {
		return fmt.Errorf("failed to create heap file: %w", err)
	}

	if err := vm.SaveTableFileMapping(); err != nil {
		return fmt.Errorf("failed to save table-fileID mapping: %w", err)
	}

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

	var schema types.TableSchema

	if err := json.Unmarshal(data, &schema); err != nil {
		return fmt.Errorf("invalid schema: %w", err)
	}
	// get the file id, that is reserved for this table
	fileID, ok := vm.tableToFileId[tableName]
	if !ok {
		return fmt.Errorf("heap file not found for table '%s'", tableName)
	}

	columnNames := []types.ColumnDef{}
	// If no column names provided, use all columns from schema
	// for now our insert query doesnt take columns as a token
	if len(columnNames) == 0 {
		for _, col := range schema.Columns {
			columnNames = append(columnNames, types.ColumnDef{Name: col.Name, Type: col.Type, IsPrimaryKey: col.IsPrimaryKey})
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

	// ================= FOREIGN KEY CHECK =================
	for _, fk := range schema.ForeignKeys {

		fkColIdx := -1
		var fkCol types.ColumnDef

		for i, c := range schema.Columns {
			if strings.EqualFold(c.Name, fk.Column) {
				fkColIdx = i
				fkCol = c
				break
			}
		}

		if fkColIdx == -1 {
			return fmt.Errorf("foreign key column %s not found", fk.Column)
		}

		fkValueBytes, err := ValueToBytes(values[fkColIdx], fkCol.Type)
		if err != nil {
			return fmt.Errorf("foreign key value error: %w", err)
		}

		refIndex, err := vm.GetOrCreateIndex(fk.RefTable)
		if err != nil {
			return fmt.Errorf("referenced table %s not found", fk.RefTable)
		}

		refRowPtr, err := refIndex.Search(fkValueBytes)
		if err != nil || refRowPtr == nil {
			return fmt.Errorf(
				"foreign key constraint failed: %s.%s → %s.%s",
				tableName, fk.Column, fk.RefTable, fk.RefColumn,
			)
		}
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

	// ================= TRANSACTION-AWARE WAL INSERT =================

	// If no active transaction, auto-begin one
	if vm.currentTxn == nil {
		vm.currentTxn = vm.TxnManager.Begin()
	}

	// WAL FIRST (logical insert)
	op := &types.Operation{
		Type:    types.OpInsert,
		TxnID:   vm.currentTxn.ID,
		Table:   tableName,
		RowData: row,
	}

	_, err = vm.WalManager.AppendOperation(op)
	if err != nil {
		return fmt.Errorf("wal append failed: %w", err)
	}

	// WAL must be durable BEFORE applying changes
	if err := vm.WalManager.Sync(); err != nil {
		return fmt.Errorf("wal sync failed: %w", err)
	}

	// THEN apply change to heap
	rowPtr, err := vm.heapfileManager.InsertRow(fileID, row)
	if err != nil {
		return fmt.Errorf("heap insert failed: %w", err)
	}

	//  Attach RowPointer to WAL entry (FOR RECOVERY UNDO)
	op.RowPtr = rowPtr
	// fmt.Printf("Inserted into heap (File:%d, Page:%d)\n", rowPtr.FileID, rowPtr.PageNumber)

	/*

		#### B+ Tree writes INDEX DATA to disk (via its pager) ####

	*/

	primaryKeyBytes, _, err := vm.ExtractPrimaryKey(schema, values, rowPtr)
	if err != nil {
		return fmt.Errorf("failed to extract primary key: %w", err)
	}

	// ================= Logical UNDO tracking =================
	if vm.currentTxn != nil {
		vm.currentTxn.InsertedRows = append(
			vm.currentTxn.InsertedRows,
			InsertedRow{
				Table:      tableName,
				RowPtr:     *rowPtr,
				PrimaryKey: primaryKeyBytes,
			},
		)
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

// ExecuteSelect performs a SELECT on a table. It currently supports "SELECT * FROM <table>"
// The argument 'arg' is taken from the instruction.Value (CodeGen may pass either cols or table name).
func (vm *VM) ExecuteSelect(arg string) error {
	// Decode select payload (table + optional WHERE)
	var payload SelectPayload
	if err := json.Unmarshal([]byte(arg), &payload); err != nil {
		return fmt.Errorf("invalid select payload: %w", err)
	}

	if payload.JoinTable != "" {
		return vm.executeSelectWithJoin(payload)
	} else {
		return vm.executeSimpleSelect(payload)
	}
}

func (vm *VM) executeSimpleSelect(payload SelectPayload) error {

	tableName := payload.Table
	if tableName == "" {
		return fmt.Errorf("table name missing in SELECT payload")
	}

	fileID, ok := vm.tableToFileId[tableName]
	if !ok {
		return fmt.Errorf("table '%s' not found in current DB", tableName)
	}

	// Get HeapFile handle
	hf, err := vm.heapfileManager.GetHeapFileByID(fileID)
	if err != nil {
		return fmt.Errorf("failed to get heapfile: %w", err)
	}

	// Read schema for deserialization
	schemaPath := filepath.Join(DB_ROOT, vm.currDb, "tables", tableName+"_schema.json")
	data, err := os.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("failed to read schema for %s: %w", tableName, err)
	}
	var schema types.TableSchema
	if err := json.Unmarshal(data, &schema); err != nil {
		return fmt.Errorf("invalid schema for %s: %w", tableName, err)
	}

	// Print header
	colNames := make([]string, 0, len(schema.Columns))
	for _, c := range schema.Columns {
		colNames = append(colNames, c.Name)
	}
	vm.PrintLine(colNames)
	vm.PrintSeparator(len(colNames))

	// Helper to print a single slice of values
	displayValues := func(values []interface{}) {
		strs := make([]string, len(values))
		for i, v := range values {
			strs[i] = vm.formatValue(v)
		}
		vm.PrintLine(strs)
	}

	// If WHERE on PK provided, use index lookup
	if payload.WhereCol != "" {
		pkColIdx := -1
		var pkCol types.ColumnDef
		for i, c := range schema.Columns {
			if strings.EqualFold(c.Name, payload.WhereCol) && c.IsPrimaryKey {
				pkColIdx = i
				pkCol = c
				break
			}
		}
		if pkColIdx == -1 {
			return fmt.Errorf("WHERE column %s is not a primary key", payload.WhereCol)
		}
		pkBytes, err := ValueToBytes([]byte(payload.WhereVal), pkCol.Type)
		if err != nil {
			return fmt.Errorf("failed to encode WHERE value: %w", err)
		}

		btree, err := vm.GetOrCreateIndex(tableName)
		if err != nil {
			return fmt.Errorf("failed to get index: %w", err)
		}
		rowPtrBytes, err := btree.Search(pkBytes)
		if err != nil {
			return fmt.Errorf("index search failed: %w", err)
		}
		if rowPtrBytes == nil {
			fmt.Println("no rows matched")
			return nil
		}
		rowPtr, err := vm.DeserializeRowPointer(rowPtrBytes)
		if err != nil {
			return fmt.Errorf("failed to decode row pointer: %w", err)
		}
		raw, err := vm.heapfileManager.GetRow(rowPtr)
		if err != nil {
			return fmt.Errorf("error reading row (Page %d Slot %d): %w", rowPtr.PageNumber, rowPtr.SlotIndex, err)
		}
		values, err := vm.DeserializeRow(raw, schema.Columns)
		if err != nil {
			return fmt.Errorf("error deserializing row (Page %d Slot %d): %w", rowPtr.PageNumber, rowPtr.SlotIndex, err)
		}
		displayValues(values)
		return nil
	}

	// No WHERE: full scan
	rowPtrs := hf.GetAllRowPointers()
	if len(rowPtrs) == 0 {
		fmt.Println("table is empty")
		return nil
	}

	// Iterate and print each row using centralized deserializer
	for _, rp := range rowPtrs {
		raw, err := vm.heapfileManager.GetRow(rp)
		if err != nil {
			fmt.Printf("error reading row (Page %d Slot %d): %v\n", rp.PageNumber, rp.SlotIndex, err)
			continue
		}

		values, err := vm.DeserializeRow(raw, schema.Columns)
		if err != nil {
			fmt.Printf("error deserializing row (Page %d Slot %d): %v\n", rp.PageNumber, rp.SlotIndex, err)
			continue
		}

		displayValues(values)
	}

	return nil
}

func (vm *VM) executeSelectWithJoin(payload SelectPayload) error {

	fmt.Println("select with join")

	// table rows are stored as array of mapping
	// where each map key is a string (the tablename.columnname) for all the columns
	// and each value is an interface to store different types of row data

	// loading the left table rows, left table schema
	leftRows, leftSchema, err := vm.loadTableRows(payload.Table)
	if err != nil {
		return fmt.Errorf("failed to load left table: %w", err)
	}

	// loading the right table rows, right table schema
	rightRows, rightSchema, err := vm.loadTableRows(payload.JoinTable)
	if err != nil {
		return fmt.Errorf("failed to load right table: %w", err)
	}

	/*
		sort the rows based on the join payload column (id)
		{"id": 2, "name": "Bob"},
		{"id": 1, "name": "Alice"},
	*/

	// the join may be like id1 = id or table1.id1 = table2.id2
	resolveKey := func(table, col string) string {
		if strings.Contains(col, ".") {
			parts := strings.Split(col, ".")
			if parts[0] != table {
				fmt.Printf("Warning: Table prefix %s does not match %s\n", parts[0], table)
				return table + "." + parts[1]
			}
			return col
		}
		return table + "." + col
	}

	leftKey := resolveKey(payload.Table, payload.LeftCol)
	rightKey := resolveKey(payload.JoinTable, payload.RightCol)

	vm.sortRowsByColumn(leftRows, leftKey)
	vm.sortRowsByColumn(rightRows, rightKey)

	var joinedRows []map[string]interface{}
	switch strings.ToUpper(payload.JoinType) {
	case "INNER", "":
		joinedRows = vm.mergeSortInnerJoin(leftRows, rightRows, leftKey, rightKey)
	case "LEFT":
		joinedRows = vm.mergeSortOuterJoin(leftRows, rightRows, leftKey, rightKey)
	case "RIGHT":
		joinedRows = vm.mergeSortOuterJoin(rightRows, leftRows, rightKey, leftKey)
	case "FULL":
		joinedRows = vm.mergeSortFullJoin(leftRows, rightRows, leftKey, rightKey)
	default:
		return fmt.Errorf("unsupported join type: %s", payload.JoinType)
	}

	fmt.Printf("DEBUG: Joined %d rows before filtering\n", len(joinedRows))

	if payload.WhereCol != "" {
		whereKey := payload.WhereCol
		if !strings.Contains(whereKey, ".") {
			whereKey = payload.Table + "." + payload.WhereCol
		}
		joinedRows = vm.filterJoinedRows(joinedRows, whereKey, payload.WhereVal)

		fmt.Printf("DEBUG: %d rows left after filtering\n", len(joinedRows))
	}

	// displaying the join output
	displayCols := payload.Columns
	if len(displayCols) == 0 || (len(displayCols) == 1 && displayCols[0] == "*") {
		displayCols = []string{}
		for _, col := range leftSchema.Columns {
			displayCols = append(displayCols, payload.Table+"."+col.Name)
		}

		for _, col := range rightSchema.Columns {
			qualified := payload.JoinTable + "." + col.Name
			displayCols = append(displayCols, qualified)
		}
	}

	// Unified Printing
	vm.PrintLine(displayCols)
	vm.PrintSeparator(len(displayCols))
	for _, row := range joinedRows {
		strs := make([]string, len(displayCols))
		for i, col := range displayCols {
			strs[i] = vm.formatValue(row[col])
		}
		vm.PrintLine(strs)
	}
	return nil

}
