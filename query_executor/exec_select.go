package executor

import (
	"DaemonDB/types"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func (vm *VM) ExecuteSelect(arg string) error {

	if err := vm.RequireDatabase(); err != nil {
		return fmt.Errorf("no database selected. Run: USE <dbname>")
	}

	var payload SelectPayload
	if err := json.Unmarshal([]byte(arg), &payload); err != nil {
		return fmt.Errorf("invalid select payload: %w", err)
	}

	if payload.JoinTable != "" {
		return vm.executeSelectWithJoin(payload)
	}
	return vm.executeSimpleSelect(payload)
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

	hf, err := vm.heapfileManager.GetHeapFileByID(fileID)
	if err != nil {
		return fmt.Errorf("failed to get heapfile: %w", err)
	}

	schemaPath := filepath.Join(DB_ROOT, vm.currDb, "tables", tableName+"_schema.json")
	data, err := os.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("failed to read schema for %s: %w", tableName, err)
	}
	var schema types.TableSchema
	if err := json.Unmarshal(data, &schema); err != nil {
		return fmt.Errorf("invalid schema for %s: %w", tableName, err)
	}

	colNames := make([]string, 0, len(schema.Columns))
	for _, c := range schema.Columns {
		colNames = append(colNames, c.Name)
	}
	vm.PrintLine(colNames)
	vm.PrintSeparator(len(colNames))

	displayValues := func(values []interface{}) {
		strs := make([]string, len(values))
		for i, v := range values {
			strs[i] = vm.formatValue(v)
		}
		vm.PrintLine(strs)
	}

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

	rowPtrs := hf.GetAllRowPointers()
	if len(rowPtrs) == 0 {
		fmt.Println("table is empty")
		return nil
	}

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
	leftRows, leftSchema, err := vm.loadTableRows(payload.Table)
	if err != nil {
		return fmt.Errorf("failed to load left table: %w", err)
	}

	rightRows, rightSchema, err := vm.loadTableRows(payload.JoinTable)
	if err != nil {
		return fmt.Errorf("failed to load right table: %w", err)
	}

	resolveKey := func(table, col string) string {
		if strings.Contains(col, ".") {
			parts := strings.Split(col, ".")
			if parts[0] != table {
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

	if payload.WhereCol != "" {
		whereKey := payload.WhereCol
		if !strings.Contains(whereKey, ".") {
			whereKey = payload.Table + "." + payload.WhereCol
		}
		joinedRows = vm.filterJoinedRows(joinedRows, whereKey, payload.WhereVal)
	}

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
