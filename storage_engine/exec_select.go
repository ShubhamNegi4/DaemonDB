package storageengine

import (
	"fmt"
	"strings"

	"DaemonDB/types"
)

/*
ExecuteSelect executes a SELECT query and returns rows as maps.
It works for both normal select query (with or without a filter) and also for select join queries

Returns:

	rows: slice of row maps (keys are qualified column names: "table.column")
	columns: ordered list of column names to display


	SQL: SELECT * FROM mytable WHERE id = 5
	     ↓
	StorageEngine.ExecuteSelect
	     ├── [PK column] → BTree.Search(pkBytes) → rowPtrBytes
	     │       └── HeapManager.GetRow(rowPtr) → rowBytes → deserialize → result
	     └── [non-PK column] → GetAllRowPointers → for each: GetRow → filter → result
*/
func (se *StorageEngine) ExecuteSelect(payload types.SelectPayload) ([]map[string]interface{}, []string, error) {
	if payload.JoinTable != "" {
		return se.executeSelectWithJoin(payload)
	}
	return se.executeSimpleSelect(payload)
}

// executeSimpleSelect handles single-table SELECT.
func (se *StorageEngine) executeSimpleSelect(payload types.SelectPayload) ([]map[string]interface{}, []string, error) {
	tableName := payload.Table
	if tableName == "" {
		return nil, nil, fmt.Errorf("table name missing in SELECT payload")
	}

	// ── Step 1: Get schema ────────────────────────────────────────────────────
	schema, err := se.CatalogManager.GetTableSchema(tableName)
	if err != nil {
		return nil, nil, fmt.Errorf("table '%s' not found: %w", tableName, err)
	}

	// ── Step 2: Determine columns to return ──────────────────────────────────
	columns := make([]string, 0, len(schema.Columns))
	for _, col := range schema.Columns {
		columns = append(columns, col.Name)
	}

	// ── Step 3: WHERE clause with primary key (index lookup) ─────────────────
	if payload.WhereCol != "" {
		// Find the PK column in schema.
		pkColIdx := -1
		var pkCol types.ColumnDef
		for i, col := range schema.Columns {
			if strings.EqualFold(col.Name, payload.WhereCol) && col.IsPrimaryKey {
				pkColIdx = i
				pkCol = col
				break
			}
		}

		if pkColIdx != -1 {
			fmt.Print("pk lookup\n")
			return se.selectWithPKLookup(tableName, schema, payload, columns, pkCol)
		}
		fmt.Print("full scan lookup\n")
		// Non-PK WHERE — full scan with filter.
		return se.selectFullScanWithFilter(tableName, schema, payload, columns)
	}

	// ── Step 4: Full table scan ──────────────────────────────────────────────
	return se.selectFullScan(tableName, schema, columns)
}

// selectWithPKLookup performs a point lookup via the primary key index.
func (se *StorageEngine) selectWithPKLookup(tableName string, schema types.TableSchema, payload types.SelectPayload, columns []string, pkCol types.ColumnDef) ([]map[string]interface{}, []string, error) {

	// Encode the WHERE value as bytes.
	pkBytes, err := ValueToBytes([]byte(payload.WhereVal), pkCol.Type)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to encode WHERE value: %w", err)
	}

	// Look up in the index.
	btree, err := se.getIndex(tableName)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get index: %w", err)
	}

	rowPtrBytes, err := btree.Search(pkBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("index search failed: %w", err)
	}
	if rowPtrBytes == nil {
		// Not found — return empty result.
		return []map[string]interface{}{}, columns, nil
	}

	fmt.Println("[B+ Tree Search for PkBytes]")
	// Deserialize RowPointer.
	rowPtr, err := se.DeserializeRowPointer(rowPtrBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to decode row pointer: %w", err)
	}

	rawRow, err := se.HeapManager.GetRow(&rowPtr)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read row: %w", err)
	}

	// Deserialize the row.
	values, err := se.DeserializeRow(rawRow, schema.Columns)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to deserialize row: %w", err)
	}

	// Build the result map.
	rowMap := make(map[string]interface{})
	for i, col := range schema.Columns {
		rowMap[col.Name] = values[i]
	}

	return []map[string]interface{}{rowMap}, columns, nil
}

// selectFullScan scans all rows in the table.
func (se *StorageEngine) selectFullScan(tableName string, schema types.TableSchema, columns []string) ([]map[string]interface{}, []string, error) {
	hf, err := se.HeapManager.GetHeapFileByTable(tableName)
	if err != nil {
		return nil, nil, fmt.Errorf("heap file not found: %w", err)
	}

	rowPtrs := hf.GetAllRowPointers()
	if len(rowPtrs) == 0 {
		return []map[string]interface{}{}, columns, nil
	}

	rows := make([]map[string]interface{}, 0, len(rowPtrs))
	for _, rp := range rowPtrs {
		rawRow, err := se.HeapManager.GetRow(&rp)
		if err != nil {
			// Skip corrupted rows.
			continue
		}

		values, err := se.DeserializeRow(rawRow, schema.Columns)
		if err != nil {
			continue
		}

		rowMap := make(map[string]interface{})
		for i, col := range schema.Columns {
			rowMap[col.Name] = values[i]
		}
		rows = append(rows, rowMap)
	}

	return rows, columns, nil
}

func (se *StorageEngine) selectFullScanWithFilter(tableName string, schema types.TableSchema, payload types.SelectPayload, columns []string) ([]map[string]interface{}, []string, error) {

	// Find WHERE column in schema for type info.
	var whereCol *types.ColumnDef
	for _, col := range schema.Columns {
		if strings.EqualFold(col.Name, payload.WhereCol) {
			c := col
			whereCol = &c
			break
		}
	}
	if whereCol == nil {
		return nil, nil, fmt.Errorf("column '%s' not found in table '%s'", payload.WhereCol, tableName)
	}

	// Get heap file — same as selectFullScan.
	hf, err := se.HeapManager.GetHeapFileByTable(tableName)
	if err != nil {
		return nil, nil, fmt.Errorf("heap file not found: %w", err)
	}

	rowPtrs := hf.GetAllRowPointers()
	rows := make([]map[string]interface{}, 0)

	for _, rp := range rowPtrs {
		rawRow, err := se.HeapManager.GetRow(&rp)
		if err != nil {
			continue
		}

		values, err := se.DeserializeRow(rawRow, schema.Columns)
		if err != nil {
			continue
		}

		// Find WHERE column value and compare.
		for i, col := range schema.Columns {
			if strings.EqualFold(col.Name, payload.WhereCol) {
				if fmt.Sprintf("%v", values[i]) == payload.WhereVal {
					rowMap := make(map[string]interface{})
					for j, c := range schema.Columns {
						rowMap[c.Name] = values[j]
					}
					rows = append(rows, rowMap)
				}
				break
			}
		}
	}

	return rows, columns, nil
}

// executeSelectWithJoin handles JOIN queries.
func (se *StorageEngine) executeSelectWithJoin(payload types.SelectPayload) ([]map[string]interface{}, []string, error) {
	// Load left table.
	leftRows, leftSchema, err := se.loadTableRows(payload.Table)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load left table: %w", err)
	}

	// Load right table.
	rightRows, rightSchema, err := se.loadTableRows(payload.JoinTable)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load right table: %w", err)
	}

	// Resolve column keys (prefix with table name if not already qualified).
	resolveKey := func(table, col string) string {
		if strings.Contains(col, ".") {
			return col
		}
		return table + "." + col
	}

	leftKey := resolveKey(payload.Table, payload.LeftCol)
	rightKey := resolveKey(payload.JoinTable, payload.RightCol)

	// Sort both sides by join key.
	se.sortRowsByColumn(leftRows, leftKey)
	se.sortRowsByColumn(rightRows, rightKey)

	// Perform the join.
	var joinedRows []map[string]interface{}
	switch strings.ToUpper(payload.JoinType) {
	case "INNER", "":
		joinedRows = se.mergeSortInnerJoin(leftRows, rightRows, leftKey, rightKey)
	case "LEFT":
		joinedRows = se.mergeSortOuterJoin(leftRows, rightRows, leftKey, rightKey)
	case "RIGHT":
		joinedRows = se.mergeSortOuterJoin(rightRows, leftRows, rightKey, leftKey)
	case "FULL":
		joinedRows = se.mergeSortFullJoin(leftRows, rightRows, leftKey, rightKey)
	default:
		return nil, nil, fmt.Errorf("unsupported join type: %s", payload.JoinType)
	}

	// Apply WHERE filter if present.
	if payload.WhereCol != "" {
		whereKey := payload.WhereCol
		if !strings.Contains(whereKey, ".") {
			whereKey = payload.Table + "." + payload.WhereCol
		}
		joinedRows = se.filterJoinedRows(joinedRows, whereKey, payload.WhereVal)
	}

	// Determine display columns.
	displayCols := payload.Columns
	if len(displayCols) == 0 || (len(displayCols) == 1 && displayCols[0] == "*") {
		displayCols = []string{}
		for _, col := range leftSchema.Columns {
			displayCols = append(displayCols, payload.Table+"."+col.Name)
		}
		for _, col := range rightSchema.Columns {
			displayCols = append(displayCols, payload.JoinTable+"."+col.Name)
		}
	}

	return joinedRows, displayCols, nil
}

// loadTableRows loads all rows from a table as maps.
func (se *StorageEngine) loadTableRows(tableName string) ([]map[string]interface{}, types.TableSchema, error) {
	schema, err := se.CatalogManager.GetTableSchema(tableName)
	if err != nil {
		return nil, types.TableSchema{}, err
	}

	hf, err := se.HeapManager.GetHeapFileByTable(tableName)
	if err != nil {
		return nil, types.TableSchema{}, err
	}

	rowPtrs := hf.GetAllRowPointers()
	rows := make([]map[string]interface{}, 0, len(rowPtrs))

	for _, rp := range rowPtrs {
		rawRow, err := se.HeapManager.GetRow(&rp)
		if err != nil {
			continue
		}

		values, err := se.DeserializeRow(rawRow, schema.Columns)
		if err != nil {
			continue
		}

		rowMap := make(map[string]interface{})
		for i, col := range schema.Columns {
			// Qualify column names with table name for join.
			rowMap[tableName+"."+col.Name] = values[i]
		}
		rows = append(rows, rowMap)
	}

	return rows, schema, nil
}
