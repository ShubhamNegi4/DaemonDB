package executor

import (
	bplus "DaemonDB/bplustree"
	heapfile "DaemonDB/heapfile_manager"
	"DaemonDB/types"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

/*

********************************* TYPE CONVERSION HELPERS *********************************

 */

func toInt(v any) (int32, error) {
	switch x := v.(type) {
	case int:
		return int32(x), nil
	case int32:
		return x, nil
	case int64:
		return int32(x), nil
	case float64:
		return int32(x), nil
	case float32:
		return int32(x), nil
	case string:
		i, err := strconv.Atoi(x)
		if err != nil {
			return 0, fmt.Errorf("cannot convert %q to int", x)
		}
		return int32(i), nil
	case []byte:
		s := strings.TrimSpace(string(x))
		i, err := strconv.Atoi(s)
		if err != nil {
			return 0, fmt.Errorf("cannot convert %q to int", s)
		}
		return int32(i), nil
	default:
		return 0, fmt.Errorf("expected int, got %T", v)
	}
}

func toString(v any) (string, error) {
	switch x := v.(type) {
	case string:
		return x, nil
	case []byte:
		return strings.TrimSpace(string(x)), nil
	case int, int32, int64:
		return fmt.Sprintf("%d", x), nil
	case float32, float64:
		return fmt.Sprintf("%g", x), nil
	default:
		return "", fmt.Errorf("expected string, got %T", v)
	}
}

func toFloat(v any) (float32, error) {
	switch x := v.(type) {

	case float64:
		return float32(x), nil

	case float32:
		return x, nil

	case int:
		return float32(x), nil

	case int32:
		return float32(x), nil

	case int64:
		return float32(x), nil

	case string:
		f, err := strconv.ParseFloat(x, 32)
		if err != nil {
			return 0, fmt.Errorf("cannot convert %q to float", x)
		}
		return float32(f), nil

	case []byte:
		s := strings.TrimSpace(string(x))
		f, err := strconv.ParseFloat(s, 32)
		if err != nil {
			return 0, fmt.Errorf("cannot convert %q to float", s)
		}
		return float32(f), nil

	default:
		return 0, fmt.Errorf("expected float, got %T", v)
	}
}

func isInteger(v reflect.Value) bool {
	kind := v.Kind()
	return kind >= reflect.Int && kind <= reflect.Int64
}

func isFloat(v reflect.Value) bool {
	kind := v.Kind()
	return kind == reflect.Float32 || kind == reflect.Float64
}

func compareValues(v1, v2 interface{}) int {
	if v1 == nil || v2 == nil {
		if v1 == v2 {
			return 0
		}
		if v1 == nil {
			return -1
		}
		return 1
	}

	// Use reflection to handle various numeric types (int32, int64, float64, etc.)
	val1 := reflect.ValueOf(v1)
	val2 := reflect.ValueOf(v2)

	switch {
	case isInteger(val1) && isInteger(val2):
		i1, i2 := val1.Int(), val2.Int()
		if i1 < i2 {
			return -1
		}
		if i1 > i2 {
			return 1
		}
		return 0
	case isFloat(val1) || isFloat(val2):
		// Promote to float if either is a float
		var f1, f2 float64
		if isFloat(val1) {
			f1 = val1.Float()
		} else {
			f1 = float64(val1.Int())
		}
		if isFloat(val2) {
			f2 = val2.Float()
		} else {
			f2 = float64(val2.Int())
		}
		if f1 < f2 {
			return -1
		}
		if f1 > f2 {
			return 1
		}
		return 0
	default:
		// Fallback to string comparison for everything else
		s1, s2 := fmt.Sprintf("%v", v1), fmt.Sprintf("%v", v2)
		if s1 < s2 {
			return -1
		}
		if s1 > s2 {
			return 1
		}
		return 0
	}
}

func (vm *VM) SerializeRow(cols []types.ColumnDef, values []any) ([]byte, error) {
	buf := new(bytes.Buffer)

	for i, col := range cols {
		b, err := ValueToBytes(values[i], col.Type)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", col.Name, err)
		}
		buf.Write(b)
	}

	result := buf.Bytes()
	return result, nil
}

func ValueToBytes(val any, typ string) ([]byte, error) {
	buf := new(bytes.Buffer)

	switch strings.ToUpper(typ) {

	case "INT":
		i32, err := toInt(val)
		if err != nil {
			return nil, err
		}
		// Always 4 bytes
		if err := binary.Write(buf, binary.LittleEndian, i32); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil

	case "FLOAT":
		f32, err := toFloat(val)
		if err != nil {
			return nil, err
		}
		bits := math.Float32bits(f32)
		if err := binary.Write(buf, binary.LittleEndian, bits); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil

	case "VARCHAR":
		s, err := toString(val)
		if err != nil {
			return nil, err
		}
		if len(s) > 65535 {
			return nil, fmt.Errorf("varchar too long")
		}

		// length prefix
		if err := binary.Write(buf, binary.LittleEndian, uint16(len(s))); err != nil {
			return nil, err
		}
		buf.Write([]byte(s))
		return buf.Bytes(), nil
	}

	return nil, fmt.Errorf("unsupported type %s", typ)
}

func BytesToValue(b []byte, typ string) (any, int, error) {
	switch strings.ToUpper(typ) {

	case "INT":
		if len(b) < 4 {
			return nil, 0, fmt.Errorf("not enough bytes for int")
		}
		i := int32(binary.LittleEndian.Uint32(b[:4]))
		return int(i), 4, nil

	case "FLOAT":
		if len(b) < 4 {
			return nil, 0, fmt.Errorf("not enough bytes for float")
		}
		bits := binary.LittleEndian.Uint32(b[:4])
		f := math.Float32frombits(bits)
		return float64(f), 4, nil

	case "VARCHAR":
		if len(b) < 2 {
			return nil, 0, fmt.Errorf("not enough bytes for varchar length")
		}
		strlen := binary.LittleEndian.Uint16(b[:2])
		if len(b) < int(2+strlen) {
			return nil, 0, fmt.Errorf("varchar length exceeds row size")
		}
		s := string(b[2 : 2+strlen])
		return s, int(2 + strlen), nil
	}

	return nil, 0, fmt.Errorf("unknown type %s", typ)
}

func (vm *VM) DeserializeRow(row []byte, cols []types.ColumnDef) ([]any, error) {
	out := make([]any, len(cols))
	offset := 0

	for i, col := range cols {
		if offset >= len(row) {
			return nil, fmt.Errorf("not enough data for column %s (offset %d >= row length %d)",
				col.Name, offset, len(row))
		}

		val, read, err := BytesToValue(row[offset:], col.Type)
		if err != nil {
			return nil, fmt.Errorf("column %s at offset %d: %w", col.Name, offset, err)
		}
		out[i] = val
		offset += read
	}

	if offset != len(row) {
		return nil, fmt.Errorf("extra bytes at end of row: expected total %d bytes, got %d bytes (unused: %d bytes)",
			offset, len(row), len(row)-offset)
	}
	return out, nil
}

func (vm *VM) SerializeRowPointer(ptr *heapfile.RowPointer) []byte {
	buf := make([]byte, 10) // FileID(4) + PageNumber(4) + SlotIndex(2)
	binary.LittleEndian.PutUint32(buf[0:4], ptr.FileID)
	binary.LittleEndian.PutUint32(buf[4:8], ptr.PageNumber)
	binary.LittleEndian.PutUint16(buf[8:10], ptr.SlotIndex)
	return buf
}

func (vm *VM) DeserializeRowPointer(b []byte) (*heapfile.RowPointer, error) {
	if len(b) < 10 {
		return nil, fmt.Errorf("row pointer buffer too short: %d", len(b))
	}
	return &heapfile.RowPointer{
		FileID:     binary.LittleEndian.Uint32(b[0:4]),
		PageNumber: binary.LittleEndian.Uint32(b[4:8]),
		SlotIndex:  binary.LittleEndian.Uint16(b[8:10]),
	}, nil
}

/*

******************************* LOAD TABLE ROWS *******************************


 */

func (vm *VM) loadTableRows(tableName string) ([]map[string]interface{}, types.TableSchema, error) {
	fileID, ok := vm.tableToFileId[tableName]
	if !ok {
		return nil, types.TableSchema{}, fmt.Errorf("table '%s' not found", tableName)
	}

	hf, err := vm.heapfileManager.GetHeapFileByID(fileID)
	if err != nil {
		return nil, types.TableSchema{}, fmt.Errorf("failed to get heapfile: %w", err)
	}

	schemaPath := filepath.Join(DB_ROOT, vm.currDb, "tables", tableName+"_schema.json")
	data, err := os.ReadFile(schemaPath)
	if err != nil {
		return nil, types.TableSchema{}, fmt.Errorf("failed to read schema: %w", err)
	}

	var schema types.TableSchema
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, types.TableSchema{}, fmt.Errorf("invalid schema: %w", err)
	}

	rowPtrs := hf.GetAllRowPointers()
	rows := []map[string]interface{}{}

	for _, rp := range rowPtrs {
		raw, err := vm.heapfileManager.GetRow(rp)
		if err != nil {
			continue
		}

		values, err := vm.DeserializeRow(raw, schema.Columns)
		if err != nil {
			continue
		}

		// there is a mapping
		// the column name acts as the key
		// because then while doing joins, table1.id = table2.id can be matched

		row := make(map[string]interface{})
		for i, col := range schema.Columns {
			val := values[i]
			if s, ok := val.(string); ok {
				val = strings.TrimSpace(s)
			}
			row[tableName+"."+col.Name] = val
		}
		rows = append(rows, row)
	}

	return rows, schema, nil
}

/*

******************************* LOAD and SAVE MAPPINGS and TABLE SCHEMA *******************************


 */

func (vm *VM) SaveTableFileMapping() error {
	mappingPath := filepath.Join(DB_ROOT, vm.currDb, "table_file_mapping.json")
	mappingData := struct {
		Counter  uint32            `json:"counter"`
		Mappings map[string]uint32 `json:"mappings"`
	}{
		Counter:  vm.heapFileCounter,
		Mappings: vm.tableToFileId,
	}

	data, err := json.MarshalIndent(mappingData, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal mapping: %w", err)
	}

	// first write to a temp file, if any error occurs our original file will not be affected (left incomplete)
	tempPath := mappingPath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp mapping file: %w", err)
	}

	if err := os.Rename(tempPath, mappingPath); err != nil {
		os.Remove(tempPath) // remove the incomplete write
		return fmt.Errorf("failed to rename mapping file: %w", err)
	}

	return nil
}

func (vm *VM) LoadTableFileMapping() error {
	mappingPath := filepath.Join(DB_ROOT, vm.currDb, "table_file_mapping.json")

	data, err := os.ReadFile(mappingPath)
	if err != nil {
		if os.IsNotExist(err) {
			// First time, initialize empty mapping
			vm.tableToFileId = make(map[string]uint32)
			vm.heapFileCounter = 1
			return nil
		}
		return fmt.Errorf("failed to read mapping file: %w", err)
	}

	var mappingData struct {
		Counter  uint32            `json:"counter"`
		Mappings map[string]uint32 `json:"mappings"`
	}

	if err := json.Unmarshal(data, &mappingData); err != nil {
		return fmt.Errorf("failed to unmarshal mapping: %w", err)
	}

	vm.heapFileCounter = mappingData.Counter
	vm.tableToFileId = mappingData.Mappings

	return nil
}

func (vm *VM) LoadAllTableSchemas() error {
	if vm.currDb == "" {
		return fmt.Errorf("no database selected")
	}

	tablesDir := filepath.Join(DB_ROOT, vm.currDb, "tables")

	entries, err := os.ReadDir(tablesDir)
	if err != nil {
		return fmt.Errorf("failed to read tables directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasSuffix(name, "_schema.json") {
			continue
		}

		schemaPath := filepath.Join(tablesDir, name)
		data, err := os.ReadFile(schemaPath)
		if err != nil {
			return fmt.Errorf("failed to read schema file %s: %w", schemaPath, err)
		}

		var schema types.TableSchema
		if err := json.Unmarshal(data, &schema); err != nil {
			return fmt.Errorf("invalid schema in file %s: %w", schemaPath, err)
		}

		// Register table schema in VM
		if vm.tableSchemas == nil {
			vm.tableSchemas = make(map[string]types.TableSchema)
		}
		vm.tableSchemas[schema.TableName] = schema
	}

	return nil
}

/*

********************************* MERGE, SORT AND FILTER FOR JOINS *********************************

 */

func (vm *VM) sortRowsByColumn(rows []map[string]interface{}, colName string) {
	/*
		rows[0]["id"] = 2
		rows[1]["id"] = 1

		or could be
		rows[0]["name"] = "temp"
	*/
	sort.Slice(rows, func(i, j int) bool {
		return compareValues(rows[i][colName], rows[j][colName]) < 0
	})
}

func (vm *VM) mergeSortInnerJoin(left, right []map[string]interface{}, leftCol, rightCol string) []map[string]interface{} {
	result := []map[string]interface{}{}
	i, j := 0, 0
	lenL, lenR := len(left), len(right)
	for i < lenL && j < lenR {
		leftVal := left[i][leftCol]
		rightVal := right[j][rightCol]

		// if in case the joining key has nil, dont use it
		if leftVal == nil || rightVal == nil {
			if leftVal == nil {
				i++
			}
			if rightVal == nil {
				j++
			}
			continue
		}

		cmp := compareValues(leftVal, rightVal)

		if cmp < 0 {
			i++
		} else if cmp > 0 {
			j++
		} else {
			target := left[i][leftCol]
			leftStart := i
			for i < len(left) && compareValues(left[i][leftCol], target) == 0 { // iterate till same
				i++
			}

			rightStart := j
			for j < len(right) && compareValues(right[j][rightCol], target) == 0 {
				j++
			}

			// combination of matching rows
			for li := leftStart; li < i; li++ {
				for ri := rightStart; ri < j; ri++ {
					merged := make(map[string]interface{})
					for k, v := range left[li] {
						merged[k] = v
					}
					for k, v := range right[ri] {
						merged[k] = v
					}
					result = append(result, merged)
				}
			}
		}
	}
	return result
}

func (vm *VM) mergeSortOuterJoin(left, right []map[string]interface{}, leftCol, rightCol string) []map[string]interface{} {

	result := []map[string]interface{}{}
	i, j := 0, 0

	// left join Style
	for i < len(left) {
		valL := left[i][leftCol]

		if valL == nil || j >= len(right) {
			result = append(result, vm.copyRowWithNulls(left[i]))
			i++
			continue
		}

		valR := right[j][rightCol]
		if valR == nil {
			j++
			continue
		}

		cmp := compareValues(valL, valR)

		if cmp < 0 {
			result = append(result, vm.copyRowWithNulls(left[i]))
			i++
		} else if cmp > 0 {
			j++
		} else {
			matchVal := valL
			leftStart, rightStart := i, j

			for i < len(left) && compareValues(left[i][leftCol], matchVal) == 0 {
				i++
			}
			for j < len(right) && compareValues(right[j][rightCol], matchVal) == 0 {
				j++
			}

			for li := leftStart; li < i; li++ {
				for ri := rightStart; ri < j; ri++ {
					merged := make(map[string]interface{})
					for k, v := range left[li] {
						merged[k] = v
					}
					for k, v := range right[ri] {
						merged[k] = v
					}
					result = append(result, merged)
				}
			}
		}
	}
	return result
}

func (vm *VM) mergeSortFullJoin(left, right []map[string]interface{}, leftCol, rightCol string) []map[string]interface{} {
	result := []map[string]interface{}{}
	i, j := 0, 0

	// until both tables are exhausted
	for i < len(left) || j < len(right) {

		// right table exhausted, but Left still has rows
		if j >= len(right) {
			result = append(result, vm.copyRowWithNulls(left[i]))
			i++
			continue
		}

		// left table exhausted, but Right still has rows
		if i >= len(left) {
			result = append(result, vm.copyRowWithNulls(right[j]))
			j++
			continue
		}

		valL := left[i][leftCol]
		valR := right[j][rightCol]

		// handle nil values
		if valL == nil {
			result = append(result, vm.copyRowWithNulls(left[i]))
			i++
			continue
		}
		if valR == nil {
			result = append(result, vm.copyRowWithNulls(right[j]))
			j++
			continue
		}

		cmp := compareValues(valL, valR)

		if cmp < 0 {
			result = append(result, vm.copyRowWithNulls(left[i]))
			i++
		} else if cmp > 0 {
			result = append(result, vm.copyRowWithNulls(right[j]))
			j++
		} else {
			matchVal := valL
			leftStart, rightStart := i, j

			for i < len(left) && compareValues(left[i][leftCol], matchVal) == 0 {
				i++
			}
			for j < len(right) && compareValues(right[j][rightCol], matchVal) == 0 {
				j++
			}

			for li := leftStart; li < i; li++ {
				for ri := rightStart; ri < j; ri++ {
					merged := make(map[string]interface{})
					for k, v := range left[li] {
						merged[k] = v
					}
					for k, v := range right[ri] {
						merged[k] = v
					}
					result = append(result, merged)
				}
			}
		}
	}
	return result
}

func (vm *VM) filterJoinedRows(rows []map[string]interface{}, whereCol, whereVal string) []map[string]interface{} {
	filtered := []map[string]interface{}{}

	for _, row := range rows {
		val := row[whereCol]

		// Handle WHERE column = NULL
		if strings.ToUpper(whereVal) == "NULL" || whereVal == "" {
			if val == nil {
				filtered = append(filtered, row)
			}
			continue
		}

		if val != nil && fmt.Sprintf("%v", val) == whereVal {
			filtered = append(filtered, row)
		}
	}
	return filtered
}

// to make sure null rows are added too in case of right or left joins or full join
func (vm *VM) copyRowWithNulls(rows map[string]interface{}) map[string]interface{} {
	merged := make(map[string]interface{})
	for k, v := range rows {
		merged[k] = v
	}
	return merged
}

/*

********************************* INDEXING FOR BPLUS TREE *********************************

 */

func (vm *VM) ExtractPrimaryKey(schema types.TableSchema, values []any, rowPtr *heapfile.RowPointer) ([]byte, string, error) {
	// Option 1: Use explicit primary key if defined
	for i, col := range schema.Columns {
		if col.IsPrimaryKey {
			keyBytes, err := ValueToBytes(values[i], col.Type)
			if err != nil {
				return nil, "", err
			}
			return keyBytes, col.Name, nil
		}
	}

	// No primary key - use RowPointer as surogate key (Physical address)
	// Format: FileID(4 bytes) + PageNumber(4 bytes) = 8 bytes unique key
	return vm.GenerateImplicitKey(rowPtr), "__rowid__", nil
}

func (vm *VM) GenerateImplicitKey(rowPtr *heapfile.RowPointer) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[0:4], rowPtr.FileID)
	binary.BigEndian.PutUint32(buf[4:8], rowPtr.PageNumber)
	return buf
}

func (vm *VM) GetOrCreateIndex(tableName string) (*bplus.BPlusTree, error) {
	if vm.currDb == "" {
		return nil, fmt.Errorf("no database selected")
	}

	indexKey := fmt.Sprintf("%s_primary", tableName)

	// Index file path: <dbPath>/indexes/<tableName>_primary.idx
	indexDir := filepath.Join(DB_ROOT, vm.currDb, "indexes")
	if err := os.MkdirAll(indexDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create indexes directory: %w", err)
	}

	indexPath := filepath.Join(indexDir, indexKey+".idx")

	// Check if table exists
	if _, exists := vm.tableToFileId[tableName]; !exists {
		return nil, fmt.Errorf("table '%s' does not exist", tableName)
	}

	// fmt.Printf("Opening B+ tree index: %s\n", indexPath)
	btree, err := OpenBPlusTree(indexPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open B+ tree: %w", err)
	}

	// fmt.Printf("Index opened: %s\n", indexKey)

	return btree, nil
}

// OpenBPlusTree opens or creates a B+ tree index file
func OpenBPlusTree(indexPath string) (*bplus.BPlusTree, error) {
	// Create/open the disk pager for the index file
	pager, err := bplus.NewOnDiskPager(indexPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create pager: %w", err)
	}

	// Create a buffer pool for caching nodes in memory
	// Capacity of 10 means we can cache up to 10 nodes
	cache := bplus.NewBufferPool(10)

	// Create the B+ tree with the pager, cache, and default byte comparison
	// If the index file is new, root will be 0 (empty tree)
	// If the index file exists, we'll need to load the root from metadata (TODO: implement root persistence)
	btree := bplus.NewBPlusTree(pager, cache, bytes.Compare)

	return btree, nil
}

/*
********************************* PRINTING TABLE *********************************
 */
func (vm *VM) PrintLine(cells []string) {
	for i, cell := range cells {
		fmt.Printf("%-20s", cell)
		if i < len(cells)-1 {
			fmt.Print("| ")
		}
	}
	fmt.Println()
}

func (vm *VM) PrintSeparator(count int) {
	if count > 0 {
		fmt.Println(strings.Repeat("-", (22*count)-2))
	}
}

func (vm *VM) formatValue(val interface{}) string {
	if val == nil {
		return "NULL"
	}
	s, err := toString(val)
	if err != nil {
		return fmt.Sprintf("%v", val)
	}
	return s
}
