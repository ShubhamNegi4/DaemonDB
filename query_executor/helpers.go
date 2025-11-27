package executor

import (
	bplus "DaemonDB/bplustree"
	heapfile "DaemonDB/heapfile_manager"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
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

func (vm *VM) SerializeRow(cols []ColumnDef, values []any) ([]byte, error) {
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

func (vm *VM) DeserializeRow(row []byte, cols []ColumnDef) ([]any, error) {
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
	buf := make([]byte, 8) // FileID(4) + PageNumber(4)
	binary.LittleEndian.PutUint32(buf[0:4], ptr.FileID)
	binary.LittleEndian.PutUint32(buf[4:8], ptr.PageNumber)
	return buf
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

		var schema TableSchema
		if err := json.Unmarshal(data, &schema); err != nil {
			return fmt.Errorf("invalid schema in file %s: %w", schemaPath, err)
		}

		// Register table schema in VM
		if vm.tableSchemas == nil {
			vm.tableSchemas = make(map[string]TableSchema)
		}
		vm.tableSchemas[schema.TableName] = schema
	}

	return nil
}

/*

********************************* INDEXING FOR BPLUS TREE *********************************

 */

func (vm *VM) ExtractPrimaryKey(schema TableSchema, values []any, rowPtr *heapfile.RowPointer) ([]byte, string, error) {
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

func (vm *VM) PrintTableHeader(columns []ColumnDef) {
	for i, col := range columns {
		fmt.Printf("%-20s", col.Name)
		if i < len(columns)-1 {
			fmt.Print("| ")
		}
	}
	fmt.Println()
	fmt.Println(strings.Repeat("-", 22*len(columns)))
}

// PrintTableRow prints a single row in formatted style with consistent width
func (vm *VM) PrintTableRow(rowValues []any, columns []ColumnDef) {
	for i := range columns {
		val := rowValues[i]

		// Convert any type to readable string safely
		str := fmt.Sprintf("%v", val)

		// FIXED: Print with consistent width (20 chars like header)
		fmt.Printf("%-20s", str)

		// Separator except for last column
		if i < len(columns)-1 {
			fmt.Print("| ")
		}
	}
	fmt.Println()
}
