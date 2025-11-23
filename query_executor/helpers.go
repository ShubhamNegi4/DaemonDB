package executor

import (
	bplus "DaemonDB/bplustree"
	heapfile "DaemonDB/heapfile_manager"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func toInt(v any) (int64, error) {
	switch x := v.(type) {
	case int:
		return int64(x), nil
	case int64:
		return x, nil
	case float64:
		return int64(x), nil
	case string:
		i, err := strconv.Atoi(x)
		if err != nil {
			return 0, fmt.Errorf("cannot convert string to int: %s", x)
		}
		return int64(i), nil
	case []byte:
		i, err := strconv.Atoi(string(x))
		if err != nil {
			return 0, fmt.Errorf("cannot convert []byte to int: %s", x)
		}
		return int64(i), nil
	default:
		return 0, fmt.Errorf("expected int, got %T", v)
	}
}

func toString(v any) (string, error) {
	switch x := v.(type) {
	case string:
		return x, nil
	case []byte:
		return string(x), nil
	case int:
		return strconv.Itoa(x), nil // NEW
	case int64:
		return strconv.FormatInt(x, 10), nil // NEW
	case float64:
		return strconv.FormatInt(int64(x), 10), nil // NEW
	default:
		return "", fmt.Errorf("expected string, got %T", v)
	}
}

func (vm *VM) SerializeRow(cols []ColumnDef, values []any) ([]byte, error) {
	row := new(bytes.Buffer)

	for i, col := range cols {
		data, err := ValueToBytes(values[i], col.Type)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", col.Name, err)
		}
		row.Write(data)
	}

	return row.Bytes(), nil
}

func ValueToBytes(value any, colType string) ([]byte, error) {
	buf := new(bytes.Buffer)

	switch strings.ToUpper(colType) {

	case "INT":
		i, err := toInt(value)
		if err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, i); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil

	case "VARCHAR":
		s, err := toString(value)
		if err != nil {
			return nil, err
		}

		if err := binary.Write(buf, binary.LittleEndian, uint16(len(s))); err != nil {
			return nil, err
		}

		buf.Write([]byte(s))
		return buf.Bytes(), nil

	default:
		return nil, fmt.Errorf("unsupported column type: %s", colType)
	}
}

func (vm *VM) SerializeRowPointer(ptr *heapfile.RowPointer) []byte {
	buf := make([]byte, 8) // FileID(4) + PageNumber(4)
	binary.LittleEndian.PutUint32(buf[0:4], ptr.FileID)
	binary.LittleEndian.PutUint32(buf[4:8], ptr.PageNumber)
	return buf
}

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

		// Register table in VM
		vm.tableToFileId[schema.TableName] = vm.tableToFileId[schema.TableName] // existing mapping
		// Optionally, keep a schema map for quick access
		if vm.tableSchemas == nil {
			vm.tableSchemas = make(map[string]TableSchema)
		}
		vm.tableSchemas[schema.TableName] = schema
	}

	return nil
}

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

	// // TODO: Create or open B+ tree
	fmt.Printf("Opening B+ tree index: %s\n", indexPath)
	btree, err := OpenBPlusTree(indexPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open B+ tree: %w", err)
	}

	fmt.Printf("Index opened: %s\n", indexKey)

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
