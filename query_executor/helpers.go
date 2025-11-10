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
)

func (vm *VM) SerializeRow(cols []ColumnDef, values []any) ([]byte, error) {
	row := new(bytes.Buffer)
	for i, col := range cols {
		switch col.Type {
		case "INT", "int":
			v := values[i].(int)
			binary.Write(row, binary.LittleEndian, int64(v))
		case "VARCHAR", "varchar":
			s := values[i].(string)
			binary.Write(row, binary.LittleEndian, uint16(len(s)))
			row.Write([]byte(s))
		default:
			panic("unknown column type")
		}
	}
	return row.Bytes(), nil
}

func ValueToBytes(value any, colType string) ([]byte, error) {
	switch colType {
	case "INT", "INTEGER":
		v, ok := value.(uint64)
		if !ok {
			return nil, fmt.Errorf("expected int, got %T", value)
		}
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, v)
		return buf, nil

	case "VARCHAR":
		v, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected string, got %T", value)
		}
		return []byte(v), nil

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
			vm.heapFileCounter = 0
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

// TODO: complete this function

func OpenBPlusTree(indexPath string) (*bplus.BPlusTree, error) {
	// The pager handles all disk I/O
	pager, err := bplus.NewOnDiskPager(indexPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create pager: %w", err)
	}

	// Check if this is a new index or existing one
	if pager.IsPageFull != 0 {
		// Create new root page

		// bplus tree page will write this new page to disk

	}

	// return this bplus tree to be worked on
	return &bplus.BPlusTree{}, nil
}
