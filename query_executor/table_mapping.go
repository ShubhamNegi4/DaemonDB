package executor

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"DaemonDB/types"
)

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

	tempPath := mappingPath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp mapping file: %w", err)
	}

	if err := os.Rename(tempPath, mappingPath); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to rename mapping file: %w", err)
	}

	return nil
}

func (vm *VM) LoadTableFileMapping() error {
	mappingPath := filepath.Join(DB_ROOT, vm.currDb, "table_file_mapping.json")

	data, err := os.ReadFile(mappingPath)
	if err != nil {
		if os.IsNotExist(err) {
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

		if vm.tableSchemas == nil {
			vm.tableSchemas = make(map[string]types.TableSchema)
		}
		vm.tableSchemas[schema.TableName] = schema
	}

	return nil
}
