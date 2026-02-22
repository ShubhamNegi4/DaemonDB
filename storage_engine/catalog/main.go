package catalog

import (
	types "DaemonDB/types"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

/*
This file is the main acess of Catalog Manager
Catalog manager maintains the metadata of the database and also persist it on the disk
It persists Heap File Counting, Table to fileId mapping and Schema of tables on the disk
All these mappings are loaded when USE command is executed
*/

func NewCatalogManager(dbRoot string) (*CatalogManager, error) {
	return &CatalogManager{
		dbRoot:        dbRoot,
		nextFileID:    1,
		TableToFileId: make(map[string]TableFileMapping),
		tableSchemas:  make(map[string]types.TableSchema),
	}, nil
}

func (cm *CatalogManager) SetCurrentDatabase(newDb string) {
	fmt.Printf("currDb: %s  newDb: %s\n", cm.currDb, newDb)
	cm.currDb = newDb
}

func (cm *CatalogManager) TableExists(tableName string) bool {
	if cm.tableSchemas == nil {
		return false
	}
	_, exists := cm.tableSchemas[tableName]
	return exists
}

func (cm *CatalogManager) GetTableSchema(name string) (types.TableSchema, error) {

	if cm.currDb == "" {
		return types.TableSchema{}, fmt.Errorf("no database selected")
	}
	fmt.Printf("tablename: %+v\n", cm.tableSchemas)
	// Initialize catalog map if nil
	if cm.tableSchemas == nil {
		cm.tableSchemas = make(map[string]types.TableSchema)
	}

	// Fast path: return from memory
	if schema, ok := cm.tableSchemas[name]; ok {
		return schema, nil
	}

	// Load from disk
	schemaPath := filepath.Join(
		cm.dbRoot,
		cm.currDb,
		"tables",
		name+"_schema.json",
	)

	data, err := os.ReadFile(schemaPath)
	if err != nil {
		return types.TableSchema{}, fmt.Errorf(
			"table '%s' does not exist",
			name,
		)
	}

	var schema types.TableSchema
	if err := json.Unmarshal(data, &schema); err != nil {
		return types.TableSchema{}, fmt.Errorf(
			"failed to parse schema for table '%s': %w",
			name,
			err,
		)
	}

	// Cache in memory for future lookups
	cm.tableSchemas[name] = schema

	return schema, nil
}

func (cm *CatalogManager) RegisterNewTable(schema types.TableSchema) (uint32, uint32, error) {

	tableName := schema.TableName

	if cm.tableSchemas == nil {
		cm.tableSchemas = make(map[string]types.TableSchema)
	}
	if cm.TableToFileId == nil {
		cm.TableToFileId = make(map[string]TableFileMapping)
	}

	heapFileID := cm.nextFileID
	cm.nextFileID++
	indexFileID := cm.nextFileID
	cm.nextFileID++

	cm.tableSchemas[tableName] = schema
	cm.TableToFileId[tableName] = TableFileMapping{
		HeapFileID:  heapFileID,
		IndexFileID: indexFileID,
	}

	if err := cm.persistSchema(schema); err != nil {
		return 0, 0, err
	}
	if err := cm.PersistTableMapping(); err != nil {
		return 0, 0, err
	}
	if err := cm.persistNextFileID(); err != nil {
		return 0, 0, err
	}

	return heapFileID, indexFileID, nil
}

func (cm *CatalogManager) UnregisterTable(tableName string) error {
	// guard
	if cm.tableSchemas == nil || cm.TableToFileId == nil {
		return fmt.Errorf("catalog not initialised")
	}

	if _, exists := cm.tableSchemas[tableName]; !exists {
		return fmt.Errorf("table '%s' not found in catalog", tableName)
	}

	// remove from in-memory maps
	delete(cm.tableSchemas, tableName)
	delete(cm.TableToFileId, tableName)

	// two files were allocated per table
	if cm.nextFileID >= 2 {
		cm.nextFileID -= 2
	}

	schemaPath := filepath.Join(cm.dbRoot, cm.currDb, "tables", tableName+"_schema.json")
	if err := os.Remove(schemaPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete schema file: %w", err)
	}

	if err := cm.PersistTableMapping(); err != nil {
		return err
	}
	if err := cm.persistNextFileID(); err != nil {
		return err
	}

	return nil
}

func (cm *CatalogManager) persistSchema(schema types.TableSchema) error {

	schemaDir := filepath.Join(cm.dbRoot, cm.currDb, "tables")
	if err := os.MkdirAll(schemaDir, 0755); err != nil {
		return err
	}

	schemaPath := filepath.Join(schemaDir, schema.TableName+"_schema.json")

	data, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(schemaPath, data, 0644)
}

func (cm *CatalogManager) PersistTableMapping() error {
	metaDir := filepath.Join(cm.dbRoot, cm.currDb, "metadata")
	if err := os.MkdirAll(metaDir, 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cm.TableToFileId, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(metaDir, "table_file_mapping.json"), data, 0644)
}

func (cm *CatalogManager) persistNextFileID() error {
	metaDir := filepath.Join(cm.dbRoot, cm.currDb, "metadata")
	if err := os.MkdirAll(metaDir, 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cm.nextFileID, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(metaDir, "next_file_id.json"), data, 0644)
}

func (cm *CatalogManager) GetTableFileID(tableName string) (uint32, error) {
	mapping, exists := cm.TableToFileId[tableName]
	if !exists {
		return 0, fmt.Errorf("table '%s' not found in file mapping", tableName)
	}
	return mapping.HeapFileID, nil
}

func (cm *CatalogManager) GetIndexFileID(tableName string) (uint32, error) {
	mapping, exists := cm.TableToFileId[tableName]
	if !exists {
		return 0, fmt.Errorf("table '%s' not found in file mapping", tableName)
	}
	return mapping.IndexFileID, nil
}
func (cm *CatalogManager) LoadTableFileMapping() error {
	metaDir := filepath.Join(cm.dbRoot, cm.currDb, "metadata")
	cm.TableToFileId = make(map[string]TableFileMapping)

	data, err := os.ReadFile(filepath.Join(metaDir, "table_file_mapping.json"))
	if err != nil {
		if os.IsNotExist(err) {
			cm.nextFileID = 1
			return nil
		}
		return fmt.Errorf("failed to read mapping file: %w", err)
	}

	if err := json.Unmarshal(data, &cm.TableToFileId); err != nil {
		return fmt.Errorf("failed to unmarshal mapping: %w", err)
	}

	// restore counter
	counterData, err := os.ReadFile(filepath.Join(metaDir, "next_file_id.json"))
	if err == nil {
		var counter uint32
		if json.Unmarshal(counterData, &counter) == nil {
			cm.nextFileID = counter
		}
	} else {
		// fallback: each table has 2 files
		cm.nextFileID = uint32(len(cm.TableToFileId)*2) + 1
	}

	return nil
}

func (cm *CatalogManager) LoadAllTableSchemas() error {
	if cm.currDb == "" {
		return fmt.Errorf("no database selected")
	}

	// reset the tableSchema
	cm.tableSchemas = make(map[string]types.TableSchema)

	tablesDir := filepath.Join(cm.dbRoot, cm.currDb, "tables")

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

		if cm.tableSchemas == nil {
			cm.tableSchemas = make(map[string]types.TableSchema)
		}
		cm.tableSchemas[schema.TableName] = schema
	}

	return nil
}

// GetAllTableMappings returns a copy of the in-memory table→fileID map.
func (cm *CatalogManager) GetAllTableMappings() map[string]TableFileMapping {
	result := make(map[string]TableFileMapping)
	for k, v := range cm.TableToFileId {
		result[k] = v
	}
	return result
}
