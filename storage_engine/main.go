package storageengine

import (
	"DaemonDB/storage_engine/catalog"
	txn "DaemonDB/storage_engine/transaction_manager"
	types "DaemonDB/types"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

/*
The main file of storage engine, that initializes the storage engine and the catalog manager
Use Database Command does the actual disk loading afterwards
*/

func NewStorageEngine(dbRoot string) (*StorageEngine, error) {
	if err := os.MkdirAll(dbRoot, 0755); err != nil {
		return nil, fmt.Errorf("failed to create db root: %w", err)
	}

	catalogManager, err := catalog.NewCatalogManager(dbRoot)
	if err != nil {
		return nil, fmt.Errorf("failed to init catalog manager: %w", err)
	}

	se := &StorageEngine{
		DbRoot:         dbRoot,
		CatalogManager: catalogManager,
	}

	return se, nil
}

// gets all row pointers of the file that belongs to that table
func (se *StorageEngine) Scan(transaction *txn.Transaction, tableName string) ([]types.RowWithPointer, types.TableSchema, error) {

	if se.currDb == "" {
		return nil, types.TableSchema{}, fmt.Errorf("no database selected")
	}

	if transaction == nil {
		return nil, types.TableSchema{}, fmt.Errorf("transaction is required")
	}

	// Load table schema
	schemaPath := filepath.Join(se.DbRoot, se.currDb, "tables", tableName+"_schema.json")

	data, err := os.ReadFile(schemaPath)
	if err != nil {
		return nil, types.TableSchema{},
			fmt.Errorf("table '%s' not found: %w", tableName, err)
	}

	var schema types.TableSchema
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, types.TableSchema{},
			fmt.Errorf("invalid schema format: %w", err)
	}

	// Get heap file ID
	fileID, err := se.CatalogManager.GetTableFileID(tableName)
	if err != nil {
		return nil, types.TableSchema{},
			fmt.Errorf("heap file not found for table '%s'", tableName)
	}

	hf, err := se.HeapManager.GetHeapFileByID(fileID)
	if err != nil {
		return nil, types.TableSchema{},
			fmt.Errorf("failed to access heap file: %w", err)
	}

	// Get all row pointers
	rowPtrs := hf.GetAllRowPointers()

	if len(rowPtrs) == 0 {
		return []types.RowWithPointer{}, schema, nil
	}

	var result []types.RowWithPointer

	// Iterate rows
	for _, ptr := range rowPtrs {

		rawRow, err := se.HeapManager.GetRow(&ptr)
		if err != nil {
			continue // skip corrupted row
		}

		values, err := se.DeserializeRow(rawRow, schema.Columns)
		if err != nil {
			continue // skip bad row
		}

		// Convert to map
		rowMap := make(map[string]interface{})
		for i, col := range schema.Columns {
			rowMap[strings.ToLower(col.Name)] = values[i]
		}

		result = append(result, types.RowWithPointer{
			Pointer: ptr,
			Row: types.Row{
				Values: rowMap,
			},
		})
	}

	return result, schema, nil
}

func (se *StorageEngine) RequireDatabase() error {
	if se.currDb == "" || se.HeapManager == nil || se.WalManager == nil {
		return fmt.Errorf("no database selected. Use 'USE <database>' command first")
	}
	return nil
}
