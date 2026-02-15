package executor

import (
	"DaemonDB/types"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func (vm *VM) ExecuteCreateTable(tableName string) error {
	if err := vm.RequireDatabase(); err != nil {
		return fmt.Errorf("no database selected. Run: USE <dbname>")
	}

	schemaPayload := string(vm.stack[len(vm.stack)-1])
	vm.stack = vm.stack[:len(vm.stack)-1]

	var payload struct {
		Columns     string                `json:"columns"`
		ForeignKeys []types.ForeignKeyDef `json:"foreign_keys"`
	}

	if err := json.Unmarshal([]byte(schemaPayload), &payload); err != nil {
		return fmt.Errorf("invalid table schema payload: %w", err)
	}

	colParts := strings.Split(payload.Columns, ",")
	columnDefs := make([]types.ColumnDef, 0, len(colParts))

	for _, col := range colParts {
		colItr := strings.Split(col, ":")
		if len(colItr) < 2 {
			return fmt.Errorf("invalid column format: %s", col)
		}
		isPK := len(colItr) >= 3 && strings.EqualFold(colItr[2], "pk")
		colType := strings.ToUpper(colItr[0])
		columnDefs = append(columnDefs, types.ColumnDef{
			Name:         colItr[1],
			Type:         colType,
			IsPrimaryKey: isPK,
		})
	}

	if vm.tableSchemas == nil {
		vm.tableSchemas = make(map[string]types.TableSchema)
	}

	for _, fk := range payload.ForeignKeys {
		refSchema, ok := vm.tableSchemas[fk.RefTable]
		if !ok {
			return fmt.Errorf(
				"foreign key error: referenced table '%s' does not exist",
				fk.RefTable,
			)
		}

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

	op := &types.Operation{
		Type:   types.OpCreateTable,
		Table:  tableName,
		Schema: &schema,
	}
	_, err := vm.WalManager.AppendOperation(op)
	if err != nil {
		return fmt.Errorf("wal append failed: %w", err)
	}
	if err := vm.WalManager.Sync(); err != nil {
		return fmt.Errorf("wal sync failed: %w", err)
	}

	schemaPath := filepath.Join(DB_ROOT, vm.currDb, "tables", tableName+"_schema.json")
	schemaJson, _ := json.MarshalIndent(schema, "", "  ")
	if err := os.WriteFile(schemaPath, schemaJson, 0644); err != nil {
		return fmt.Errorf("cannot write schema: %w", err)
	}

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
