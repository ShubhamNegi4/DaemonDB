package executor

import (
	"DaemonDB/types"
	"fmt"
	"strings"
)

/*
This file contains helper functions that are required by the vm for query pre processing before it can call storage engine
*/

func (vm *VM) buildColumnDefs(columns string) ([]types.ColumnDef, error) {
	colParts := strings.Split(columns, ",")
	columnDefs := make([]types.ColumnDef, 0, len(colParts))

	for _, col := range colParts {
		colItr := strings.Split(col, ":")
		if len(colItr) < 2 {
			return nil, fmt.Errorf("invalid column format: %s", col)
		}

		isPK := len(colItr) >= 3 && strings.EqualFold(colItr[2], "pk")
		colType := strings.ToUpper(colItr[0])

		columnDefs = append(columnDefs, types.ColumnDef{
			Name:         colItr[1],
			Type:         colType,
			IsPrimaryKey: isPK,
		})
	}

	return columnDefs, nil
}

func (vm *VM) validateForeignKeys(schema types.TableSchema) error {
	for _, fk := range schema.ForeignKeys {

		// Get referenced table schema from storage engine
		refSchema, err := vm.storageEngine.CatalogManager.GetTableSchema(fk.RefTable)
		if err != nil {
			return fmt.Errorf(
				"foreign key error: referenced table '%s' does not exist",
				fk.RefTable,
			)
		}

		// Find FK column in current table
		var fkCol types.ColumnDef
		foundFKCol := false
		for _, c := range schema.Columns {
			if strings.EqualFold(c.Name, fk.Column) {
				fkCol = c
				foundFKCol = true
				break
			}
		}
		if !foundFKCol {
			return fmt.Errorf(
				"foreign key error: column '%s' does not exist in table '%s'",
				fk.Column, schema.TableName,
			)
		}

		// Find referenced PK column
		var refPKCol types.ColumnDef
		foundRefPK := false
		for _, c := range refSchema.Columns {
			if strings.EqualFold(c.Name, fk.RefColumn) {
				if !c.IsPrimaryKey {
					return fmt.Errorf(
						"foreign key error: referenced column '%s.%s' is not PRIMARY KEY",
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

		// Type match validation
		if !strings.EqualFold(fkCol.Type, refPKCol.Type) {
			return fmt.Errorf(
				"foreign key error: type mismatch (%s.%s is %s, %s.%s is %s)",
				schema.TableName, fk.Column, fkCol.Type,
				fk.RefTable, fk.RefColumn, refPKCol.Type,
			)
		}
	}

	return nil
}

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
	s, err := types.ToString(val)
	if err != nil {
		return fmt.Sprintf("%v", val)
	}
	return s
}
