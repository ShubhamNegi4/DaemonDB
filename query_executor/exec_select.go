package executor

import (
	"DaemonDB/types"
	"encoding/json"
	"fmt"
)

/*
This file contains select query for the table
the vm function does the pre processing like unmarshling the payload sent in the query
then send it to the storage engine to perform the operation
and prints the returned result (columns header and rows)
SELECT is a read-only operation, so it doesn't need transaction boundaries (no auto-transaction wrapping).
*/

func (vm *VM) ExecuteSelect(payload string) error {
	if err := vm.storageEngine.RequireDatabase(); err != nil {
		return fmt.Errorf("no database selected. Run: USE <dbname>")
	}

	var selectPayload types.SelectPayload
	if err := json.Unmarshal([]byte(payload), &selectPayload); err != nil {
		return fmt.Errorf("invalid select payload: %w", err)
	}

	// StorageEngine returns rows as []map[string]interface{}
	rows, columns, err := vm.storageEngine.ExecuteSelect(selectPayload)
	if err != nil {
		return err
	}

	if len(rows) == 0 {
		fmt.Println("no rows returned")
		return nil
	}

	// Print column headers
	vm.PrintLine(columns)
	vm.PrintSeparator(len(columns))

	// Print each row
	for _, row := range rows {
		strs := make([]string, len(columns))
		for i, col := range columns {
			strs[i] = vm.formatValue(row[col])
		}
		vm.PrintLine(strs)
	}
	return nil
}
