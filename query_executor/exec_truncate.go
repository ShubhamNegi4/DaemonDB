package executor

import "fmt"

/*
ExecTruncate handles TRUNCATE TABLE execution.
Delegates to the storage engine.
*/

func (vm *VM) ExecTruncate(tableName string) error {

	if vm.storageEngine == nil {
		return fmt.Errorf("storage engine not initialized")
	}

	return vm.storageEngine.TruncateTable(tableName)
}
