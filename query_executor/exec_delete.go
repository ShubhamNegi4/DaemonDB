package executor

import "fmt"

// ExecDelete executes DELETE through the storage engine
func (vm *VM) ExecDelete(table string) error {

	if vm.storageEngine == nil {
		return fmt.Errorf("storage engine not initialized")
	}

	return vm.storageEngine.DeleteRows(table)
}
