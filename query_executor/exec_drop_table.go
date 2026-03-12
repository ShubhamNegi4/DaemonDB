package executor

import "fmt"

// ExecDropTable executes DROP TABLE through the storage engine
func (vm *VM) ExecDropTable(tableName string) error {

	if vm.storageEngine == nil {
		return fmt.Errorf("storage engine not initialized")
	}

	return vm.storageEngine.DropTable(tableName)
}
