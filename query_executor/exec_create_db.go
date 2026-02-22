package executor

/*
This file contains simple wrapper function for database related command that bridges the gap between vm and storage engine
*/

func (vm *VM) ExecuteCreateDatabase(name string) error {
	return vm.storageEngine.CreateDatabase(name)
}

func (vm *VM) ExecuteUseDatabase(name string) error {
	return vm.storageEngine.UseDatabase(name)
}
