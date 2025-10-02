package executor

/*
this files executes the code generator output (process the sql statements) based on a large switch case statement on the OpCode in the Execute function
the vdbe like vm is currently a stack based apporach
*/

import (
	bplus "DaemonDB/B+Tree-Implementation"
	"encoding/json"
	"fmt"
)

type OpCode byte

const (
	// stack
	OP_PUSH_VAL OpCode = iota
	OP_PUSH_KEY

	// sql command
	OP_INSERT
	OP_SELECT
	OP_END
)

type Instruction struct {
	Op    OpCode
	Value string
}

type VM struct {
	tree  *bplus.BPlusTree
	stack [][]byte
}

func NewVM(tree *bplus.BPlusTree) *VM {
	return &VM{
		tree:  tree,
		stack: make([][]byte, 0),
	}
}

func (vm *VM) Execute(instructions []Instruction) error {
	for _, instr := range instructions {
		fmt.Printf("%v --> %v\n", instr.Op, instr.Value)
		switch instr.Op {
		case OP_PUSH_VAL:
			// Push value onto stack
			vm.stack = append(vm.stack, []byte(instr.Value))
			fmt.Printf("  Pushed value: %s (stack size: %d)\n", instr.Value, len(vm.stack))

		case OP_PUSH_KEY:
			vm.stack = append(vm.stack, []byte(instr.Value))
			fmt.Printf("  Pushed key: %s\n", instr.Value)

		case OP_INSERT:
			return vm.ExecuteInsert(instr.Value)

		case OP_SELECT:
			return vm.ExecuteSelect(instr.Value)

		case OP_END:
			return nil

		default:
			return fmt.Errorf("unknown opcode: %d", instr.Op)
		}
	}
	return nil
}

/*


implementation of functions that vm will execute based on the instruction OpCode


*/

func (vm *VM) serializeRow(values [][]byte) ([]byte, error) {
	strValues := make([]string, len(values))
	for i, v := range values {
		strValues[i] = string(v)
	}
	return json.Marshal(strValues)
}

func (vm *VM) ExecuteInsert(table string) error {
	if len(vm.stack) == 0 {
		return fmt.Errorf("no values to insert")
	}

	// Use first value as the key
	key := vm.stack[0]

	// Serialize remaining values (or all values) into a blob
	vm.stack = vm.stack[1:]
	valueBlob, err := vm.serializeRow(vm.stack)
	if err != nil {
		return fmt.Errorf("failed to serialize row: %v", err)
	}

	// Insert into B+ tree
	vm.tree.Insertion(key, valueBlob)

	fmt.Printf("Inserted into %s with key: %s\n", table, string(key))

	// Clear the stack after insertion ( the query was executed successfully )
	vm.stack = vm.stack[:0]

	return nil
}

func (vm *VM) ExecuteSelect(cols string) error {
	// to be decided
	return nil
}
