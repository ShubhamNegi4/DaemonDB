package executor

/*
this files executes the code generator output (process the sql statements) based on a large switch case statement on the OpCode in the Execute function
the vdbe like vm is currently a stack based apporach
*/

import (
	bplus "DaemonDB/bplustree"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const DB_ROOT = "./databases" // all databases stored here

type OpCode byte

const (
	// stack
	OP_PUSH_VAL OpCode = iota
	OP_PUSH_KEY

	// sql command
	OP_CREATE_DB
	OP_SHOW_DB
	OP_CREATE_TABLE
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

		case OP_CREATE_DB:
			return vm.ExecuteCreateDatabase(instr.Value)

		case OP_SHOW_DB:
			databases, err := vm.ExecuteShowDatabases()
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("Databases:")
				for _, db := range databases {
					fmt.Printf("  - %s\n", db)
				}
			}
			return nil

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

func (vm *VM) ExecuteCreateDatabase(dbName string) error {
	println("make a db with: ", dbName)
	if dbName == "" {
		return fmt.Errorf("database name cannot be empty")
	}
	if err := os.MkdirAll(DB_ROOT, 0755); err != nil {
		return fmt.Errorf("failed to create DB directory: %w", err)
	}

	dbPath := filepath.Join(DB_ROOT, dbName)

	if _, err := os.Stat(dbPath); err == nil {
		return fmt.Errorf("database %s already exists", dbName)
	}
	if err := os.Mkdir(dbPath, 0755); err != nil {
		return fmt.Errorf("failed to create database %s: %w", dbName, err)
	}

	fmt.Printf("Created database directory: %s\n", dbPath)
	return nil
}

func (vm *VM) ExecuteShowDatabases() ([]string, error) {
	entries, err := os.ReadDir(DB_ROOT)
	if err != nil {
		return nil, fmt.Errorf("failed to read DB root directory: %w", err)
	}

	var databases []string

	for _, entry := range entries {
		if entry.IsDir() {
			databases = append(databases, entry.Name())
		}
	}

	return databases, nil
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
