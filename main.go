package main

import (
	bplus "DaemonDB/bplustree"
	executor "DaemonDB/query_executor"
	codegen "DaemonDB/query_parser/code-generator"
	lex "DaemonDB/query_parser/lexer"
	"DaemonDB/query_parser/parser"
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strings"
)

func main() {

	// Initialize B+ Tree with in-memory pager; table-specific on-disk indexes are opened per-table via GetOrCreateIndex
	pager := bplus.NewInMemoryPager()
	cache := bplus.NewBufferPool(10)
	tree := bplus.NewBPlusTree(pager, cache, bytes.Compare)

	vm := executor.NewVM(tree, nil, nil)
	defer vm.CloseIndexCache()

	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("Welcome to DaemonDB!")
	fmt.Println("Please use 'USE <database>' or 'SHOW DATABASES' or 'CREATE DATABASE <database>' to begin.")
	fmt.Println("Type 'help' for more commands.")
	// REPL
	for {
		fmt.Print("daemon> ")

		if !scanner.Scan() { // Ctrl+D pressed
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if strings.EqualFold(line, "exit") {
			break
		}
		if line == "" {
			continue
		}
		if line == "?" || strings.EqualFold(line, "help") {
			printHelp()
			continue
		}

		query := line

		// Lexer + Parser
		l := lex.New(query)
		p := parser.New(l)

		stmt, err := p.ParseStatement()
		if err != nil {
			fmt.Printf("Parse error: %v\n", err)
			continue
		}

		fmt.Println("\n=== AST ===")
		fmt.Printf("%#v", stmt)

		fmt.Println("\n\n=== Bytecode ===")

		instructions, err := codegen.EmitBytecode(stmt)
		if err != nil {
			fmt.Printf("Codegen error: %v\n", err)
			continue
		}
		for i, instr := range instructions {
			fmt.Printf("%d: OP=%v, VALUE=%v\n", i, instr.Op, instr.Value)
		}

		fmt.Println("\n=== Execution ===")
		if err := vm.Execute(instructions); err != nil {
			fmt.Printf("Execution error: %v\n", err)
		}
	}
}

func printHelp() {
	fmt.Println("Supported commands:")
	fmt.Println("  SHOW DATABASES")
	fmt.Println("  CREATE DATABASE <name>")
	fmt.Println("  USE <database>")
	fmt.Println("  CREATE TABLE <name> ( col type [primary key], ... )")
	fmt.Println("  INSERT INTO <table> VALUES ( val1, val2, ... )")
	fmt.Println("  SELECT * FROM <table> [ WHERE col = value ]")
	fmt.Println("  SELECT * FROM t1 [ INNER|LEFT|RIGHT|FULL ] JOIN t2 ON col1 = col2 [ WHERE ... ]")
	fmt.Println("  BEGIN; COMMIT; ROLLBACK")
	fmt.Println("  exit")
	fmt.Println("Note: UPDATE/DELETE/DROP are parsed but not executed yet.")
}
