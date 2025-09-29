package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	lex "query-parser/lexer"
	"query-parser/parser"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	inputLines := []string{}

	for {
		if !scanner.Scan() { // Ctrl+D pressed
			break
		}
		line := scanner.Text()
		inputLines = append(inputLines, line)
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "Error reading input:", err)
		return
	}

	query := strings.Join(inputLines, " ")

	// Lexer + Parser
	l := lex.New(query)
	p := parser.New(l)

	stmt := p.ParseStatement()

	fmt.Println("=== AST ===")
	fmt.Printf("%#v\n", stmt)

	fmt.Println("=== Bytecode ===")
	emitBytecode(stmt)
}

func emitBytecode(stmt parser.Statement) {
	switch s := stmt.(type) {
	case *parser.SelectStmt:
		fmt.Println("PUSH_COLS", s.Columns)
		fmt.Println("SCAN", s.Table)
	case *parser.CreateTableStmt:
		fmt.Println("CREATE_TABLE", s.TableName)
		for _, c := range s.Columns {
			fmt.Println("  ADD_COL", c.Name, c.Type)
		}
	case *parser.InsertStmt:
		fmt.Println("INSERT", s.Table)
		for _, v := range s.Values {
			fmt.Println("  PUSH_VAL", v)
		}
	case *parser.DropStmt:
		fmt.Println("DROP_TABLE", s.Table)
	case *parser.UpdateStmt:
		fmt.Println("UPDATE", s.Table)
		for k, v := range s.Assignments {
			fmt.Println("  SET", k, v)
		}
	default:
		fmt.Println("Unknown statement")
	}
}
