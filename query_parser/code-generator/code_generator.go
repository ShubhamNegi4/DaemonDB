package codegen

import (
	executor "DaemonDB/query_executor"
	"DaemonDB/query_parser/parser"
	"fmt"
	"strings"
)

func EmitBytecode(stmt parser.Statement) []executor.Instruction {

	instructions := []executor.Instruction{}

	switch s := stmt.(type) {
	case *parser.InsertStmt:
		fmt.Println("INSERT", s.Table)

		// Push values onto stack (in reverse for correct order)
		for i := len(s.Values) - 1; i >= 0; i-- {
			fmt.Println("  PUSH_VAL", s.Values[i])
			instructions = append(instructions, executor.Instruction{
				Op:    executor.OP_PUSH_VAL,
				Value: s.Values[i],
			})
		}
		// Execute insert
		instructions = append(instructions, executor.Instruction{
			Op:    executor.OP_INSERT,
			Value: s.Table,
		})

	case *parser.SelectStmt:
		fmt.Println("SELECT", s.Table)
		cols := "*" // select all bydefault or in case of *
		if len(s.Columns) > 0 {
			cols = strings.Join(s.Columns, ",") // get columns (comma seperated)
		}
		fmt.Printf("  values: %s", cols)
		// Execute select
		instructions = append(instructions, executor.Instruction{
			Op:    executor.OP_SELECT,
			Value: cols,
		})

	default:
		fmt.Println("Unknown statement")
	}

	// for END of queries
	instructions = append(instructions, executor.Instruction{
		Op: executor.OP_END,
	})
	return instructions
}
