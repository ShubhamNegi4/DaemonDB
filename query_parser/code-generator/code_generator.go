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

	case *parser.CreateDatabaseStmt:
		fmt.Println("CREATE DATABASE", s.DbName)

		instructions = append(instructions, executor.Instruction{
			Op:    executor.OP_CREATE_DB,
			Value: s.DbName,
		})

	case *parser.ShowDatabasesStmt:
		instructions = append(instructions, executor.Instruction{
			Op: executor.OP_SHOW_DB,
		})

	case *parser.UseDatabaseStatement:
		fmt.Println("USE DATABASE", s.DbName)

		instructions = append(instructions, executor.Instruction{
			Op:    executor.OP_USE_DB,
			Value: s.DbName,
		})

	case *parser.CreateTableStmt:

		// this cols will actually store the schema of the table
		cols := []string{}
		for _, col := range s.Columns {
			cols = append(cols, col.Type+":"+col.Name) // sepreate int:id
		}
		// join all the cols into a string, to get the schema of the table
		instructions = append(instructions, executor.Instruction{
			Op:    executor.OP_PUSH_VAL,
			Value: strings.Join(cols, ","), // id:int,name:varchar like this schema will be stored
		})

		instructions = append(instructions, executor.Instruction{
			Op:    executor.OP_CREATE_TABLE,
			Value: s.TableName,
		})

	case *parser.InsertStmt:
		fmt.Println("INSERT", s.Table)

		for _, val := range s.Values {
			fmt.Println("  PUSH_VAL", val)
			instructions = append(instructions, executor.Instruction{
				Op:    executor.OP_PUSH_VAL,
				Value: val,
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
			Op:    executor.OP_PUSH_VAL,
			Value: cols,
		})

		instructions = append(instructions, executor.Instruction{
			Op:    executor.OP_SELECT,
			Value: s.Table,
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
