package codegen

import (
	executor "DaemonDB/query_executor"
	"DaemonDB/query_parser/parser"
	"encoding/json"
	"fmt"
	"strings"
)

func EmitBytecode(stmt parser.Statement) ([]executor.Instruction, error) {

	instructions := []executor.Instruction{}

	switch s := stmt.(type) {

	case *parser.BeginTxnStmt:
		instructions = append(instructions, executor.Instruction{
			Op: executor.OP_TXN_BEGIN,
		})

	case *parser.CommitTxnStmt:
		instructions = append(instructions, executor.Instruction{
			Op: executor.OP_TXN_COMMIT,
		})

	case *parser.RollbackTxnStmt:
		instructions = append(instructions, executor.Instruction{
			Op: executor.OP_TXN_ROLLBACK,
		})

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

		// -------- Build column schema --------
		cols := []string{}
		for _, col := range s.Columns {
			segment := col.Type + ":" + col.Name
			if col.IsPrimaryKey {
				segment += ":pk"
			}
			cols = append(cols, segment)
		}

		// -------- Build full schema payload (with foreign keys) --------
		payload := struct {
			Columns     string                 `json:"columns"`
			ForeignKeys []parser.ForeignKeyDef `json:"foreign_keys,omitempty"`
		}{
			Columns:     strings.Join(cols, ","),
			ForeignKeys: s.ForeignKeys,
		}

		payloadJSON, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize table schema: %w", err)
		}

		// Push schema payload
		instructions = append(instructions, executor.Instruction{
			Op:    executor.OP_PUSH_VAL,
			Value: string(payloadJSON),
		})

		// Create table
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
		whereCol := s.WhereCol
		whereVal := s.WhereValue
		// package select metadata as JSON for executor
		payload := executor.SelectPayload{
			Table:     s.Table,
			WhereCol:  whereCol,
			WhereVal:  whereVal,
			JoinTable: s.JoinTable,
			JoinType:  s.JoinType,
			LeftCol:   s.LeftCol,
			RightCol:  s.Rightcol,
		}
		payloadJSON, _ := json.Marshal(payload)
		// Execute select
		instructions = append(instructions, executor.Instruction{
			Op:    executor.OP_PUSH_VAL,
			Value: cols,
		})

		instructions = append(instructions, executor.Instruction{
			Op:    executor.OP_SELECT,
			Value: string(payloadJSON),
		})

	case *parser.UpdateStmt:
		fmt.Println("UPDATE", s.Table)
		updateInstructions, err := EmitUpdateBytecode(s)
		if err != nil {
			return nil, err
		}
		instructions = append(instructions, updateInstructions...)
	default:
		return nil, fmt.Errorf("unknown statement type (no bytecode emitted)")
	}

	// for END of queries
	instructions = append(instructions, executor.Instruction{
		Op: executor.OP_END,
	})
	return instructions, nil
}
