package codegen

import (
	executor "DaemonDB/query_executor"
	"DaemonDB/query_parser/parser"
	"DaemonDB/types"
	"encoding/json"
	"fmt"
)

// UPDATE QUERY HELPERS

func EmitUpdateBytecode(stmt *parser.UpdateStmt) ([]executor.Instruction, error) {
	instructions := []executor.Instruction{}

	payload := types.UpdatePayload{
		Table:     stmt.Table,
		SetExprs:  make(map[string]types.ExpressionNode),
		WhereExpr: nil,
	}

	for colName, expr := range stmt.SetExprs {
		payload.SetExprs[colName] = convertExprToNode(expr)
	}

	if stmt.WhereExpr != nil {
		whereNode := convertExprToNode(stmt.WhereExpr)
		payload.WhereExpr = &whereNode
	}

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize update payload: %w", err)
	}

	instructions = append(instructions, executor.Instruction{
		Op:    executor.OP_PUSH_VAL,
		Value: string(payloadJSON),
	})

	instructions = append(instructions, executor.Instruction{
		Op:    executor.OP_UPDATE,
		Value: stmt.Table,
	})

	return instructions, nil
}

// convertExprToNode converts parser.ValueExpr to executor.ExpressionNode
func convertExprToNode(expr *parser.ValueExpr) types.ExpressionNode {
	node := types.ExpressionNode{
		Type:    int(expr.Type),
		Literal: expr.Literal,
		Column:  expr.ColumnName,
		Op:      expr.Op,
	}

	if expr.Left != nil {
		leftNode := convertExprToNode(expr.Left)
		node.Left = &leftNode
	}

	if expr.Right != nil {
		rightNode := convertExprToNode(expr.Right)
		node.Right = &rightNode
	}

	return node
}
