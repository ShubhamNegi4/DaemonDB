package executor

import (
	"DaemonDB/types"
	"encoding/json"
	"fmt"
	"strings"
)

/*
This file contains update query for the table
the vm function does the pre processing like unmarshling the payload sent in the query
begins an auto transaction
scan for the row pointers that are present for the target table, and do the necessary updates in those rows

includes the helper functions for update set clause like expression evaluator, value comparator, type converter
*/

// ExecuteUpdate handles UPDATE statements
func (vm *VM) ExecuteUpdate(tableName string) error {
	if err := vm.storageEngine.RequireDatabase(); err != nil {
		return fmt.Errorf("no database selected. Run: USE <dbname>")
	}

	if len(vm.stack) == 0 {
		return fmt.Errorf("stack underflow: no update payload")
	}
	payloadJSON := vm.stack[len(vm.stack)-1]
	vm.stack = vm.stack[:len(vm.stack)-1]

	var updatePayload types.UpdatePayload
	if err := json.Unmarshal(payloadJSON, &updatePayload); err != nil {
		return fmt.Errorf("invalid update payload: %w", err)
	}

	// Auto Commit Command
	if vm.currentTxn == nil { // check if there is no running transaction
		err := vm.autoTransactionBegin()
		if err != nil {
			return fmt.Errorf("failed to auto-begin transaction: %w", err)
		}
	}

	rowPtrs, tableSchema, err := vm.storageEngine.Scan(vm.currentTxn, tableName)
	if err != nil {
		return err
	}

	updatedCount := 0

	// Iterate through all rows
	for _, row := range rowPtrs {
		rowData := row.ToMap()

		// Evaluate WHERE condition
		if updatePayload.WhereExpr != nil {
			match, err := vm.evaluateExpression(updatePayload.WhereExpr, rowData, tableSchema)
			if err != nil {
				return fmt.Errorf("error evaluating WHERE: %w", err)
			}
			if !match {
				continue
			}
		}

		// Apply SET expressions
		newRow := row.Row.Clone()

		for colName, expr := range updatePayload.SetExprs {
			val, err := vm.evaluateExpressionValue(&expr, rowData, tableSchema)
			if err != nil {
				return err
			}
			newRow.Set(colName, val)
		}

		if err := vm.storageEngine.UpdateRow(vm.currentTxn, tableName, row.Pointer, newRow); err != nil {
			if vm.autoTxn {
				_ = vm.autoTransactionAbort()
			}
			return fmt.Errorf("failed to update row: %w", err)
		}

		updatedCount++
	}

	if vm.autoTxn == true {
		if err := vm.autoTransactionCommit(); err != nil {
			return fmt.Errorf("failed to auto-commit: %w", err)
		}
	}

	fmt.Printf("%d row(s) updated\n", updatedCount)
	return nil
}

// evaluateExpression evaluates a comparison expression and returns true/false
func (vm *VM) evaluateExpression(expr *types.ExpressionNode, rowData map[string]interface{}, schema types.TableSchema) (bool, error) {
	if expr.Type != 3 { // EXPR_COMPARISON
		return false, fmt.Errorf("WHERE expression must be a comparison")
	}

	leftVal, err := vm.evaluateExpressionValue(expr.Left, rowData, schema)
	if err != nil {
		return false, err
	}

	rightVal, err := vm.evaluateExpressionValue(expr.Right, rowData, schema)
	if err != nil {
		return false, err
	}

	return vm.compareValues(leftVal, rightVal, expr.Op)
}

// evaluateExpressionValue evaluates an expression and returns its value
func (vm *VM) evaluateExpressionValue(expr *types.ExpressionNode, rowData map[string]interface{}, schema types.TableSchema) (interface{}, error) {
	switch expr.Type {
	case 0: // EXPR_LITERAL
		return expr.Literal, nil

	case 1: // EXPR_COLUMN
		colName := strings.ToLower(expr.Column)
		val, ok := rowData[colName]
		if !ok {
			return nil, fmt.Errorf("column %s not found", expr.Column)
		}
		return val, nil

	case 2: // EXPR_BINARY (arithmetic)
		leftVal, err := vm.evaluateExpressionValue(expr.Left, rowData, schema)
		if err != nil {
			return nil, err
		}

		rightVal, err := vm.evaluateExpressionValue(expr.Right, rowData, schema)
		if err != nil {
			return nil, err
		}

		return vm.applyArithmeticOp(leftVal, rightVal, expr.Op)

	default:
		return nil, fmt.Errorf("unsupported expression type: %d", expr.Type)
	}
}

// applyArithmeticOp applies arithmetic operations (+, -, *, /)
func (vm *VM) applyArithmeticOp(left, right interface{}, op string) (interface{}, error) {
	// Convert to int64 for arithmetic
	leftInt, leftOk := vm.toInt64(left)
	rightInt, rightOk := vm.toInt64(right)

	if !leftOk || !rightOk {
		return nil, fmt.Errorf("arithmetic operations require numeric values")
	}

	switch op {
	case "+":
		return leftInt + rightInt, nil
	case "-":
		return leftInt - rightInt, nil
	case "*":
		return leftInt * rightInt, nil
	case "/":
		if rightInt == 0 {
			return nil, fmt.Errorf("division by zero")
		}
		return leftInt / rightInt, nil
	default:
		return nil, fmt.Errorf("unknown operator: %s", op)
	}
}

// compareValues compares two values using the given operator
func (vm *VM) compareValues(left, right interface{}, op string) (bool, error) {
	switch op {
	case "=":
		return vm.equalValues(left, right), nil
	case "!=":
		return !vm.equalValues(left, right), nil
	case "<":
		return vm.lessThan(left, right)
	case ">":
		return vm.lessThan(right, left)
	case "<=":
		lt, err := vm.lessThan(left, right)
		if err != nil {
			return false, err
		}
		return lt || vm.equalValues(left, right), nil
	case ">=":
		lt, err := vm.lessThan(left, right)
		if err != nil {
			return false, err
		}
		return !lt || vm.equalValues(left, right), nil
	default:
		return false, fmt.Errorf("unknown comparison operator: %s", op)
	}
}

// equalValues checks if two values are equal
func (vm *VM) equalValues(left, right interface{}) bool {
	// Handle string comparison
	leftStr, leftIsStr := left.(string)
	rightStr, rightIsStr := right.(string)
	if leftIsStr && rightIsStr {
		return leftStr == rightStr
	}

	// Handle numeric comparison
	leftInt, leftOk := vm.toInt64(left)
	rightInt, rightOk := vm.toInt64(right)
	if leftOk && rightOk {
		return leftInt == rightInt
	}

	return false
}

// lessThan checks if left < right
func (vm *VM) lessThan(left, right interface{}) (bool, error) {
	// String comparison
	leftStr, leftIsStr := left.(string)
	rightStr, rightIsStr := right.(string)
	if leftIsStr && rightIsStr {
		return leftStr < rightStr, nil
	}

	// Numeric comparison
	leftInt, leftOk := vm.toInt64(left)
	rightInt, rightOk := vm.toInt64(right)
	if leftOk && rightOk {
		return leftInt < rightInt, nil
	}

	return false, fmt.Errorf("cannot compare values of different types")
}

// toInt64 converts various numeric types to int64
func (vm *VM) toInt64(val interface{}) (int64, bool) {
	switch v := val.(type) {
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case float64:
		return int64(v), true
	default:
		return 0, false
	}
}
