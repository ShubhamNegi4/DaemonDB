package executor

import (
	"DaemonDB/types"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ExecuteUpdate handles UPDATE statements
func (vm *VM) ExecuteUpdate(tableName string) error {
	if vm.currDb == "" {
		return fmt.Errorf("no database selected. Run: USE <dbname>")
	}

	if len(vm.stack) == 0 {
		return fmt.Errorf("stack underflow: no update payload")
	}
	payloadJSON := vm.stack[len(vm.stack)-1]
	vm.stack = vm.stack[:len(vm.stack)-1]

	var payload UpdatePayload
	if err := json.Unmarshal(payloadJSON, &payload); err != nil {
		return fmt.Errorf("invalid update payload: %w", err)
	}

	// Auto Commit Command
	if vm.currentTxn == nil { // check if there is no running transaction
		err := vm.autoTransactionBegin()
		if err != nil {
			return fmt.Errorf("failed to auto commit: %w", err)
		}
	}

	// Load table schema
	schemaPath := filepath.Join(DB_ROOT, vm.currDb, "tables", tableName+"_schema.json")
	data, err := os.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("table not found %s: %w", tableName, err)
	}

	var schema types.TableSchema
	if err := json.Unmarshal(data, &schema); err != nil {
		return fmt.Errorf("invalid schema: %w", err)
	}

	// Get heap file
	fileID, ok := vm.tableToFileId[tableName]
	if !ok {
		return fmt.Errorf("heap file not found for table '%s'", tableName)
	}

	hf, err := vm.heapfileManager.GetHeapFileByID(fileID)
	if err != nil {
		return fmt.Errorf("failed to get heapfile: %w", err)
	}

	// Get all row pointers
	rowPtrs := hf.GetAllRowPointers()
	if len(rowPtrs) == 0 {
		fmt.Println("table is empty, no rows to update")
		if vm.autoTxn {
			return vm.autoTransactionEnd() // Close the auto txn just opened
		}
		return nil
	}

	updatedCount := 0

	// Iterate through all rows
	for _, rp := range rowPtrs {
		raw, err := vm.heapfileManager.GetRow(rp)
		if err != nil {
			fmt.Printf("error reading row (Page %d Slot %d): %v\n", rp.PageNumber, rp.SlotIndex, err)
			continue
		}

		values, err := vm.DeserializeRow(raw, schema.Columns)
		if err != nil {
			fmt.Printf("error deserializing row (Page %d Slot %d): %v\n", rp.PageNumber, rp.SlotIndex, err)
			continue
		}

		// Create a map of column name -> value for easy access
		rowData := make(map[string]interface{})
		for i, col := range schema.Columns {
			rowData[strings.ToLower(col.Name)] = values[i]
		}

		// Evaluate WHERE condition
		if payload.WhereExpr != nil {
			match, err := vm.evaluateExpression(payload.WhereExpr, rowData, schema)
			if err != nil {
				return fmt.Errorf("error evaluating WHERE: %w", err)
			}
			if !match {
				continue
			}
		}

		// Apply SET expressions
		modified := false
		for colName, expr := range payload.SetExprs {
			newValue, err := vm.evaluateExpressionValue(&expr, rowData, schema)
			if err != nil {
				return fmt.Errorf("error evaluating SET expression for %s: %w", colName, err)
			}

			// Find column index
			colIdx := -1
			for i, col := range schema.Columns {
				if strings.EqualFold(col.Name, colName) {
					colIdx = i
					break
				}
			}

			if colIdx == -1 {
				return fmt.Errorf("column %s not found in table", colName)
			}

			// Update the value
			values[colIdx] = newValue
			rowData[strings.ToLower(colName)] = newValue
			modified = true
		}

		if !modified {
			continue
		}

		// Serialize updated row
		newRow, err := vm.SerializeRow(schema.Columns, values)
		if err != nil {
			return fmt.Errorf("failed to serialize updated row: %w", err)
		}

		// Log to WAL
		op := &types.Operation{
			Type:    types.OpUpdate,
			TxnID:   vm.currentTxn.ID,
			Table:   tableName,
			RowData: newRow,
			RowPtr:  rp,
		}
		lsn, err := vm.WalManager.AppendOperation(op)
		op.LSN = lsn
		if err != nil {
			return fmt.Errorf("wal append failed: %w", err)
		}

		// Update the heap file
		if err := vm.heapfileManager.UpdateRow(rp, newRow, lsn); err != nil {
			return fmt.Errorf("failed to update row in heap: %w", err)
		}

		updatedCount++
	}

	// Sync WAL (if the db failed during the loop only, the whole command was ignored, this will keep the data as if the command never ran)
	if err := vm.WalManager.Sync(); err != nil {
		return fmt.Errorf("wal sync failed: %w", err)
	}

	if vm.autoTxn == true { // check if this command was autoCommited
		err := vm.autoTransactionEnd()
		if err != nil {
			return fmt.Errorf("failed to auto commit: %w", err)
		}
	}

	fmt.Printf("%d row(s) updated\n", updatedCount)
	return nil
}

// evaluateExpression evaluates a comparison expression and returns true/false
func (vm *VM) evaluateExpression(expr *ExpressionNode, rowData map[string]interface{}, schema types.TableSchema) (bool, error) {
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
func (vm *VM) evaluateExpressionValue(expr *ExpressionNode, rowData map[string]interface{}, schema types.TableSchema) (interface{}, error) {
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
