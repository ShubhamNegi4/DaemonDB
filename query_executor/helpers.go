// Helper logic for the executor is split into:
//   - type_conv.go: toInt, toString, toFloat, compareValues
//   - serialization.go: ValueToBytes, BytesToValue, SerializeRow, DeserializeRow, RowPointer
//   - table_mapping.go: loadTableRows, SaveTableFileMapping, LoadTableFileMapping, LoadAllTableSchemas
//   - joins.go: sortRowsByColumn, mergeSort*Join, filterJoinedRows, copyRowWithNulls
//   - index.go: ExtractPrimaryKey, GetOrCreateIndex, OpenBPlusTree
//   - print.go: PrintLine, PrintSeparator, formatValue
package executor
