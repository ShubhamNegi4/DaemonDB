// Package executor runs bytecode from the query parser code generator.
//
// Main entry: vm.go (NewVM, Execute).
// Statement execution: exec_create_db.go, exec_create_table.go, exec_insert.go, exec_select.go.
// Helpers: type_conv.go, serialization.go, table_mapping.go, joins.go, index.go, print.go.
// See structs.go, txn_manager.go, wal_replay.go for types and WAL recovery.
package executor
