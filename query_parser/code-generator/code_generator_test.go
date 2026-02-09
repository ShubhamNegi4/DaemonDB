package codegen

import (
	lex "DaemonDB/query_parser/lexer"
	"DaemonDB/query_parser/parser"
	"testing"
)

// TestEmitBytecode_UnsupportedStatement_ReturnsError ensures that statements
// not yet implemented in the codegen (e.g. DROP, UPDATE) return an error instead of panicking.
func TestEmitBytecode_UnsupportedStatement_ReturnsError(t *testing.T) {
	l := lex.New("DROP TABLE x")
	p := parser.New(l)
	stmt, err := p.ParseStatement()
	if err != nil {
		t.Fatalf("parse DROP TABLE: %v", err)
	}
	instructions, err := EmitBytecode(stmt)
	if err == nil {
		t.Errorf("EmitBytecode(DropStmt) expected error, got %d instructions", len(instructions))
	}
	if instructions != nil {
		t.Errorf("EmitBytecode(DropStmt) expected nil instructions on error, got %v", instructions)
	}
}

// TestEmitBytecode_ValidStatement_NoError ensures valid statements return bytecode and no error.
func TestEmitBytecode_ValidStatement_NoError(t *testing.T) {
	l := lex.New("SHOW DATABASES")
	p := parser.New(l)
	stmt, err := p.ParseStatement()
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	instructions, err := EmitBytecode(stmt)
	if err != nil {
		t.Errorf("EmitBytecode(ShowDatabasesStmt) unexpected error: %v", err)
	}
	if len(instructions) == 0 {
		t.Error("EmitBytecode expected at least one instruction (OP_END)")
	}
}
