package codegen

import (
	lex "DaemonDB/query_parser/lexer"
	"DaemonDB/query_parser/parser"
	"testing"
)

// TestEmitBytecode_DropTable_EmitsBytecode ensures DROP TABLE produces bytecode.
func TestEmitBytecode_DropTable_EmitsBytecode(t *testing.T) {
	l := lex.New("DROP TABLE x")
	p := parser.New(l)
	stmt, err := p.ParseStatement()
	if err != nil {
		t.Fatalf("parse DROP TABLE: %v", err)
	}
	instructions, err := EmitBytecode(stmt)
	if err != nil {
		t.Fatalf("EmitBytecode(DropStmt) unexpected error: %v", err)
	}
	if len(instructions) == 0 {
		t.Fatalf("EmitBytecode(DropStmt) expected instructions, got none")
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
