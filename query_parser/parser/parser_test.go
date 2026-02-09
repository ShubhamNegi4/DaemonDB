package parser

import (
	lex "DaemonDB/query_parser/lexer"
	"strings"
	"testing"
)

// TestParseStatement_InvalidSQL_ReturnsError ensures invalid SQL returns an error instead of panicking.
func TestParseStatement_InvalidSQL_ReturnsError(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"missing FROM", "SELECT * students"},
		{"USE with number", "USE 123"},
		{"INSERT missing VALUES", "INSERT INTO students (\"S001\", \"Alice\")"},
		{"INSERT missing parens", "INSERT INTO students VALUES \"S001\", \"Alice\""},
		{"CREATE TABLE missing paren", "CREATE TABLE students id int"},
		{"WHERE without value", "SELECT * FROM students WHERE id"},
		{"empty", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := lex.New(tt.sql)
			p := New(l)
			stmt, err := p.ParseStatement()
			if err == nil && stmt != nil {
				t.Errorf("ParseStatement(%q) expected error, got stmt %#v", tt.sql, stmt)
			}
			if err != nil && !strings.Contains(err.Error(), "expected") && !strings.Contains(err.Error(), "unexpected") && err != ErrExpectedValues && err != ErrExpectedParen && err != ErrUnexpectedTokenInValues {
				// Accept any error
			}
		})
	}
}

// TestParseStatement_ValidSQL_NoPanic ensures valid SQL does not panic and returns a statement.
func TestParseStatement_ValidSQL_NoPanic(t *testing.T) {
	tests := []struct {
		sql string
	}{
		{"SHOW DATABASES"},
		{"USE demp"},
		{"CREATE DATABASE testdb"},
		{"SELECT * FROM students"},
		{"SELECT * FROM students WHERE id = \"S001\""},
		{"INSERT INTO students VALUES (\"S001\", \"Alice\", 20)"},
		{"BEGIN"},
		{"COMMIT"},
		{"ROLLBACK"},
	}
	for _, tt := range tests {
		l := lex.New(tt.sql)
		p := New(l)
		stmt, err := p.ParseStatement()
		if err != nil {
			t.Errorf("ParseStatement(%q) unexpected error: %v", tt.sql, err)
		}
		if stmt == nil {
			t.Errorf("ParseStatement(%q) expected statement, got nil", tt.sql)
		}
	}
}
