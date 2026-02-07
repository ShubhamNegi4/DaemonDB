package parser

import (
	lex "DaemonDB/query_parser/lexer"
	"strings"
)

func (p *Parser) parseInsert() *InsertStmt {
	p.nextToken()
	p.expect(lex.INTO)
	p.nextToken()

	table := p.curToken.Value
	p.nextToken()

	if strings.ToUpper(p.curToken.Value) != "VALUES" {
		panic("expected VALUES")
	}
	p.nextToken()

	if p.curToken.Kind != lex.OPENROUNDED {
		panic("expected (")
	}
	p.nextToken()

	values := []string{}
	for p.curToken.Kind != lex.CLOSEDROUNDED && p.curToken.Kind != lex.END {
		switch p.curToken.Kind {
		case lex.STRING, lex.INT:
			values = append(values, p.curToken.Value)
			p.nextToken()
		case lex.COMMA:
			p.nextToken()
		default:
			panic("unexpected token in values list")
		}
	}

	if p.curToken.Kind == lex.CLOSEDROUNDED {
		p.nextToken()
	}

	return &InsertStmt{Table: table, Values: values}
}

func (p *Parser) parseDrop() *DropStmt {
	p.nextToken()
	table := p.curToken.Value
	p.nextToken()
	return &DropStmt{Table: table}
}

func (p *Parser) parseUpdate() *UpdateStmt {
	p.nextToken()
	table := p.curToken.Value
	p.nextToken()

	p.expect(lex.SET)
	p.nextToken()

	assignments := map[string]string{}
	for p.curToken.Kind == lex.IDENT {
		col := p.curToken.Value
		p.nextToken()
		p.expect(lex.EQUAL)
		p.nextToken()
		val := p.curToken.Value
		assignments[col] = val
		p.nextToken()
		if p.curToken.Kind == lex.COMMA {
			p.nextToken()
		} else {
			break
		}
	}
	return &UpdateStmt{Table: table, Assignments: assignments}
}
