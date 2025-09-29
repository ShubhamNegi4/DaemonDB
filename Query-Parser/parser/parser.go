package parser

import (
	"fmt"
	lex "query-parser/lexer"
)

type Parser struct {
	l         *lex.Lexer
	curToken  lex.Token
	peekToken lex.Token
}

func New(l *lex.Lexer) *Parser {
	p := &Parser{l: l}
	p.nextToken()
	p.nextToken()
	return p
}

func (p *Parser) nextToken() {
	p.curToken = p.peekToken
	p.peekToken = p.l.NextToken()
}

func (p *Parser) expect(kind lex.TokenKind) {
	if p.curToken.Kind != kind {
		panic(fmt.Sprintf("expected %s, got %s (%s)", kind, p.curToken.Kind, p.curToken.Value))
	}
}

// Entry point
func (p *Parser) ParseStatement() Statement {
	switch p.curToken.Kind {
	case lex.SELECT:
		return p.parseSelect()
	case lex.INSERT:
		return p.parseInsert()
	case lex.UPDATE:
		return p.parseUpdate()
	case lex.DROP:
		return p.parseDrop()
	case lex.IDENT: // CREATE TABLE starts with "create"
		if p.curToken.Value == "create" || p.curToken.Value == "CREATE" {
			return p.parseCreateTable()
		}
	}
	panic(fmt.Sprintf("unexpected token: %s (%s)", p.curToken.Kind, p.curToken.Value))
}

// --- SELECT ---
func (p *Parser) parseSelect() *SelectStmt {
	p.nextToken() // consume SELECT

	cols := []string{}
	if p.curToken.Kind == lex.ASTERISK {
		cols = append(cols, "*")
		p.nextToken()
	} else {
		for p.curToken.Kind == lex.IDENT {
			cols = append(cols, p.curToken.Value)
			p.nextToken()
			if p.curToken.Kind == lex.COMMA {
				p.nextToken()
			} else {
				break
			}
		}
	}

	p.expect(lex.FROM)
	p.nextToken()
	table := p.curToken.Value
	p.nextToken()

	return &SelectStmt{Columns: cols, Table: table}
}

// --- CREATE TABLE ---
func (p *Parser) parseCreateTable() *CreateTableStmt {
	// curToken is "create"
	p.nextToken()
	p.expect(lex.TABLE)
	p.nextToken()

	table := p.curToken.Value
	p.nextToken()

	p.expect(lex.OPENCURLY)
	p.nextToken()

	cols := []ColumnDef{}
	for p.curToken.Kind == lex.IDENT {
		name := p.curToken.Value
		p.nextToken()
		typ := p.curToken.Value
		p.nextToken()
		cols = append(cols, ColumnDef{Name: name, Type: typ})

		if p.curToken.Kind == lex.COMMA {
			p.nextToken()
		} else {
			break
		}
	}

	p.expect(lex.CLOSECURLY)
	p.nextToken()

	return &CreateTableStmt{TableName: table, Columns: cols}
}

// --- INSERT ---
func (p *Parser) parseInsert() *InsertStmt {
	p.nextToken()
	p.expect(lex.INTO)
	p.nextToken()

	table := p.curToken.Value
	p.nextToken()

	// VALUES keyword
	if p.curToken.Value != "values" && p.curToken.Value != "VALUES" {
		panic("expected VALUES")
	}
	p.nextToken()

	p.expect(lex.OPENROUNDED)
	p.nextToken()

	values := []string{}
	for p.curToken.Kind == lex.STRING || p.curToken.Kind == lex.INT {
		values = append(values, p.curToken.Value)
		p.nextToken()
		if p.curToken.Kind == lex.COMMA {
			p.nextToken()
		} else {
			break
		}
	}

	p.expect(lex.CLOSEDROUNDED)
	p.nextToken()

	return &InsertStmt{Table: table, Values: values}
}

// --- DROP ---
func (p *Parser) parseDrop() *DropStmt {
	p.nextToken()
	table := p.curToken.Value
	p.nextToken()
	return &DropStmt{Table: table}
}

// --- UPDATE ---
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
