package parser

import (
	lex "DaemonDB/query_parser/lexer"
	"fmt"
	"strconv"
	"strings"
)

func (p *Parser) parseInsert() (*InsertStmt, error) {
	p.nextToken()
	if err := p.expect(lex.INTO); err != nil {
		return nil, err
	}
	p.nextToken()

	table := p.curToken.Value
	p.nextToken()

	if strings.ToUpper(p.curToken.Value) != "VALUES" {
		return nil, ErrExpectedValues
	}
	p.nextToken()

	if p.curToken.Kind != lex.OPENROUNDED {
		return nil, ErrExpectedParen
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
			return nil, fmt.Errorf("%w: got %s (%s)", ErrUnexpectedTokenInValues, p.curToken.Kind, p.curToken.Value)
		}
	}

	if p.curToken.Kind == lex.CLOSEDROUNDED {
		p.nextToken()
	}

	return &InsertStmt{Table: table, Values: values}, nil
}

func (p *Parser) parseDrop() (*DropStmt, error) {
	p.nextToken()
	table := p.curToken.Value
	p.nextToken()
	return &DropStmt{Table: table}, nil
}
func (p *Parser) parseUpdate() (*UpdateStmt, error) {
	stmt := &UpdateStmt{
		SetExprs: make(map[string]*ValueExpr),
	}

	p.nextToken()
	stmt.Table = p.curToken.Value
	p.nextToken()

	if err := p.expect(lex.SET); err != nil {
		return nil, err
	}

	p.nextToken()

	// Parse SET clauses (e.g., SET age = age + 1, name = 'John')
	for p.curToken.Kind != lex.WHERE && p.curToken.Kind != lex.END {
		colName := p.curToken.Value
		p.nextToken() // move to '='

		if p.curToken.Kind != lex.EQUAL {
			return nil, fmt.Errorf("expected '=' after column name, got %s", p.curToken.Value)
		}
		p.nextToken() // move to expression

		expr := p.parseExpression()
		stmt.SetExprs[colName] = expr

		if p.curToken.Kind == lex.COMMA {
			p.nextToken()
		}
	}

	// Parse WHERE clause
	if p.curToken.Kind == lex.WHERE {
		p.nextToken()
		stmt.WhereExpr = p.parseWhereExpression()
	}

	return stmt, nil
}

func (p *Parser) parseExpression() *ValueExpr {
	left := p.parsePrimary()

	for p.curToken.Kind == lex.PLUS ||
		p.curToken.Kind == lex.MINUS ||
		p.curToken.Kind == lex.ASTERISK ||
		p.curToken.Kind == lex.DIV {

		op := p.curToken.Value
		p.nextToken()

		right := p.parsePrimary()

		left = &ValueExpr{
			Type:  EXPR_BINARY,
			Left:  left,
			Right: right,
			Op:    op,
		}
	}

	return left
}

func (p *Parser) parseWhereExpression() *ValueExpr {
	left := p.parsePrimary()

	// Handle comparison operators (=, !=, <, >, <=, >=)
	if p.curToken.Kind == lex.EQUAL ||
		p.curToken.Kind == lex.NOTEQUAL ||
		p.curToken.Kind == lex.LESSTHAN ||
		p.curToken.Kind == lex.GREATERTHAN ||
		p.curToken.Kind == lex.LESSTHANEQUAL ||
		p.curToken.Kind == lex.GREATERTHANEQUAL {

		op := p.curToken.Value
		p.nextToken()

		right := p.parsePrimary()

		return &ValueExpr{
			Type:  EXPR_COMPARISON,
			Left:  left,
			Right: right,
			Op:    op,
		}
	}

	return left
}

func (p *Parser) parsePrimary() *ValueExpr {
	tok := p.curToken

	switch tok.Kind {
	case lex.INT:
		val, _ := strconv.Atoi(tok.Value)
		p.nextToken()
		return &ValueExpr{
			Type:    EXPR_LITERAL,
			Literal: val,
		}
	case lex.STRING:
		p.nextToken()
		return &ValueExpr{
			Type:    EXPR_LITERAL,
			Literal: tok.Value,
		}
	case lex.IDENT:
		p.nextToken()
		return &ValueExpr{
			Type:       EXPR_COLUMN,
			ColumnName: tok.Value,
		}
	case lex.OPENROUNDED:
		p.nextToken()
		expr := p.parseExpression()
		if p.curToken.Kind == lex.CLOSEDROUNDED {
			p.nextToken()
		}
		return expr
	}

	return nil
}
