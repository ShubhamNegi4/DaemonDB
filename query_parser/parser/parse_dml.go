package parser

import (
	lex "DaemonDB/query_parser/lexer"
	"fmt"
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
	p.nextToken()
	table := p.curToken.Value
	p.nextToken()

	if err := p.expect(lex.SET); err != nil {
		return nil, err
	}
	p.nextToken()

	assignments := map[string]string{}
	for p.curToken.Kind == lex.IDENT {
		col := p.curToken.Value
		p.nextToken()
		if err := p.expect(lex.EQUAL); err != nil {
			return nil, err
		}
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
	return &UpdateStmt{Table: table, Assignments: assignments}, nil
}
