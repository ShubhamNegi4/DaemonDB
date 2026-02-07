package parser

import (
	lex "DaemonDB/query_parser/lexer"
	"strings"
)

func (p *Parser) parseSelect() *SelectStmt {
	p.nextToken()

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

	var joinTable, joinType, leftCol, rightCol string
	isJoin := p.curToken.Kind == lex.JOIN ||
		p.curToken.Kind == lex.INNER ||
		p.curToken.Kind == lex.LEFT ||
		p.curToken.Kind == lex.RIGHT ||
		p.curToken.Kind == lex.FULL ||
		p.curToken.Value == "JOIN" ||
		p.curToken.Value == "INNER"

	if isJoin {
		joinTable, joinType, leftCol, rightCol = p.parseJoin()
	}

	var whereCol, whereVal string
	if p.curToken.Kind == lex.WHERE {
		p.nextToken()
		whereCol = p.parseQualifiedIdentifier()
		p.expect(lex.EQUAL)
		p.nextToken()
		whereVal = p.curToken.Value
		p.nextToken()
	}

	return &SelectStmt{
		Columns:    cols,
		Table:      table,
		WhereCol:   whereCol,
		WhereValue: whereVal,
		JoinTable:  joinTable,
		JoinType:   joinType,
		LeftCol:    leftCol,
		Rightcol:   rightCol,
	}
}

func (p *Parser) parseJoin() (joinTable, joinType, leftCol, rightCol string) {
	joinType = ""
	if p.curToken.Kind == lex.INNER || p.curToken.Kind == lex.LEFT || p.curToken.Kind == lex.RIGHT || p.curToken.Kind == lex.FULL {
		joinType = p.curToken.Value
		p.nextToken()
	}

	p.expect(lex.JOIN)
	p.nextToken()

	joinTable = strings.TrimSpace(p.curToken.Value)
	p.nextToken()

	p.expect(lex.ON)
	p.nextToken()

	leftCol = p.parseQualifiedIdentifier()
	p.expect(lex.EQUAL)
	p.nextToken()
	rightCol = p.parseQualifiedIdentifier()

	return joinTable, joinType, leftCol, rightCol
}

func (p *Parser) parseQualifiedIdentifier() string {
	ident := p.curToken.Value
	p.nextToken()

	if p.curToken.Kind == lex.DOT || p.curToken.Value == "." {
		p.nextToken()
		ident = ident + "." + p.curToken.Value
		p.nextToken()
	}
	return ident
}
