package parser

import (
	lex "DaemonDB/query_parser/lexer"
	"strings"
)

func (p *Parser) parseSelect() (*SelectStmt, error) {
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

	if err := p.expect(lex.FROM); err != nil {
		return nil, err
	}
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
		var err error
		joinTable, joinType, leftCol, rightCol, err = p.parseJoin()
		if err != nil {
			return nil, err
		}
	}

	var whereCol, whereVal string
	if p.curToken.Kind == lex.WHERE {
		p.nextToken()
		whereCol = p.parseQualifiedIdentifier()
		if err := p.expect(lex.EQUAL); err != nil {
			return nil, err
		}
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
	}, nil
}

func (p *Parser) parseJoin() (joinTable, joinType, leftCol, rightCol string, err error) {
	joinType = ""
	if p.curToken.Kind == lex.INNER || p.curToken.Kind == lex.LEFT || p.curToken.Kind == lex.RIGHT || p.curToken.Kind == lex.FULL {
		joinType = p.curToken.Value
		p.nextToken()
	}

	if err = p.expect(lex.JOIN); err != nil {
		return "", "", "", "", err
	}
	p.nextToken()

	joinTable = strings.TrimSpace(p.curToken.Value)
	p.nextToken()

	if err = p.expect(lex.ON); err != nil {
		return "", "", "", "", err
	}
	p.nextToken()

	leftCol = p.parseQualifiedIdentifier()
	if err = p.expect(lex.EQUAL); err != nil {
		return "", "", "", "", err
	}
	p.nextToken()
	rightCol = p.parseQualifiedIdentifier()

	return joinTable, joinType, leftCol, rightCol, nil
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
