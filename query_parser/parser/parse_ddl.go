package parser

import (
	lex "DaemonDB/query_parser/lexer"
	"strings"
)

func (p *Parser) parseCreateDatabase() (*CreateDatabaseStmt, error) {
	p.nextToken()
	dbName := p.curToken.Value
	return &CreateDatabaseStmt{DbName: dbName}, nil
}

func (p *Parser) parseShowDatabases() (*ShowDatabasesStmt, error) {
	p.nextToken()
	if err := p.expect(lex.DATABASES); err != nil {
		return nil, err
	}
	return &ShowDatabasesStmt{}, nil
}

func (p *Parser) parseUseDatabase() (*UseDatabaseStatement, error) {
	p.nextToken()

	if p.curToken.Kind != lex.IDENT {
		return nil, ErrExpectedDatabaseName
	}

	dbName := p.curToken.Value
	p.nextToken()

	return &UseDatabaseStatement{DbName: dbName}, nil
}

func (p *Parser) parseCreateTable() (*CreateTableStmt, error) {
	p.nextToken()
	table := p.curToken.Value
	p.nextToken()

	if err := p.expect(lex.OPENROUNDED); err != nil {
		return nil, err
	}
	p.nextToken()

	cols := []ColumnDef{}
	fks := []ForeignKeyDef{}

	for p.curToken.Kind != lex.CLOSEDROUNDED {

		if p.curToken.Kind == lex.IDENT &&
			strings.EqualFold(p.curToken.Value, "foreign") {

			p.nextToken()

			if !(p.curToken.Kind == lex.IDENT &&
				strings.EqualFold(p.curToken.Value, "key")) {
				return nil, ErrExpectedKeyAfterForeign
			}
			p.nextToken()

			if err := p.expect(lex.OPENROUNDED); err != nil {
				return nil, err
			}
			p.nextToken()

			fkColumn := p.curToken.Value
			if err := p.expect(lex.IDENT); err != nil {
				return nil, err
			}
			p.nextToken()

			if err := p.expect(lex.CLOSEDROUNDED); err != nil {
				return nil, err
			}
			p.nextToken()

			if !(p.curToken.Kind == lex.IDENT &&
				strings.EqualFold(p.curToken.Value, "references")) {
				return nil, ErrExpectedReferences
			}
			p.nextToken()

			refTable := p.curToken.Value
			if err := p.expect(lex.IDENT); err != nil {
				return nil, err
			}
			p.nextToken()

			if err := p.expect(lex.OPENROUNDED); err != nil {
				return nil, err
			}
			p.nextToken()

			refColumn := p.curToken.Value
			if err := p.expect(lex.IDENT); err != nil {
				return nil, err
			}
			p.nextToken()

			if err := p.expect(lex.CLOSEDROUNDED); err != nil {
				return nil, err
			}
			p.nextToken()

			fks = append(fks, ForeignKeyDef{
				Column:    fkColumn,
				RefTable:  refTable,
				RefColumn: refColumn,
			})

			if p.curToken.Kind == lex.COMMA {
				p.nextToken()
			}
			continue
		}

		name := p.curToken.Value
		if err := p.expect(lex.IDENT); err != nil {
			return nil, err
		}
		p.nextToken()

		typ := p.curToken.Value
		if err := p.expect(lex.IDENT); err != nil {
			return nil, err
		}
		p.nextToken()

		isPK := false
		if p.curToken.Kind == lex.IDENT &&
			strings.EqualFold(p.curToken.Value, "primary") {

			p.nextToken()
			if p.curToken.Kind == lex.IDENT &&
				strings.EqualFold(p.curToken.Value, "key") {
				isPK = true
				p.nextToken()
			}
		}

		cols = append(cols, ColumnDef{
			Name:         name,
			Type:         typ,
			IsPrimaryKey: isPK,
		})

		if p.curToken.Kind == lex.COMMA {
			p.nextToken()
		}
	}

	if err := p.expect(lex.CLOSEDROUNDED); err != nil {
		return nil, err
	}
	p.nextToken()

	return &CreateTableStmt{
		TableName:   table,
		Columns:     cols,
		ForeignKeys: fks,
	}, nil
}
