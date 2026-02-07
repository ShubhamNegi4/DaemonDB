package parser

import (
	lex "DaemonDB/query_parser/lexer"
	"strings"
)

func (p *Parser) parseCreateDatabase() *CreateDatabaseStmt {
	p.nextToken()
	dbName := p.curToken.Value
	return &CreateDatabaseStmt{DbName: dbName}
}

func (p *Parser) parseShowDatabases() *ShowDatabasesStmt {
	p.nextToken()
	p.expect(lex.DATABASES)
	return &ShowDatabasesStmt{}
}

func (p *Parser) parseUseDatabase() *UseDatabaseStatement {
	p.nextToken()

	if p.curToken.Kind != lex.IDENT {
		panic("expected database name after USE")
	}

	dbName := p.curToken.Value
	p.nextToken()

	return &UseDatabaseStatement{DbName: dbName}
}

func (p *Parser) parseCreateTable() *CreateTableStmt {
	p.nextToken()
	table := p.curToken.Value
	p.nextToken()

	p.expect(lex.OPENROUNDED)
	p.nextToken()

	cols := []ColumnDef{}
	fks := []ForeignKeyDef{}

	for p.curToken.Kind != lex.CLOSEDROUNDED {

		if p.curToken.Kind == lex.IDENT &&
			strings.EqualFold(p.curToken.Value, "foreign") {

			p.nextToken()

			if !(p.curToken.Kind == lex.IDENT &&
				strings.EqualFold(p.curToken.Value, "key")) {
				panic("expected KEY after FOREIGN")
			}
			p.nextToken()

			p.expect(lex.OPENROUNDED)
			p.nextToken()

			fkColumn := p.curToken.Value
			p.expect(lex.IDENT)
			p.nextToken()

			p.expect(lex.CLOSEDROUNDED)
			p.nextToken()

			if !(p.curToken.Kind == lex.IDENT &&
				strings.EqualFold(p.curToken.Value, "references")) {
				panic("expected REFERENCES in foreign key")
			}
			p.nextToken()

			refTable := p.curToken.Value
			p.expect(lex.IDENT)
			p.nextToken()

			p.expect(lex.OPENROUNDED)
			p.nextToken()

			refColumn := p.curToken.Value
			p.expect(lex.IDENT)
			p.nextToken()

			p.expect(lex.CLOSEDROUNDED)
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
		p.expect(lex.IDENT)
		p.nextToken()

		typ := p.curToken.Value
		p.expect(lex.IDENT)
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

	p.expect(lex.CLOSEDROUNDED)
	p.nextToken()

	return &CreateTableStmt{
		TableName:   table,
		Columns:     cols,
		ForeignKeys: fks,
	}
}
