package parser

import (
	lex "DaemonDB/query_parser/lexer"
	"fmt"
	"strings"
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

	// 🔹 TRANSACTIONS (NEW)
	case lex.BEGIN:
		p.nextToken()
		return &BeginTxnStmt{}

	case lex.COMMIT:
		p.nextToken()
		return &CommitTxnStmt{}

	case lex.ROLLBACK:
		p.nextToken()
		return &RollbackTxnStmt{}

	// 🔹 EXISTING
	case lex.SHOW:
		return p.parseShowDatabases()
	case lex.SELECT:
		return p.parseSelect()
	case lex.INSERT:
		return p.parseInsert()
	case lex.UPDATE:
		return p.parseUpdate()
	case lex.USE:
		return p.parseUseDatabase()
	case lex.DROP:
		return p.parseDrop()
	case lex.IDENT: // CREATE TABLE starts with "create"
		if p.curToken.Value == "create" || p.curToken.Value == "CREATE" {
			p.nextToken() // consume create
			fmt.Print(p.curToken)
			switch p.curToken.Value {
			case "database", "DATABASE":
				return p.parseCreateDatabase()
			case "table", "TABLE":
				return p.parseCreateTable()
			}
		}
	}

	panic(fmt.Sprintf("unexpected token: %s (%s)", p.curToken.Kind, p.curToken.Value))
}

/*


-------------------parser functions implementation-------------------



*/
// --- CREATE DATABASE ---
func (p *Parser) parseCreateDatabase() *CreateDatabaseStmt {
	p.nextToken() // curtoken is <databasename>
	dbName := p.curToken.Value
	return &CreateDatabaseStmt{DbName: dbName}
}

// --- SHOW DATABASES ---
func (p *Parser) parseShowDatabases() *ShowDatabasesStmt {
	// No additional tokens required
	p.nextToken()
	print(p.curToken.Kind, p.curToken.Value)
	p.expect(lex.DATABASES)
	return &ShowDatabasesStmt{}
}

// --- USE DATABASE ---
func (p *Parser) parseUseDatabase() *UseDatabaseStatement {
	p.nextToken() // move to the database name

	if p.curToken.Kind != lex.IDENT {
		panic("expected database name after USE")
	}

	dbName := p.curToken.Value

	p.nextToken() // consume db name

	return &UseDatabaseStatement{DbName: dbName}
}

// --- CREATE TABLE ---
// --- CREATE TABLE ---
func (p *Parser) parseCreateTable() *CreateTableStmt {

	p.nextToken() // curToken is <tablename>
	table := p.curToken.Value
	p.nextToken()

	p.expect(lex.OPENROUNDED)
	p.nextToken()

	cols := []ColumnDef{}
	fks := []ForeignKeyDef{}

	for p.curToken.Kind != lex.CLOSEDROUNDED {

		// ================= FOREIGN KEY =================
		if p.curToken.Kind == lex.IDENT &&
			strings.EqualFold(p.curToken.Value, "foreign") {

			p.nextToken() // FOREIGN

			if !(p.curToken.Kind == lex.IDENT &&
				strings.EqualFold(p.curToken.Value, "key")) {
				panic("expected KEY after FOREIGN")
			}
			p.nextToken() // KEY

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

		// ================= COLUMN =================
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
		whereCol = p.parseQualifiedIdentifier() // could be a simple colname or table.colname in join
		p.expect(lex.EQUAL)
		p.nextToken()
		whereVal = p.curToken.Value
		p.nextToken()
	}

	return &SelectStmt{Columns: cols, Table: table, WhereCol: whereCol, WhereValue: whereVal, JoinTable: joinTable, JoinType: joinType, LeftCol: leftCol, Rightcol: rightCol}
}

// --- INSERT ---
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

	// Only consume CLOSEDROUNDED if present
	if p.curToken.Kind == lex.CLOSEDROUNDED {
		p.nextToken()
	}

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

// -- JOIN --
func (p *Parser) parseJoin() (joinTable, joinType, leftCol, rightCol string) {
	fmt.Println("parsing join")
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

	// join condition: table1.col = table2.col
	leftCol = p.parseQualifiedIdentifier()
	p.expect(lex.EQUAL)
	p.nextToken()
	rightCol = p.parseQualifiedIdentifier()

	return joinTable, joinType, leftCol, rightCol
}

func (p *Parser) parseQualifiedIdentifier() string {
	ident := p.curToken.Value
	p.nextToken()

	// If the next token is a dot, consume it and the next part
	if p.curToken.Kind == lex.DOT || p.curToken.Value == "." {
		p.nextToken()
		ident = ident + "." + p.curToken.Value
		p.nextToken()
	}
	return ident
}
