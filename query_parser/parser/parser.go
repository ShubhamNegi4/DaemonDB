package parser

import (
	lex "DaemonDB/query_parser/lexer"
	"fmt"
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

// ParseStatement is the entry point; dispatches to parse_ddl, parse_dml, parse_select.
func (p *Parser) ParseStatement() Statement {
	switch p.curToken.Kind {

	case lex.BEGIN:
		p.nextToken()
		return &BeginTxnStmt{}

	case lex.COMMIT:
		p.nextToken()
		return &CommitTxnStmt{}

	case lex.ROLLBACK:
		p.nextToken()
		return &RollbackTxnStmt{}

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
	case lex.IDENT:
		if p.curToken.Value == "create" || p.curToken.Value == "CREATE" {
			p.nextToken()
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
