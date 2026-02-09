package parser

import (
	lex "DaemonDB/query_parser/lexer"
	"errors"
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

// expect returns an error if the current token is not of the expected kind (replaces panic).
func (p *Parser) expect(kind lex.TokenKind) error {
	if p.curToken.Kind != kind {
		return fmt.Errorf("expected %s, got %s (%s)", kind, p.curToken.Kind, p.curToken.Value)
	}
	return nil
}

// ParseStatement is the entry point; returns (nil, error) on parse error instead of panicking.
func (p *Parser) ParseStatement() (Statement, error) {
	switch p.curToken.Kind {

	case lex.BEGIN:
		p.nextToken()
		return &BeginTxnStmt{}, nil

	case lex.COMMIT:
		p.nextToken()
		return &CommitTxnStmt{}, nil

	case lex.ROLLBACK:
		p.nextToken()
		return &RollbackTxnStmt{}, nil

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

	return nil, fmt.Errorf("unexpected token: %s (%s)", p.curToken.Kind, p.curToken.Value)
}

var (
	ErrExpectedDatabaseName = errors.New("expected database name after USE")
	ErrExpectedKeyAfterForeign = errors.New("expected KEY after FOREIGN")
	ErrExpectedReferences = errors.New("expected REFERENCES in foreign key")
	ErrExpectedValues = errors.New("expected VALUES")
	ErrExpectedParen = errors.New("expected (")
	ErrUnexpectedTokenInValues = errors.New("unexpected token in values list")
)
