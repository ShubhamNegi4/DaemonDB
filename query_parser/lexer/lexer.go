package lex

import (
	"strings"
)

type Lexer struct {
	input   string
	pos     int
	readPos int
	ch      byte
}

func New(input string) *Lexer {
	l := &Lexer{
		input:   input,
		pos:     0,
		readPos: 0,
		ch:      0,
	}
	l.readChar()
	return l
}

func (l *Lexer) NextToken() Token {
	l.skipWhiteSpaces()

	switch l.ch {
	case ',':
		tok := Token{Kind: COMMA, Value: string(l.ch)}
		l.readChar()
		return tok
	case '*':
		tok := Token{Kind: ASTERISK, Value: string(l.ch)}
		l.readChar()
		return tok
	case '=':
		tok := Token{Kind: EQUAL, Value: string(l.ch)}
		l.readChar()
		return tok
	case '{':
		tok := Token{Kind: OPENCURLY, Value: string(l.ch)}
		l.readChar()
		return tok
	case '}':
		tok := Token{Kind: CLOSECURLY, Value: string(l.ch)}
		l.readChar()
		return tok
	case '(':
		tok := Token{Kind: OPENROUNDED, Value: string(l.ch)}
		l.readChar()
		return tok
	case ')':
		tok := Token{Kind: CLOSEDROUNDED, Value: string(l.ch)}
		l.readChar()
		return tok
	case '"':
		str := l.readString()
		tok := Token{Kind: STRING, Value: str}
		return tok
	case 0:
		tok := Token{Kind: END, Value: ""}
		return tok
	default:
		if isLetter(l.ch) {
			str := l.keyIdentLookup() // str could be a keyword or an identifier
			return Token{Kind: KeyIdentKind(str), Value: str}
		} else if isNumber(l.ch) {
			return Token{Kind: INT, Value: l.readNumber()}
		} else {
			return Token{Kind: INVALID, Value: string(l.ch)}
		}
	}
}

func (l *Lexer) readChar() {
	if l.readPos >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.readPos]
	}
	l.pos = l.readPos
	l.readPos++
}

func (l *Lexer) skipWhiteSpaces() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' {
		l.readChar()
	}
}

func isLetter(ch byte) bool {
	return ('a' <= ch && ch <= 'z') || ('A' <= ch && ch <= 'Z')
}

func isNumber(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

func (l *Lexer) keyIdentLookup() string {
	start := l.pos
	for isLetter(l.ch) {
		l.readChar()
	}
	return l.input[start:l.pos]
}

func (l *Lexer) readNumber() string {
	start := l.pos
	for isNumber(l.ch) {
		l.readChar()
	}
	return l.input[start:l.pos]
}

func (l *Lexer) readString() string {
	l.readChar() // read start " of string
	start := l.pos
	for l.ch != '"' && l.ch != 0 { // read everything until closing "
		l.readChar()
	}
	str := l.input[start:l.pos]
	l.readChar() // read end " of string
	return str
}

func KeyIdentKind(str string) TokenKind {
	switch strings.ToUpper(str) {
	case "INSERT":
		return INSERT
	case "INTO":
		return INTO
	case "SELECT":
		return SELECT
	case "UPDATE":
		return UPDATE
	case "SET":
		return SET
	case "FROM":
		return FROM
	case "WHERE":
		return WHERE
	case "TABLE":
		return TABLE
	case "DATABASE":
		return DATABASE
	case "DATABASES":
		return DATABASES
	case "SHOW":
		return SHOW
	case "DROP":
		return DROP
	default:
		return IDENT
	}
}
