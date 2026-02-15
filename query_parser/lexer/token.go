package lex

type TokenKind int

const (
	// identifier
	IDENT TokenKind = iota

	// keywords
	USE
	INSERT
	INTO
	SELECT
	UPDATE
	SET
	FROM
	WHERE
	PLUS
	MINUS
	MUL
	DIV
	INT
	VARCHAR
	COMMA
	ASTERISK
	EQUAL
	NOTEQUAL
	LESSTHAN
	GREATERTHAN
	LESSTHANEQUAL
	GREATERTHANEQUAL
	OPENCURLY
	CLOSECURLY
	OPENROUNDED
	CLOSEDROUNDED
	DROP

	// TRANSACTIONS (NEW)
	BEGIN
	COMMIT
	ROLLBACK

	END
	INVALID
	TABLE
	DATABASE
	DATABASES
	SHOW
	JOIN
	INNER
	LEFT
	RIGHT
	FULL
	ON
	DOT
	NULL
	ILLEGAL
)

type Token struct {
	Kind  TokenKind
	Value string
}

func (tk TokenKind) String() string {
	switch tk {
	case USE:
		return "USE"
	case IDENT:
		return "IDENT"
	case INSERT:
		return "INSERT"
	case INTO:
		return "INTO"
	case SELECT:
		return "SELECT"
	case UPDATE:
		return "UPDATE"
	case SET:
		return "SET"
	case BEGIN:
		return "BEGIN"
	case COMMIT:
		return "COMMIT"
	case ROLLBACK:
		return "ROLLBACK"
	case FROM:
		return "FROM"
	case WHERE:
		return "WHERE"
	case PLUS:
		return "PLUS"
	case MINUS:
		return "MINUS"
	case DIV:
		return "DIV"
	case INT:
		return "INT"
	case VARCHAR:
		return "VARCHAR"
	case COMMA:
		return "COMMA"
	case ASTERISK:
		return "ASTERISK"
	case LESSTHAN:
		return "LESSTHAN"
	case GREATERTHAN:
		return "GREATERTHAN"
	case LESSTHANEQUAL:
		return "LESSTHANEQUAL"
	case GREATERTHANEQUAL:
		return "GREATERTHANEQUAL"
	case NOTEQUAL:
		return "NOTEQUAL"
	case EQUAL:
		return "EQUAL"
	case OPENCURLY:
		return "OPENCURLY"
	case CLOSECURLY:
		return "CLOSECURLY"
	case OPENROUNDED:
		return "OPENROUNDED"
	case CLOSEDROUNDED:
		return "CLOSEDROUNDED"
	case DROP:
		return "DROP"
	case END:
		return "END"
	case INVALID:
		return "INVALID"
	case TABLE:
		return "TABLE"
	case DATABASE:
		return "DATABASE"
	case DATABASES:
		return "DATABASES"
	case SHOW:
		return "SHOW"
	case JOIN:
		return "JOIN"
	case INNER:
		return "INNER"
	case LEFT:
		return "LEFT"
	case RIGHT:
		return "RIGHT"
	case FULL:
		return "FULL"
	case ON:
		return "ON"
	case DOT:
		return "DOT"
	case NULL:
		return "NULL"
	case ILLEGAL:
		return "ILLEGAL"
	default:
		return "UNKNOWN"
	}
}
