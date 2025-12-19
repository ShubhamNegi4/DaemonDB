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
	INT
	STRING
	COMMA
	ASTERISK
	EQUAL
	OPENCURLY
	CLOSECURLY
	OPENROUNDED
	CLOSEDROUNDED
	DROP
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
	ON
	DOT
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
	case FROM:
		return "FROM"
	case WHERE:
		return "WHERE"
	case INT:
		return "INT"
	case STRING:
		return "STRING"
	case COMMA:
		return "COMMA"
	case ASTERISK:
		return "ASTERISK"
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
	case ON:
		return "ON"
	case DOT:
		return "DOT"
	default:
		return "UNKNOWN"
	}
}
