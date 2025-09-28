package main

import (
	"bufio"
	"fmt"
	"os"
	lex "query-parser/lexer"
	"strings"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	inputLines := []string{}

	for {
		if !scanner.Scan() { // Ctrl+D pressed
			break
		}
		line := scanner.Text()
		inputLines = append(inputLines, line)
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "Error reading input:", err)
		return
	}

	query := strings.Join(inputLines, " ")

	lexer := lex.New(query)

	tokens := []lex.Token{}
	for {
		tok := lexer.NextToken()
		tokens = append(tokens, tok)
		if tok.Kind == lex.END {
			break
		}
	}

	for i := range tokens {
		fmt.Printf("kind: %s     value: %s\n", tokens[i].Kind, tokens[i].Value)
	}

}
