package main

import (
	bplus "DaemonDB/bplustree"
	executor "DaemonDB/query_executor"
	codegen "DaemonDB/query_parser/code-generator"
	lex "DaemonDB/query_parser/lexer"
	"DaemonDB/query_parser/parser"
	"bufio"
	"bytes"

	// "bytes"
	"fmt"
	"os"
	"strings"
)

func main() {

	// Initialize B+ Tree
	pager := bplus.NewInMemoryPager()
	cache := bplus.NewBufferPool(10)
	tree := bplus.NewBPlusTree(pager, cache, bytes.Compare)

	// Initialize Virtual Database Engine ( byte code executor )
	vm := executor.NewVM(tree)

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

	/*
		// printing tokens

		l := lex.New(query)
		tokens := []lex.Token{}
		for {
			tok := l.NextToken()
			tokens = append(tokens, tok)
			if tok.Kind == lex.END {
				break
			}
		}
		for i := range tokens {
			fmt.Printf("kind: %s     value: %s\n", tokens[i].Kind, tokens[i].Value)
		}
	*/

	// Lexer + Parser
	l := lex.New(query)
	p := parser.New(l)

	stmt := p.ParseStatement()

	fmt.Println("\n=== AST ===")
	fmt.Printf("%#v", stmt)

	fmt.Println("\n\n=== Bytecode ===")

	instructions := codegen.EmitBytecode(stmt)

	fmt.Println("\n=== Execution ===")
	if err := vm.Execute(instructions); err != nil {
		fmt.Printf("Error: %v\n", err)
	}

	// testing
	fmt.Println("\n=== TESTING ===")
	searchIDs := []string{"34", "asdf", "S999"}
	for _, id := range searchIDs {
		result, _ := tree.Search([]byte(id))
		if result != nil {
			fmt.Printf("Found %s --> %s\n", id, string(result))
		} else {
			fmt.Printf("Student %s not found\n", id)
		}
	}
}
