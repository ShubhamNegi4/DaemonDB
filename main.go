package main

import (
	bplus "DaemonDB/bplustree"
	heapfile "DaemonDB/heapfile_manager"
	executor "DaemonDB/query_executor"
	codegen "DaemonDB/query_parser/code-generator"
	lex "DaemonDB/query_parser/lexer"
	"DaemonDB/query_parser/parser"
	"DaemonDB/wal_manager"
	"bufio"
	"bytes"
	"log"

	// "bytes"
	"fmt"
	"os"
	"strings"
)

func main() {

	walManager, err := wal_manager.OpenWAL("databases/demoDB/logs") // fixed for now, depends on database too
	if err != nil {
		log.Fatal(err)
	}

	// Initialize B+ Tree with in-memory pager; table-specific on-disk indexes are opened per-table via GetOrCreateIndex
	pager := bplus.NewInMemoryPager()
	cache := bplus.NewBufferPool(10)
	tree := bplus.NewBPlusTree(pager, cache, bytes.Compare)

	// a must `USE DATABASE` command will initialize this
	heapFileManager, err := heapfile.NewHeapFileManager("databases/demoDB")
	if err != nil {
		walManager.Close()
		log.Fatal(err)
	}

	vm := executor.NewVM(tree, heapFileManager, walManager)

	if err := vm.RecoverAndReplayFromWAL(); err != nil {
		walManager.Close()
		log.Fatal(err)
	}

	scanner := bufio.NewScanner(os.Stdin)
	// REPL
	for {
		fmt.Print("daemon> ")

		if !scanner.Scan() { // Ctrl+D pressed
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if strings.EqualFold(line, "exit") {
			break
		}
		if line == "" {
			continue
		}

		// user typed a single SQL query
		query := line

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

		stmt, err := p.ParseStatement()
		if err != nil {
			fmt.Printf("Parse error: %v\n", err)
			continue
		}

		fmt.Println("\n=== AST ===")
		fmt.Printf("%#v", stmt)

		fmt.Println("\n\n=== Bytecode ===")

		instructions, err := codegen.EmitBytecode(stmt)
		if err != nil {
			fmt.Printf("Codegen error: %v\n", err)
			continue
		}
		for i, instr := range instructions {
			fmt.Printf("%d: OP=%v, VALUE=%v\n", i, instr.Op, instr.Value)
		}

		fmt.Println("\n=== Execution ===")
		if err := vm.Execute(instructions); err != nil {
			fmt.Printf("Error: %v\n", err)
		}
	}
}
