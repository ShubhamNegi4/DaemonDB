// Seed program: creates database "demp" with 2-3 tables and sample rows.
// Run: go run ./cmd/seed
// Then inspect: databases/demp/tables/ (heap + schema) and databases/demp/indexes/ (B+ tree .idx files).
package main

import (
	bplus "DaemonDB/bplustree"
	heapfile "DaemonDB/heapfile_manager"
	executor "DaemonDB/query_executor"
	codegen "DaemonDB/query_parser/code-generator"
	lex "DaemonDB/query_parser/lexer"
	"DaemonDB/query_parser/parser"
	"DaemonDB/wal_manager"
	"bytes"
	"fmt"
	"log"
	"os"
)

const (
	baseDir = "databases/demp"
	walDir  = "databases/demp/logs"
)

func main() {
	// Create DB dir and tables/logs subdirs so USE demp works. Do not run CREATE DATABASE
	// (that would fail because OpenWAL already creates baseDir when using baseDir/logs).
	if err := os.MkdirAll(baseDir+"/tables", 0755); err != nil {
		log.Fatalf("mkdir: %v", err)
	}
	if err := os.MkdirAll(walDir, 0755); err != nil {
		log.Fatalf("mkdir wal: %v", err)
	}

	walManager, err := wal_manager.OpenWAL(walDir)
	if err != nil {
		log.Fatalf("open WAL: %v", err)
	}
	defer walManager.Close()

	pager := bplus.NewInMemoryPager()
	cache := bplus.NewBufferPool(10)
	tree := bplus.NewBPlusTree(pager, cache, bytes.Compare)

	heapFileManager, err := heapfile.NewHeapFileManager(baseDir)
	if err != nil {
		log.Fatalf("heap file manager: %v", err)
	}
	defer heapFileManager.CloseAll()

	vm := executor.NewVM(tree, heapFileManager, walManager)
	defer vm.CloseIndexCache()

	if err := vm.RecoverAndReplayFromWAL(); err != nil {
		log.Fatalf("recover WAL: %v", err)
	}

	run := func(sql string) {
		l := lex.New(sql)
		p := parser.New(l)
		stmt := p.ParseStatement()
		instructions := codegen.EmitBytecode(stmt)
		if err := vm.Execute(instructions); err != nil {
			log.Fatalf("execute %q: %v", sql, err)
		}
	}

	fmt.Println("Using database demp and creating tables...")

	run("USE demp")

	// Table 1: students (id PK string, name, age)
	run(`CREATE TABLE students ( id string primary key, name string, age int )`)
	run(`INSERT INTO students VALUES ("S001", "Alice", 20)`)
	run(`INSERT INTO students VALUES ("S002", "Bob", 21)`)
	run(`INSERT INTO students VALUES ("S003", "Carol", 19)`)

	// Table 2: courses (code PK, title)
	run(`CREATE TABLE courses ( code string primary key, title string )`)
	run(`INSERT INTO courses VALUES ("CS101", "Intro to CS")`)
	run(`INSERT INTO courses VALUES ("CS102", "Data Structures")`)

	// Table 3: grades (id PK, course_code, grade)
	run(`CREATE TABLE grades ( id int primary key, course_code string, grade string )`)
	run(`INSERT INTO grades VALUES (1, "CS101", "A")`)
	run(`INSERT INTO grades VALUES (2, "CS102", "B")`)
	run(`INSERT INTO grades VALUES (3, "CS101", "A")`)

	fmt.Println("\n--- SELECT * FROM students ---")
	run("SELECT * FROM students")

	fmt.Println("\n--- SELECT * FROM courses ---")
	run("SELECT * FROM courses")

	fmt.Println("\n--- SELECT * FROM grades ---")
	run("SELECT * FROM grades")

	fmt.Println("\nDone. Inspect:")
	fmt.Println("  - Heap files (table data):", baseDir+"/tables/*.heap")
	fmt.Println("  - Schemas:                ", baseDir+"/tables/*_schema.json")
	fmt.Println("  - Primary key indexes:    ", baseDir+"/indexes/*.idx")
	fmt.Println("  - Table-file mapping:     ", baseDir+"/table_file_mapping.json")
}
