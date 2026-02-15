<div align="center">
  <img src="./Sample_Image/Necessary_Image.png" alt="DaemonDB Logo" width="300" />
</div>

<h1 align="center">DaemonDB: Lightweight Relational Database Engine</h1>

DaemonDB is a lightweight relational database engine built from scratch in Go. It implements core database concepts including B+ tree indexing, heap file storage, SQL parsing, a bytecode executor, WAL-based durability, and basic transaction support. The project is designed for education: every subsystem is small enough to read, yet complete enough to show the real mechanics.

## Overview

DaemonDB provides a clean, well-documented implementation of fundamental database components. The project implements a complete database stack from storage to query execution:

**Key Features:**
- Complete B+ tree with insert, search, delete, and range scan operations
- Heap file storage system with slot directory for O(1) row lookup
- SQL parser supporting DDL and DML statements
- Query executor with bytecode-based virtual machine
- Page-based storage architecture (4KB pages)
- Thread-safe operations with proper concurrency control

## Architecture

The database follows a layered architecture separating storage, indexing, and query processing:

```
┌─────────────────────────────────────────┐
│         SQL Query Layer                  │
│  (Parser → Code Generator → Executor)   │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│         Index Layer (B+ Tree)           │
│  PrimaryKey → RowPointer(File,Page,Slot)│
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│      Storage Layer (Heap Files)          │
│  Pages (4KB) with Slot Directory         │
└─────────────────────────────────────────┘
```

### Component Overview

- **Query Parser**: Hand-written lexer and recursive-descent parser that builds AST for DDL/DML (+ joins and transactions)
- **Code Generator**: Emits compact bytecode instructions (stack-based, VDBE-style)
- **Query Executor (VM)**: Executes bytecode, orchestrates B+ tree, heap files, and WAL
- **B+ Tree**: Primary index mapping keys → row pointers with disk pager + buffer pool
- **Heap File Manager**: Page + slot–based heap files for row storage
- **Pager Layer**: Abstracts page I/O (in-memory & on-disk implementations)
- **WAL + Txn Manager**: Write-ahead logging with BEGIN/COMMIT/ROLLBACK and crash replay

## B+ Tree Implementation

The B+ tree serves as the primary index, providing O(log n) performance for key lookups and range scans.

### Tree Structure

- **Internal Nodes**: Store separator keys and child pointers (navigation only)
- **Leaf Nodes**: Store key-value pairs with linked list structure for range scans
- **Node Capacity**: 32 keys per node (configurable via `MaxKeys`)
- **Automatic Splitting**: Nodes split when capacity exceeded, with parent propagation
- **Balanced Tree**: All leaves at same depth, maintains B+ tree invariants

### Operations

```go
// Initialize tree
pager := bplus.NewInMemoryPager()
cache := bplus.NewBufferPool(10)
tree := bplus.NewBPlusTree(pager, cache, bytes.Compare)

// Insert data
tree.Insertion([]byte("S001"), []byte("RowPointerBytes"))

// Search data
result, _ := tree.Search([]byte("S001"))
fmt.Printf("Found: %s\n", string(result))

// Delete data
tree.Delete([]byte("S001"))

// Range scan
iter := tree.SeekGE([]byte("S001"))
for iter.Valid() {
    key := iter.Key()
    value := iter.Value()
    iter.Next()
}
```

### Implementation Details

**Completed Features:**
- ✅ Leaf/internal splits with parent propagation
- ✅ Borrow/merge delete logic and root collapse
- ✅ Binary search inside nodes; linked leaves for range scans
- ✅ On-disk pager with 4KB pages + persisted root (page 0 metadata)
- ✅ LRU buffer pool with pin/unpin + dirty tracking
- ✅ Node serialization (`encodeNode`/`decodeNode`)
- ✅ Iterator for SeekGE/Next range scans
- ✅ Thread-safe with RW locks

**File Structure:**
- `struct.go`: Node and tree data structures
- `new_bplus_tree.go`: Constructs tree, loads persisted root
- `insertion.go`: Insert operations
- `split_leaf.go` / `split_internal.go` / `parent_insert.go`: Split + propagation
- `deletion.go`: Delete with borrow/merge
- `search.go`: Point lookup
- `iterator.go`: Range scan operations
- `find_leaf.go`: Leaf navigation
- `binary_search.go`: Binary search utilities
- `inmemory_pager.go` / `disk_pager.go`: Pager implementations
- `buffer_pool.go`: LRU cache with pin/unpin & flush
- `node_codec.go`: Node serialization

## Heap File System

The heap file system stores actual row data in page-based files with a slot directory for efficient row access.

### Page Structure

Each 4KB page contains:
- **Header (32 bytes)**: FileID, PageNo, FreePtr, NumRows, SlotCount, etc.
- **Data Area**: Rows stored sequentially from offset 32
- **Slot Directory**: Grows backward from end of page (4 bytes per slot: offset + length)

```
┌─────────────────────────────────────────┐
│ Page (4096 bytes)                       │
├─────────────────────────────────────────┤
│ Header (32B): metadata                  │
├─────────────────────────────────────────┤
│ Row 1 | Row 2 | Row 3 | ...            │
│ (grows forward)                          │
├─────────────────────────────────────────┤
│ ...                                     │
│ Slot 3 | Slot 2 | Slot 1 | Slot 0      │
│ (grows backward)                         │
└─────────────────────────────────────────┘
```

### Operations

```go
// Create heap file manager
hfm, _ := heapfile.NewHeapFileManager("./data")

// Create heap file for a table
hfm.CreateHeapfile("students", fileID)

// Insert row
rowPtr, _ := hfm.InsertRow(fileID, rowData)
// Returns: RowPointer{FileID, PageNumber, SlotIndex}

// Read row
rowData, _ := hfm.GetRow(rowPtr)
```

### Implementation Details

**Completed Features:**
- ✅ Page-based storage (4KB pages, 32B headers)
- ✅ Slot directory (offset+length) for O(1) row access
- ✅ Automatic page allocation + header bookkeeping
- ✅ Row insertion, GetRow, tombstone DeleteRow (slot zeroed)
- ✅ Full-table scan via `GetAllRowPointers`
- ✅ Thread-safe file operations

**File Structure:**
- `struct.go`: PageHeader, Slot, RowPointer, HeapFile structures
- `helpers.go`: InsertRow, GetRow, DeleteRow, LoadHeapFile, mapping helpers
- `heapfile_pager.go`: Pager + page allocation, sync/close
- `slots.go`: Slot directory helpers
- `page_header_io.go`: Header serialization/deserialization
- `heapfile_manager.go`: Manager for per-table heap files

**Architecture:**
- 1 Table = 1 HeapFile (one `.heap` file per table)
- 1 HeapFile = Multiple Pages (grows automatically as needed)
- 1 Page = Multiple Rows (~100-200 rows per page, depends on row size)

## SQL Parser

A complete SQL processing pipeline from lexical analysis to AST generation.

### Supported Statements

**Note:** Invalid SQL returns a parse or codegen error (no panics). See `cmd/PARSER_PANIC_TRIGGERS.md` for correct syntax and common mistakes.

```sql
-- Table creation (use parentheses, not braces)
CREATE TABLE students ( id int primary key, name string, age int, grade string )

-- Data insertion
INSERT INTO students VALUES ("S001", "Alice", 20, "A")
INSERT INTO students VALUES ("S002", "Bob", 21, "B")

-- Data querying
SELECT * FROM students
SELECT name, grade FROM students

-- Data updates
UPDATE students SET id=id+3 WHERE id=5 -- similary for -,*,/,<,>,<=,>=,!=
UPDATE students SET id=5 -- changes all rows
UPDATE students SET name="newName" WHERE name="currName"

-- UPDATE students SET grade = "A+" WHERE id = "S001"

-- Table Join
-- JOIN by default does INNER JOIN

SELECT * FROM table1 JOIN table2 ON id1 = id2 

SELECT * FROM table1 INNER JOIN table2 ON id1 = id2 

SELECT * FROM table1 JOIN table2 ON table1.id1 = table2.id2

SELECT * from table1 JOIN table2 ON id1 = id2 WHERE table1.id = 5

SELECT * from table1 JOIN table2 ON table1.id1 = table2.id2 WHERE table1.id = 5

SELECT * from table1 JOIN table2 ON table1.name = table2.refname WHERE table1.name = "abc"

SELECT * from table1 JOIN table2 ON table1.name = table2.refname WHERE table1.id = NULL

-- similary
select * from table1 LEFT JOIN table2 on id1 = id2

select * from table1 RIGHT JOIN table2 on id1 = id2

select * from table1 FULL JOIN table2 on id1 = id2


-- Table management (parser accepts; executor not yet implemented)
-- DROP TABLE students
```

### Parser Architecture

- **Lexer**: Hand-written tokenizer for SQL keywords, identifiers, literals
- **Parser**: Recursive descent parser (supports joins, WHERE on PK, transactions)
- **AST**: Abstract syntax tree per statement
- **Code Generator**: Emits stack-based bytecode for the VM

**File Structure:**
- `lexer/lexer.go`: Tokenization implementation
- `lexer/token.go`: Token definitions
- `parser/parser.go`: Recursive descent parser
- `parser/ast.go`: AST node definitions
- `code-generator/code_generator.go`: Bytecode emission

## Query Executor

The query executor uses a bytecode-based virtual machine (VDBE-style) to execute SQL statements.

### Execution Flow

```
SQL Query
  ↓
Parser → AST
  ↓
Code Generator → Bytecode Instructions
  ↓
VM.Execute()
  ├─→ HeapFileManager.InsertRow() → Write row data
  ├─→ B+ Tree.Insertion() → Index the row
  └─→ Return result
```

### Current Implementation

**Completed:**
- ✅ CREATE DATABASE / SHOW DATABASES / USE
- ✅ CREATE TABLE (with foreign keys validation)
- ✅ INSERT (heap write + primary index write)
- ✅ SELECT:
  - Full table scan
  - PK index lookup when WHERE targets primary key
  - Sort-merge joins (INNER/LEFT/RIGHT/FULL) with optional WHERE filter
- ✅ Bytecode instruction set (stack-based VM)
- ✅ Row serialization/deserialization
- ✅ Primary key extraction (explicit PK or implicit rowid)
- ✅ RowPointer serialization
- ✅ Transaction opcodes (BEGIN/COMMIT/ROLLBACK) with logical undo of inserts
- ✅ WAL append + fsync before data/index writes; crash recovery replays committed ops

**Pending/Partial:**
- 🚧 UPDATE/DELETE execution (parser exists, executor not implemented)
- 🚧 Secondary indexes and non-PK predicates

**File Structure:**
- `vm.go`: VM loop and opcode dispatch
- `exec_create_db.go`, `exec_create_table.go`, `exec_insert.go`, `exec_select.go`: Statement execution
- `serialization.go`, `table_mapping.go`, `joins.go`, `index.go`, `print.go`, `type_conv.go`: Helpers
- `structs.go`: Opcodes, VM struct, payloads
- `txn_manager.go`: Transaction bookkeeping
- `wal_replay.go`: Crash recovery and replay
- Per-table B+ tree index cache (closed on DB switch / VM shutdown)

## Project Structure

```
DaemonDB/
├── bplustree/                    # B+ Tree index implementation
│   ├── struct.go                # Data structures
│   ├── insertion.go             # Insert operations
│   ├── deletion.go              # Delete operations
│   ├── search.go                # Point lookup
│   ├── iterator.go              # Range scan
│   ├── split_leaf.go            # Leaf splitting
│   ├── split_internal.go        # Internal node splitting
│   ├── parent_insert.go         # Parent propagation
│   ├── find_leaf.go             # Leaf navigation
│   ├── binary_search.go         # Binary search utilities
│   ├── pager.go                 # Pager interface (in-memory)
│   └── ...
├── heapfile_manager/             # Heap file storage system
│   ├── struct.go                # PageHeader, Slot, RowPointer
│   ├── heapfile.go              # HeapFile operations
│   ├── heapfile_manager.go      # HeapFileManager
│   ├── page_io.go               # Page read/write
│   ├── page_header.go           # Header serialization
│   ├── slots.go                 # Slot directory operations
│   └── heapfile_test.go         # Comprehensive tests
├── query_parser/                 # SQL parsing
│   ├── lexer/                   # Lexical analysis
│   ├── parser/                  # Syntax analysis (parser.go, parse_ddl.go, parse_dml.go, parse_select.go)
│   └── code-generator/          # Bytecode generation
├── query_executor/               # Query execution (vm.go, exec_*.go, helpers)
├── cmd/                          # CLI tools (seed, inspect_idx, dump_sample)
├── main.go                       # REPL entry point
└── README.md                     # This file
```

## Quick Start

### 1. Run the Database

```bash
go run main.go
```

Then enter SQL queries. Type `help` or `?` for supported commands.

```sql
CREATE DATABASE demoDB
USE demoDB
CREATE TABLE students ( id string primary key, name string, age int, grade string )

INSERT INTO students VALUES ("S001", "Alice", 20, "A")
INSERT INTO students VALUES ("S002", "Bob", 21, "B")

-- Point lookup uses the B+ tree on the declared primary key
SELECT * FROM students WHERE id = "S002"
```

### 2. Test Heap File System

```bash
cd heapfile_manager
go test -v -run TestAll
```

This runs comprehensive tests:
- Basic insert/read operations
- Multiple pages
- Slot directory functionality
- Page header management

### 3. Test B+ Tree

```bash
cd bplustree
go run bplus.go
```

## Current Status

| Component | Status | Description |
|-----------|--------|-------------|
| **B+ Tree Core** | ✅ Complete | Full CRUD with parent propagation, internal splits |
| **B+ Tree Iterator** | ✅ Complete | Range scan operations (SeekGE, Next) |
| **Heap File Storage** | ✅ Complete | Page-based storage with slot directory |
| **Heap File Operations** | ✅ Complete | Insert, GetRow (Delete/Update TODO) |
| **SQL Parser** | ✅ Complete | Lexer and parser for DDL/DML; errors instead of panics |
| **Code Generator** | ✅ Complete | AST to bytecode conversion; returns error on unknown/marshal failure |
| **Query Executor** | 🚧 Partial | INSERT/CREATE TABLE working, SELECT uses PK index; UPDATE/DELETE TODO |
| **Concurrency** | ✅ Complete | Thread-safe operations |
| **File Persistence** | 🚧 Partial | Heap files on disk, B+ tree index pages on disk (root persisted) |
| **Buffer Pool** | ✅ Complete | LRU cache with pin/unpin, dirty tracking |
| **Node Serialization** | ✅ Complete | Encode/decode nodes to pages |

## Data Flow Example

### INSERT Operation

```
1. User: INSERT INTO students VALUES ("S001", "Alice", 20, "A")
   ↓
2. Parser: Parse SQL → AST
   ↓
3. Code Generator: AST → Bytecode
   ↓
4. VM.Execute():
   a. SerializeRow() → Convert values to bytes
   b. HeapFileManager.InsertRow() → Write to heap file
      → Returns: RowPointer(FileID=1, PageNumber=0, SlotIndex=3)
   c. SerializeRowPointer() → Convert to 10 bytes (FileID, PageNumber, SlotIndex)
   d. ExtractPrimaryKey() → declared PK if present, otherwise implicit rowid
   e. B+ Tree.Insertion(PK, RowPointerBytes) → Stores index: PK → RowPointer
```

### SELECT Operation (Conceptual)

```
1. User: SELECT * FROM students WHERE id = "S001"
   ↓
2. Parser: Parse SQL → AST
   ↓
3. Code Generator: AST → Bytecode
   ↓
4. VM.Execute():
   a. B+ Tree.Search("S001") (only when WHERE is on the primary key)
      → Returns: RowPointer bytes
   b. DeserializeRowPointer() → RowPointer(1, 0, 3)
   c. HeapFileManager.GetRow(RowPointer)
      → Reads page 0, slot 3 → Returns row data
   d. DeserializeRow() → Convert bytes to values
   e. Return result to user (SELECT without WHERE still does a full scan)
```

## Transactions & WAL

- **WAL format**: Fixed-size records with LSN, length, CRC + JSON-encoded operation payloads
- **Segmented log**: 16MB segments (`wal_XXXXXXXXXXXX.log`)
- **Durability path**: Append log → fsync → apply to heap/index
- **Recovery**: Two-pass replay (collect committed txn IDs, then reapply committed CREATE TABLE / INSERT)
- **Rollback**: Logical undo of inserted rows (heap tombstone + index delete)

## Performance Characteristics

- **B+ Tree Search**: O(log n) for point lookups
- **B+ Tree Range Scan**: O(log n + k) where k is result size
- **Heap File Insert**: O(1) per row (amortized)
- **Heap File Read**: O(1) using slot directory
- **Page Size**: 4KB (disk-aligned)
- **Node Capacity**: 32 keys per node
- **Concurrency**: Reader-writer locks on tree nodes
- **Buffer Pool**: LRU with pin/unpin to protect pages during operations

## Technical Specifications

- **Language**: Go 1.19+
- **Storage**: Heap files on disk (4KB pages)
- **Indexing**: B+ tree with on-disk pager + buffer pool
- **Query Language**: SQL with DDL/DML, joins, PK-based WHERE
- **Transactions**: BEGIN/COMMIT/ROLLBACK, WAL-backed durability
- **Concurrency**: Thread-safe with mutex locks
- **Architecture**: Index-organized (B+ tree points to heap file rows)

## Testing

```bash
# All packages
go test ./...

# Heap file system tests
cd heapfile_manager && go test -v

# Parser and codegen (error paths, no panics)
cd query_parser/parser && go test -v
cd query_parser/code-generator && go test -v

# B+ tree demo (interactive)
cd bplustree && go run bplus.go
```

## Future Work

- [ ] Executor support for UPDATE/DELETE
- [ ] Secondary indexes and non-PK predicates
- [ ] Garbage collection / compaction for tombstoned rows
- [ ] Background checkpointing of WAL segments

## License

This project is licensed under the MIT License.

## Contributing

This is an educational project built for learning database internals. Contributions and suggestions are welcome!
