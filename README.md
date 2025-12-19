<div align="center">
  <img src="./Sample_Image/Necessary_Image.png" alt="DaemonDB Logo" width="300" />
</div>

<h1 align="center">DaemonDB: Lightweight Relational Database Engine</h1>

DaemonDB is a lightweight relational database engine built from scratch in Go. It implements core database concepts including B+ tree indexing, heap file storage, SQL parsing, and query execution, designed for educational purposes and learning database internals.

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

- **Query Parser**: Lexical analysis and syntax parsing of SQL statements
- **Code Generator**: Converts AST to bytecode instructions
- **Query Executor (VM)**: Executes bytecode, orchestrates B+ tree and heap file operations
- **B+ Tree**: Index structure mapping primary keys to row locations
- **Heap File Manager**: Manages row storage in page-based heap files
- **Pager**: Abstract interface for page-level I/O (currently in-memory)

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
- ✅ Leaf node insertion with splitting
- ✅ Internal node splitting with parent propagation
- ✅ Recursive parent updates (handles multi-level splits)
- ✅ Delete operations with borrow/merge logic
- ✅ Binary search optimization for key lookups
- ✅ Range scan iterator (SeekGE, Next)
- ✅ Thread-safe operations with reader-writer locks

**File Structure:**
- `struct.go`: Node and tree data structures
- `insertion.go`: Insert operations
- `split_leaf.go`: Leaf node splitting
- `split_internal.go`: Internal node splitting
- `parent_insert.go`: Parent propagation logic
- `deletion.go`: Delete with borrow/merge
- `search.go`: Point lookup
- `iterator.go`: Range scan operations
- `find_leaf.go`: Leaf node navigation
- `binary_search.go`: Binary search utilities

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
- ✅ Page-based storage (4KB pages)
- ✅ Slot directory for O(1) row lookup within pages
- ✅ Automatic page creation when pages fill up
- ✅ Row insertion with space management
- ✅ Row retrieval using RowPointer
- ✅ Thread-safe file operations

**File Structure:**
- `struct.go`: PageHeader, Slot, RowPointer, HeapFile structures
- `heapfile.go`: HeapFile operations (insertRow, GetRow, findSuitablePage)
- `heapfile_manager.go`: HeapFileManager (CreateHeapfile, InsertRow, GetRow)
- `page_io.go`: Low-level page read/write operations
- `page_header.go`: Header serialization/deserialization
- `slots.go`: Slot directory operations (readSlot, writeSlot, addSlot)

**Architecture:**
- 1 Table = 1 HeapFile (one `.heap` file per table)
- 1 HeapFile = Multiple Pages (grows automatically as needed)
- 1 Page = Multiple Rows (~100-200 rows per page, depends on row size)

## SQL Parser

A complete SQL processing pipeline from lexical analysis to AST generation.

### Supported Statements

```sql
-- Table creation
CREATE TABLE students {
    id int,
    name string,
    age int,
    grade string
}

-- Data insertion
INSERT INTO students VALUES ("S001", "Alice", 20, "A")
INSERT INTO students VALUES ("S002", "Bob", 21, "B")

-- Data querying
SELECT * FROM students
SELECT name, grade FROM students

-- Data updates
UPDATE students SET grade = "A+" WHERE id = "S001"

-- Table Join
-- JOIN by default does INNER JOIN

SELECT * FROM table1 JOIN table2 ON id1 = id2 

SELECT * FROM table1 INNER JOIN table2 ON id1 = id2 

SELECT * FROM table1 JOIN table2 ON table1.id1 = table2.id2

SELECT * from table1 JOIN table2 ON id1 = id2 WHERE table1.id = 5

SELECT * from table1 JOIN table2 ON table1.id1 = table2.id2 WHERE table1.id = 5

SELECT * from table1 JOIN table2 ON table1.name = table2.refname WHERE table1.name = "abc"

-- Table management
DROP students
```

### Parser Architecture

- **Lexer**: Hand-written tokenizer for SQL keywords, identifiers, literals
- **Parser**: Recursive descent parser for syntax analysis
- **AST**: Abstract syntax tree generation for each statement type
- **Code Generator**: Converts AST to bytecode instructions

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
- ✅ CREATE TABLE execution
- ✅ INSERT execution (writes to heap file + indexes in B+ tree)
- ✅ Bytecode instruction set (OP_PUSH_VAL, OP_INSERT, OP_SELECT, etc.)
- ✅ Row serialization/deserialization
- ✅ Primary key extraction
- ✅ RowPointer serialization

**In Progress:**
- 🚧 SELECT execution (parser complete, executor TODO)
- 🚧 UPDATE execution (parser complete, executor TODO)
- 🚧 DELETE execution (parser complete, executor TODO)

**File Structure:**
- `executor.go`: VM implementation and statement execution
- `helpers.go`: Serialization utilities, table schema management

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
│   ├── parser/                  # Syntax analysis
│   └── code-generator/          # Bytecode generation
├── query_executor/               # Query execution
│   ├── executor.go              # VM and execution
│   └── helpers.go               # Utilities
├── main.go                       # Entry point
└── README.md                     # This file
```

## Quick Start

### 1. Run the Database

```bash
go run main.go
```

Then enter SQL queries:
```sql
CREATE TABLE students {
    id int primary key,
    name string,
    age int,
    grade string
}

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
| **SQL Parser** | ✅ Complete | Lexer and parser for DDL/DML |
| **Code Generator** | ✅ Complete | AST to bytecode conversion |
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

## Performance Characteristics

- **B+ Tree Search**: O(log n) for point lookups
- **B+ Tree Range Scan**: O(log n + k) where k is result size
- **Heap File Insert**: O(1) per row (amortized)
- **Heap File Read**: O(1) using slot directory
- **Page Size**: 4KB (disk-aligned)
- **Node Capacity**: 32 keys per node
- **Concurrency**: Reader-writer locks for optimal read performance

## Technical Specifications

- **Language**: Go 1.19+
- **Storage**: Heap files on disk (4KB pages)
- **Indexing**: B+ tree (currently in-memory, disk persistence planned)
- **Query Language**: SQL with DDL/DML support
- **Concurrency**: Thread-safe with mutex locks
- **Architecture**: Index-organized (B+ tree points to heap file rows)

## Testing

The project includes comprehensive tests:

```bash
# Test heap file system
cd heapfile_manager
go test -v

# Test specific functionality
go test -v -run TestHeapFileOperations
go test -v -run TestMultiplePages
go test -v -run TestSlotDirectory
```

## Future Work

- [ ] Implement UPDATE/DELETE operations in heap files
- [ ] Add secondary indexes and non-PK WHERE filtering
- [ ] Add transaction support
- [ ] Implement WAL (Write-Ahead Logging) for durability

## License

This project is licensed under the MIT License.

## Contributing

This is an educational project built for learning database internals. Contributions and suggestions are welcome!
