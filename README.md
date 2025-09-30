<div align="center">
  <img src="./Sample_Image/Necessary_Image.jpeg" alt="DaemonDB Logo" width="150" />
</div>

<h1 align="center">DaemonDB: Lightweight Relational Database Engine</h1>

DaemonDB is a lightweight relational database engine built from scratch in Go. It implements core database concepts including B+ tree indexing, SQL parsing, and in-memory storage management, designed for both educational purposes and practical use cases.

## Overview

DaemonDB provides a clean, well-documented implementation of fundamental database components. The project consists of two main modules: a B+ tree implementation for efficient indexing and a SQL parser for query processing.

**Key Features:**
- Complete B+ tree with insert, search, and delete operations
- SQL parser supporting standard DDL and DML statements
- In-memory storage with page-based architecture
- Thread-safe operations with proper concurrency control
- Educational codebase with clear documentation

## Architecture

The database is built with a modular design separating storage, indexing, and query processing:

- **Storage Layer**: Page-based in-memory storage with 4KB pages
- **Index Layer**: B+ tree implementation for efficient key-value operations
- **Query Layer**: SQL lexer and parser for standard database operations

## B+ Tree Implementation

The B+ tree forms the core indexing mechanism, providing O(log n) performance for all operations.

**Tree Structure:**
- Internal nodes contain keys and child pointers for navigation
- Leaf nodes store actual data with linked list structure for range scans
- Automatic node splitting when capacity is exceeded (32 keys per node)
- Binary search optimization for key lookups

**Operations:**
```go
// Initialize tree
pager := NewInMemoryPager()
cache := NewBufferPool(10)
tree := NewBPlusTree(pager, cache, bytes.Compare)

// Insert data
tree.Insertion([]byte("student_001"), []byte("Alice|A"))

// Search data
result, _ := tree.Search([]byte("student_001"))
fmt.Printf("Found: %s\n", string(result))

// Delete data
tree.Delete([]byte("student_001"))
```

**Configuration:**
- Page size: 4KB (aligned with typical disk block sizes)
- Max keys per node: 32
- Buffer pool: 10 pages (40KB total memory)
- Thread-safe with reader-writer locks

## SQL Parser

A complete SQL processing pipeline from lexical analysis to AST generation.

**Supported Statements:**
```sql
-- Table creation with schema definition
CREATE TABLE students {
    name string,
    age int,
    grade string
}

-- Data insertion
INSERT INTO students VALUES ("Alice Johnson", 20, "A")
INSERT INTO students VALUES ("Bob Smith", 19, "B")

-- Data querying
SELECT * FROM students
SELECT name, grade FROM students

-- Data updates
UPDATE students SET grade = "A+" WHERE name = "Alice Johnson"

-- Table management
DROP students
```

**Parser Architecture:**
- Hand-written lexer for efficient tokenization
- Recursive descent parser for syntax analysis
- AST generation for each statement type
- Bytecode emission for execution planning

## Quick Start

**1. B+ Tree Implementation:**
```bash
cd B+Tree-Implementation
go run *.go
```

This runs the student database example, demonstrating:
- Inserting 5 student records
- Searching for specific students
- Displaying database statistics

**2. SQL Parser:**
```bash
cd Query-Parser
go mod tidy
go run .

# Test with sample queries
echo 'select * from mytable' | go run .
echo 'create table students { name string, age int }' | go run .
```

## Project Structure

```
DaemonDB/
├── B+Tree-Implementation/          # Core B+ Tree engine
│   ├── treeStruct.go              # Data structures and interfaces
│   ├── treeInsertion.go           # Insert operations with splitting
│   ├── treeDeletion.go            # Delete operations with merging
│   ├── treeSearch.go              # Search operations with binary search
│   ├── treeSplitLeaf.go           # Leaf node splitting logic
│   ├── treePager.go               # In-memory storage implementation
│   ├── binarySearch.go            # Binary search utilities
│   └── main.go                    # Example usage and tests
├── Query-Parser/                  # SQL processing
│   ├── lexer/                     # Lexical analysis
│   │   ├── lexer.go              # Tokenizer implementation
│   │   └── token.go              # Token definitions
│   ├── parser/                    # Syntax analysis
│   │   ├── parser.go             # Recursive descent parser
│   │   ├── ast.go                # Abstract syntax tree
│   │   └── commands.md           # Supported SQL commands
│   └── main.go                    # Parser CLI interface
└── README.md                      # This file
```

## Implementation Details

**B+ Tree Features:**
- Complete CRUD operations (Create, Read, Update, Delete)
- Automatic node splitting when capacity exceeded
- Binary search for O(log n) key lookups
- Thread-safe operations with mutex synchronization
- In-memory storage with page-based allocation

**SQL Parser Features:**
- Complete lexer supporting all SQL keywords and operators
- Recursive descent parser with proper error handling
- AST generation for each statement type
- Bytecode emission for execution planning
- CLI interface for interactive testing

**Storage Layer:**
- In-memory pager with 4KB page allocation
- Configurable buffer pool size
- Thread-safe page management
- Page allocation and deallocation tracking

## Current Status

| Component | Status | Description |
|-----------|--------|-------------|
| **B+ Tree Core** | ✅ Complete | Full CRUD operations with splitting |
| **SQL Parser** | ✅ Complete | Lexer and parser for standard SQL |
| **Storage Layer** | ✅ Complete | In-memory page-based storage |
| **Concurrency** | ✅ Complete | Thread-safe operations |
| **Parent Propagation** | 🚧 Partial | Leaf splitting works, parent updates TODO |
| **File Persistence** | 📋 Planned | Disk-based storage implementation |
| **Query Execution** | 📋 Planned | Execute parsed SQL against B+ tree |

## Performance Characteristics

- **Search Complexity**: O(log n) for all operations
- **Memory Usage**: 40KB buffer pool (10 pages × 4KB)
- **Node Capacity**: 32 keys per node
- **Page Size**: 4KB (disk-aligned)
- **Concurrency**: Reader-writer locks for optimal read performance

## Example: Student Database

The included example demonstrates a complete student database:

```go
// Student records
students := []struct {
    id    string
    name  string
    grade string
}{
    {"S001", "Alice Johnson", "A"},
    {"S002", "Bob Smith", "B"},
    {"S003", "Charlie Brown", "A"},
    {"S004", "Diana Prince", "C"},
    {"S005", "Eve Wilson", "B"},
}

// Insert all students
for _, student := range students {
    record := student.name + "|" + student.grade
    tree.Insertion([]byte(student.id), []byte(record))
}

// Search for specific students
result, _ := tree.Search([]byte("S001"))
if result != nil {
    fmt.Printf("Found: %s\n", string(result))
}
```

## Technical Specifications

- **Language**: Go 1.19+
- **Storage**: In-memory with page-based architecture
- **Indexing**: B+ tree with configurable branching factor
- **Query Language**: SQL with standard DDL/DML support
- **Concurrency**: Thread-safe with mutex locks
- **Memory Management**: LRU-based buffer pool

## License

This project is licensed under the MIT License.