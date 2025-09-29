# DaemonDB

A lightweight relational database engine built from scratch in Go.

## What's Built

- **B+ Tree**: In-memory index structure with search, insert, delete operations
- **SQL Parser**: Lexer and parser for CREATE, INSERT, SELECT, UPDATE, DROP statements  
- **Storage**: Page-based architecture with buffer pool (10 pages × 4KB = 40KB)

## Quick Start

```bash
cd B+Tree-Implementation
go run *.go
```

## Example Usage

```go
// Create tree
pager := NewInMemoryPager()
cache := NewBufferPool(10)
tree := NewBPlusTree(pager, cache, bytes.Compare)

// Insert data
tree.Insertion([]byte("key1"), []byte("value1"))

// Search data
result, _ := tree.Search([]byte("key1"))
fmt.Printf("Found: %s\n", string(result))
```

## Project Structure

```
DaemonDB/
├── B+Tree-Implementation/     # Core B+ Tree
├── Query-Parser/             # SQL parsing
└── README.md
```

## Current Status

✅ **Working**: B+ Tree operations, in-memory storage, SQL parsing  
🚧 **In Progress**: Leaf splitting, parent propagation  
📋 **Planned**: Disk persistence, transactions

## Memory Usage

- **Data Storage**: In-memory only (lost when program exits)
- **Buffer Pool**: 40KB (10 pages)
- **Max Keys/Node**: 32
- **Page Size**: 4KB


