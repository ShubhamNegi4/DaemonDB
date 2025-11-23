# B+ Tree Implementation

This directory contains a complete B+ tree implementation with disk persistence, buffer pooling, and node serialization.

## File Organization

### Core Data Structures
- **`struct.go`** - Core type definitions:
  - `Node` struct (internal/leaf nodes)
  - `BPlusTree` struct
  - `Pager` interface
  - Constants (PageSize, MaxKeys, etc.)

### Tree Operations
- **`new_bplus_tree.go`** - Tree constructor
- **`new_node.go`** - Node constructor
- **`insertion.go`** - Insert operation
- **`deletion.go`** - Delete operation
- **`search.go`** - Point lookup (Search)
- **`find_leaf.go`** - Navigate to leaf node
- **`iterator.go`** - Range scan (SeekGE, Next)

### Tree Maintenance
- **`split_leaf.go`** - Leaf node splitting
- **`split_internal.go`** - Internal node splitting
- **`parent_insert.go`** - Parent propagation after splits

### Utilities
- **`binary_search.go`** - Binary search helpers for keys

### Persistence Layer
- **`inmemory_pager.go`** - In-memory pager (for testing)
- **`disk_pager.go`** - Disk-based pager (production)
- **`buffer_pool.go`** - LRU buffer pool with eviction
- **`node_codec.go`** - Node serialization/deserialization

### Testing & Examples
- **`bplus.go`** - Demo/test function
- **`*_test.go`** - Test files

## Architecture

```
BPlusTree
├── Pager (InMemoryPager | OnDiskPager)
│   └── Handles page-level I/O (4KB pages)
├── BufferPool
│   ├── LRU cache of nodes
│   ├── Loads from disk on cache miss
│   └── Evicts unpinned nodes when full
└── Node Codec
    ├── encodeNode: Node → []byte (4KB page)
    └── decodeNode: []byte → Node
```

## Current Status

### ✅ Completed
- B+ tree operations (insert, delete, search, range scan)
- Disk pager with proper I/O
- Buffer pool with LRU eviction
- Node serialization/deserialization
- Pin/Unpin for preventing eviction
- Dirty page tracking and flushing

### ⚠️ Known Issues
- **Direct cache access**: Many B+ tree operations directly access `cache.pages[nodeId]` instead of using `cache.Get(nodeId)`. This bypasses:
  - Cache miss handling (loading from disk)
  - LRU tracking
  - Proper eviction
  
  **Files affected:**
  - `insertion.go`
  - `deletion.go`
  - `find_leaf.go`
  - `split_leaf.go`
  - `split_internal.go`
  - `parent_insert.go`
  - `iterator.go`

### 🔄 Recommended Refactoring
Replace all `t.cache.pages[nodeId]` with:
- `t.cache.Get(nodeId)` - for reading nodes
- `t.cache.Put(node)` - for adding/modifying nodes
- `t.cache.MarkDirty(nodeId)` - when modifying nodes
- `t.cache.Pin(nodeId)` / `t.cache.Unpin(nodeId)` - during traversals

## Usage

```go
// Create pager and buffer pool
pager := bplus.NewOnDiskPager("index.idx")
cache := bplus.NewBufferPool(10)

// Create tree
tree := bplus.NewBPlusTree(pager, cache, bytes.Compare)

// Operations
tree.Insertion([]byte("key"), []byte("value"))
result, _ := tree.Search([]byte("key"))
tree.Delete([]byte("key"))

// Range scan
iter := tree.SeekGE([]byte("key"))
for iter.Valid() {
    key := iter.Key()
    value := iter.Value()
    iter.Next()
}
```

