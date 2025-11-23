# Buffer Pool System - Complete Explanation

## 🎯 Purpose

The buffer pool is a **cache layer** between the B+ tree operations and disk storage. It keeps frequently accessed nodes in memory to avoid expensive disk I/O operations.

**Without buffer pool:** Every tree operation = disk read/write (SLOW)  
**With buffer pool:** Most operations = memory access (FAST)

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│              B+ Tree Operations                         │
│  (Insert, Delete, Search, Range Scan)                  │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────┐
│              Buffer Pool (Cache)                        │
│  ┌──────────────────────────────────────────────────┐   │
│  │  pages: map[int64]*Node  (in-memory cache)      │   │
│  │  capacity: 10  (max nodes in cache)             │   │
│  │  accessOrder: []int64  (LRU tracking)           │   │
│  └──────────────────────────────────────────────────┘   │
└──────────────────┬──────────────────────────────────────┘
                   │
                   ▼ (cache miss)
┌─────────────────────────────────────────────────────────┐
│              Pager (Disk I/O)                            │
│  - ReadPage(pageID) → loads 4KB from disk                │
│  - WritePage(pageID, data) → writes 4KB to disk         │
└─────────────────────────────────────────────────────────┘
```

---

## 📦 Data Structures

### BufferPool Struct

```go
type BufferPool struct {
    mu          sync.Mutex      // Thread safety
    pages       map[int64]*Node  // Cache: pageID → Node
    capacity    int              // Max nodes in cache
    pager       Pager            // Disk I/O interface
    accessOrder []int64          // LRU: oldest → newest
}
```

**Key Fields:**
- **`pages`**: The actual cache - stores Node objects by their page ID
- **`capacity`**: Maximum number of nodes that can be cached (e.g., 10)
- **`accessOrder`**: Tracks which nodes were accessed when (for LRU eviction)
- **`pager`**: Interface to read/write pages from disk

---

## 🔄 Core Operations

### 1. **Get(pageID)** - Read a Node

**Flow:**
```
1. Check cache (pages map)
   ├─ HIT: Node found in memory
   │   ├─ Update LRU order (move to end)
   │   └─ Return node immediately ✅
   │
   └─ MISS: Node not in cache
       ├─ Read 4KB page from disk (via pager)
       ├─ Decode page → Node (deserialize)
       ├─ Add to cache (may evict if full)
       └─ Return node ✅
```

**Example:**
```go
node, err := cache.Get(5)  // Get node at page ID 5
// If in cache: instant return
// If not: loads from disk, decodes, caches, then returns
```

**Why it's fast:**
- Cache hit: O(1) map lookup (microseconds)
- Cache miss: Disk read + decode (milliseconds, but only when needed)

---

### 2. **Put(node)** - Add/Update a Node

**Flow:**
```
1. Check if node already in cache
   ├─ YES: Update access order (mark as recently used)
   └─ NO: Continue
   
2. Check if cache is full
   ├─ YES: Evict LRU unpinned node
   │   ├─ If dirty: Write to disk first
   │   └─ Remove from cache
   └─ NO: Continue
   
3. Add node to cache
4. Update access order
```

**Example:**
```go
newNode := NewNode(NodeLeaf)
newNode.id = 10
cache.Put(newNode)  // Adds to cache, evicts if needed
```

---

### 3. **Pin/Unpin** - Prevent Eviction

**Problem:** During tree traversal, you might need to keep multiple nodes in memory. If the cache fills up, you don't want to evict a node you're currently using!

**Solution:** Pin nodes that are actively being used.

```go
// During tree traversal
cache.Pin(rootID)        // Prevent root from being evicted
cache.Pin(leafID)        // Prevent leaf from being evicted

// ... do operations ...

cache.Unpin(rootID)      // Allow eviction again
cache.Unpin(leafID)
```

**How it works:**
- Each Node has a `pincnt` field (pin count)
- `Pin()` increments `pincnt`
- `Unpin()` decrements `pincnt`
- Eviction only happens when `pincnt == 0`

---

### 4. **MarkDirty(pageID)** - Track Modifications

**Problem:** If you modify a node in memory, you need to write it back to disk eventually.

**Solution:** Mark nodes as "dirty" when modified.

```go
node := cache.Get(5)
node.key[0] = []byte("modified")  // Modify node
cache.MarkDirty(5)                // Mark as dirty
```

**What happens:**
- `isDirty` flag is set on the node
- When evicted or flushed, dirty nodes are written to disk
- After write, `isDirty` is cleared

---

### 5. **Flush()** - Write All Dirty Nodes to Disk

**Purpose:** Ensure all modifications are persisted to disk.

```go
cache.Flush()  // Writes all dirty nodes to disk
```

**Flow:**
```
For each node in cache:
  ├─ If dirty:
  │   ├─ Encode node → 4KB page
  │   ├─ Write to disk via pager
  │   └─ Clear dirty flag
  └─ Continue
```

**When to use:**
- Before closing the database
- At transaction commit
- Periodically for durability

---

## 🔀 LRU Eviction Algorithm

**LRU = Least Recently Used**

**Goal:** When cache is full, evict the node that hasn't been accessed in the longest time.

### How it works:

1. **Access Order Tracking:**
   ```
   accessOrder = [1, 3, 5, 2, 4]
   //              ↑ oldest    ↑ newest
   ```

2. **On Access (Get/Put):**
   ```go
   // Node 3 is accessed
   accessOrder = [1, 5, 2, 4, 3]  // Move 3 to end
   ```

3. **On Eviction:**
   ```go
   // Cache full, need to evict
   // Find first unpinned node in accessOrder
   // Evict node 1 (oldest, unpinned)
   accessOrder = [5, 2, 4, 3]  // Remove 1
   ```

### Eviction Rules:

1. ✅ Can evict: `pincnt == 0` (not pinned)
2. ❌ Cannot evict: `pincnt > 0` (pinned)
3. 💾 If dirty: Write to disk before evicting

---

## 🔗 Integration with Node Serialization

### Encoding (Node → Disk)

```go
node := &Node{
    id: 5,
    nodeType: NodeLeaf,
    key: [][]byte{[]byte("key1"), []byte("key2")},
    vals: [][]byte{[]byte("val1"), []byte("val2")},
    numKeys: 2,
}

// Encode to 4KB page
pageData := encodeNode(node)  // Node → []byte (4096 bytes)

// Write to disk
pager.WritePage(5, pageData)
```

### Decoding (Disk → Node)

```go
// Read 4KB page from disk
pageData := pager.ReadPage(5)  // []byte (4096 bytes)

// Decode to Node
node := decodeNode(pageData, 5)  // []byte → Node
```

**Why 4KB?**
- Standard page size for efficient disk I/O
- Matches OS page size
- Good balance between memory and disk efficiency

---

## 📊 Example: Complete Operation Flow

### Scenario: Search for key "S001"

```
1. B+ Tree calls: cache.Get(rootID)
   ├─ Cache MISS
   ├─ Pager reads page from disk
   ├─ Decode page → root Node
   ├─ Add root to cache
   └─ Return root Node

2. B+ Tree navigates: root → internal → leaf
   ├─ cache.Get(internalID)  → Cache MISS → Load from disk
   ├─ cache.Get(leafID)      → Cache MISS → Load from disk
   └─ All nodes now in cache

3. Search finds key in leaf
   └─ Return value ✅

4. Next search for "S002"
   ├─ cache.Get(rootID)     → Cache HIT ✅ (instant)
   ├─ cache.Get(internalID) → Cache HIT ✅ (instant)
   ├─ cache.Get(leafID)     → Cache HIT ✅ (instant)
   └─ Much faster! 🚀
```

---

## 🎯 Benefits

### Performance
- **Cache Hit:** ~1 microsecond (memory access)
- **Cache Miss:** ~1-10 milliseconds (disk I/O)
- **Speedup:** 1000-10000x faster for cached nodes

### Memory Efficiency
- Only keeps frequently used nodes in memory
- Automatically evicts unused nodes
- Respects capacity limit

### Durability
- Dirty nodes written to disk on eviction
- Flush ensures all changes persisted
- No data loss on crashes (if flushed)

---

## 🔒 Thread Safety

All buffer pool operations are **thread-safe**:
- `sync.Mutex` protects all operations
- Multiple goroutines can safely call Get/Put/Pin/Unpin
- No race conditions

---

## 📝 Summary

**Buffer Pool = Smart Cache for Database Pages**

1. **Caches** frequently accessed nodes in memory
2. **Loads** from disk on cache miss
3. **Evicts** least recently used nodes when full
4. **Protects** pinned nodes from eviction
5. **Tracks** dirty nodes for write-back
6. **Flushes** all changes to disk when needed

**Result:** Fast database operations with efficient memory usage! 🚀

