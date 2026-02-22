<div align="center">
  <img src="./Sample_Image/Necessary_Image.png" alt="DaemonDB Logo" width="300" />
</div>

<h1 align="center">DaemonDB: Lightweight Relational Database Engine</h1>

DaemonDB is a lightweight relational database engine built from scratch in Go. It implements core database concepts including B+ tree indexing, heap file storage, SQL parsing, a bytecode executor, WAL-based durability, and basic transaction support. The project is designed for education: every subsystem is small enough to read, yet complete enough to show the real mechanics.


## Architecture

The database follows a layered architecture separating storage, indexing, and query processing:


```
VM (VDBE) - Orchestrates operations, does NOT write to disk
    ↓
    └─→ StorageEngine - Coordinates all subsystems
            ├─→ HeapFileManager  - Writes ROW DATA to disk
            ├─→ IndexFileManager - Writes INDEX DATA to disk (B+ Tree)
            ├─→ WALManager       - fsync operations to Disk → Replay Logs
            ├─→ CatalogManager   - Schema + file ID metadata
            └─→ TxnManager       - Transaction lifecycle + rollback records
                    ↓
            DiskManager  - OS file handles, global↔local page ID mapping
            BufferPool   - Page cache, pinning, LRU eviction, dirty flushing
```
### Layered View

```
┌─────────────────────────────────────────┐
│         SQL Query Layer                  │
│  (Parser → Code Generator → VM)         │
└──────────────┬──────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────┐
│         StorageEngine                   │
│  (Insert/Select/Update/Delete + Txn)    │
└──────────┬──────────┬───────────────────┘
           │          │
           ▼          ▼
┌──────────────┐  ┌──────────────────────┐
│ HeapFile     │  │ B+ Tree Index        │
│ (Row Data)   │  │ PK → RowPointer      │
└──────────────┘  └──────────────────────┘
           │          │
           ▼          ▼
┌─────────────────────────────────────────┐
│   BufferPool + DiskManager              │
│   (4KB Pages, LRU Cache, File I/O)      │
└─────────────────────────────────────────┘
```


## Supported Statements

```sql
-- Table creation
CREATE TABLE students ( id int primary key, name string, age int, grade string )

-- Data insertion
INSERT INTO students VALUES ("S001", "Alice", 20, "A")

-- Data querying
SELECT * FROM students
SELECT name, grade FROM students WHERE id = "S001"

-- Joins
SELECT * FROM t1 [ INNER|LEFT|RIGHT|FULL ] JOIN t2 ON col1 = col2 [ WHERE ... ]

-- Updates
UPDATE students SET name = "Bob" WHERE id = "S001"
UPDATE students SET id = id + 3 WHERE id = 5

-- Transactions
BEGIN
COMMIT
ROLLBACK
```

---

## Component Details

### VM / VDBE (`query_executor/`)

The virtual machine executes bytecode compiled from parsed SQL. It does not touch disk directly — all persistence goes through the StorageEngine.

**Opcodes:**

| Opcode | Description |
|--------|-------------|
| `OP_PUSH_VAL` | Push a literal value onto the stack |
| `OP_PUSH_KEY` | Push a key onto the stack |
| `OP_CREATE_DB` | Create a new database directory |
| `OP_USE_DB` | Switch active database |
| `OP_CREATE_TABLE` | Create table schema + heap file + index |
| `OP_INSERT` | Insert a row into a table |
| `OP_SELECT` | Query rows from a table |
| `OP_UPDATE` | Update rows matching a WHERE clause |
| `OP_TXN_BEGIN` | Begin an explicit transaction |
| `OP_TXN_COMMIT` | Commit the active transaction |
| `OP_TXN_ROLLBACK` | Rollback the active transaction |
| `OP_END` | End of instruction stream |

**Auto-transactions:** If no explicit `BEGIN` is issued, the VM wraps each DML statement in an implicit transaction that commits or aborts atomically.

---

### StorageEngine (`storage_engine/`)

Coordinates all subsystems. Entry points: `InsertRow`, `UpdateRow`, `DeleteRow`, `ExecuteSelect`.

**Insert flow:**
1. Load schema from CatalogManager
2. Validate foreign key constraints via index lookup
3. Serialize row to binary
4. Allocate LSN from WALManager
5. Insert row into heap file → get `RowPointer`
6. Append `OpInsert` to WAL (rowData, rowPtr, LSN)
7. Insert `(pkBytes → RowPointer)` into B+ tree index
8. Record insert in txn for rollback

**Update flow:**
1. Full scan (or PK lookup) to find matching rows
2. Fetch before-image (old row data) from heap
3. Update row in heap (may relocate if row grew)
4. Append `OpUpdate` to WAL with **both** before-image (`OldRowData`, `OldRowPtr`) and after-image
5. Update B+ tree index if PK changed or row relocated

**Delete flow:**
1. Find matching rows
2. Fetch before-image for undo
3. Tombstone slot in heap
4. Append `OpDelete` to WAL with before-image
5. Delete from B+ tree index

---

### DiskManager (`storage_engine/disk_manager/`)

Owns OS file handles and the global page ID space.

**Page ID encoding (deterministic, no counter):**
```
globalPageID = int64(fileID) << 32 | localPageNum
```

Global IDs are stable across restarts regardless of file load order.

**Key functions:**

| Function | Purpose |
|----------|---------|
| `OpenFileWithID(path, catalogFileID)` | Open a file registered under the catalog's stable fileID |
| `OpenFile(path)` | Open a file with auto-assigned ID (WAL segments only) |
| `AllocatePage(fileID, pageType)` | Reserve a new page, update `localToGlobal` map |
| `RegisterPage(fileID, localPageNum)` | Re-register existing pages on restart |
| `GetGlobalPageID(fileID, localNum)` | `int64(fileID)<<32 \| localNum` |
| `GetLocalPageID(fileID, globalID)` | `globalID & 0xFFFFFFFF` |

**Why two OpenFile variants:**

| Function | Used for | FileID source |
|----------|----------|---------------|
| `OpenFileWithID` | Heap files, index files | CatalogManager (stable across restarts) |
| `OpenFile` | WAL segments | DiskManager counter (session-scoped) |

---

### BufferPool (`storage_engine/bufferpool/`)

Fixed-capacity page cache with LRU eviction.

- Pages identified by `globalPageID`
- Cache miss → `DiskManager.ReadPage` loads from disk
- Eviction → dirty pages flushed via `DiskManager.WritePage`
- `NewPage(fileID, pageType)` allocates via `DiskManager.AllocatePage`
- `FetchPage(globalPageID)` increments pin count — caller must `UnpinPage` when done

**Page type byte:** `WritePage` stamps `pg.Data[8] = byte(pg.PageType)` on every write. All page formats must treat byte 8 as reserved for this stamp.

---

### HeapFileManager (`storage_engine/access/heapfile_manager/`)

Manages row storage in `.heap` files — one file per table.

**File path:** `database/{db}/tables/{catalogFileID}.heap`

**Page layout:**

```
┌─────────────────────────────────────────┐
│ Page (4096 bytes)                        │
├─────────────────────────────────────────┤
│ [0-7]  localPageID                       │
│ [8]    page type (stamped by WritePage)  │
│ [9-28] header: numRows, slotCount,       │
│         recordEndPtr, slotRegionStart,   │
│         isPageFull, LSN                  │
├─────────────────────────────────────────┤
│ Row 1 | Row 2 | Row 3 | ...             │
│ (records grow forward →)                 │
├─────────────────────────────────────────┤
│ ← slot directory grows backward          │
│ Slot N | ... | Slot 1 | Slot 0          │
│ (4 bytes each: offset uint16 + len uint16│
└─────────────────────────────────────────┘
```

**Slot:** `offset=0 && length=0` means tombstoned (deleted row).

**Row pointer:** `(fileID uint32, pageNumber uint32, slotIndex uint16)` — `pageNumber` is always the **local** page number.

---

### IndexFileManager + B+ Tree (`storage_engine/access/indexfile_manager/`)

Manages primary key indexes using a B+ tree stored in `.idx` files.

**File path:** `database/{db}/indexes/{tableName}_primary.idx`

**File layout:**
```
Page 0:  metadata — local root page ID stored at bytes 9–16
Page 1+: B+ tree nodes
```

**Node serialization layout (4096 bytes):**

```
[0-7]   local pageID
[8]     RESERVED — page type stamp (WritePage writes here, must not be used by node data)
[9]     isLeaf (1=leaf, 0=internal)
[10-11] numKeys (int16)
[12-19] local parent page ID (-1 if root)
[20-27] local next page ID (leaf linked list, -1 if none)
[28-34] reserved padding (7 bytes)
[35+]   keys, then values (leaf) or local child IDs (internal)
```

**Local vs global IDs — the core rule:**
- **On disk:** all page IDs stored as **local** (lower 32 bits)
- **In memory:** all page IDs are **global** (`int64(fileID)<<32 | local`)
- `SerializeNode` masks to local before writing
- `DeserializeNode` reconstructs global on read using `fileID`
- `fetchNode` always overrides `n.pageID = pageID` (the actual global ID) after deserialize

**Tree invariants:**
- Internal nodes: `len(children) == len(keys) + 1`
- Leaf nodes: `len(values) == len(keys)`
- All leaves at same depth, linked via `next` for range scans
- `MaxKeys=32`, `MinKeys=16`

---

### WALManager (`storage_engine/wal/`)

Write-ahead log for crash recovery.

**Operation record:**

```go
type Operation struct {
    Type       OpType      // OpInsert, OpUpdate, OpDelete, OpBegin, OpCommit, OpAbort
    TxnID      uint64
    Table      string
    LSN        uint64
    RowData    []byte      // after-image (new data)
    OldRowData []byte      // before-image (old data) — for UPDATE/DELETE undo
    RowPtr     RowPointer  // new row location
    OldRowPtr  RowPointer  // old row location — for UPDATE undo
}
```

**Sync strategy:**
- Auto-commit transactions: `fsync` after every statement
- Explicit transactions: `fsync` only on `COMMIT`

**Recovery (ARIES-style):**
1. Load latest checkpoint LSN
2. Scan WAL forward — collect all ops, identify committed vs aborted txns
3. REDO committed ops not yet on disk (page LSN < op LSN)
4. UNDO ops from aborted/uncommitted txns in reverse LSN order

**UNDO per operation:**

| Op | UNDO action |
|----|-------------|
| `OpInsert` | `HeapManager.DeleteRow(rowPtr)` + `BTree.Delete(pkBytes)` |
| `OpUpdate` | `HeapManager.UpdateRow(newRowPtr, oldRowData)` + index fixup |
| `OpDelete` | `HeapManager.InsertRow(oldRowData)` + `BTree.Insert(pkBytes, rowPtr)` |

---

### CatalogManager (`storage_engine/catalog/`)

Manages schema and file ID metadata, persisted to the `metadata/` directory.

**Persisted files:**

| File | Contents |
|------|----------|
| `metadata/table_file_mapping.json` | `tableName → {heap_file_id, index_file_id}` |
| `metadata/next_file_id.json` | Next fileID counter |
| `tables/{tableName}_schema.json` | Column definitions, PK flag, foreign keys |

**FileID allocation:** Each table gets two consecutive file IDs — one for heap, one for index. Counter is persisted and restored on restart.

**Startup sequence (`UseDatabase`):**
1. `LoadTableFileMapping()` — restore `tableName → fileIDs` from disk
2. `LoadAllTableSchemas()` — restore column definitions
3. For each table: `HeapManager.LoadHeapFile(catalogFileID, tableName)`
4. For each table: `IndexManager.LoadIndex(tableName, indexFileID)`
5. WAL recovery

---


## Project Structure

```
DaemonDB/
├── main.go
├── parser/           — SQL parser (AST)
├── compiler/         — AST → bytecode
├── query_executor/   — VM, instruction execution
├── storage_engine/
│   ├── access/
│   │   ├── heapfile_manager/   — row storage
│   │   └── indexfile_manager/  — B+ tree index
│   │       └── bplustree/      — node ops, serialization, splits
│   ├── bufferpool/             — page cache
│   ├── catalog/                — schema + file ID metadata
│   ├── disk_manager/           — OS file I/O, page ID mapping
│   ├── page/                   — page struct, slot ops
│   ├── transaction_manager/    — txn lifecycle, rollback records
│   └── wal/                    — write-ahead log
├── types/            — shared types (PageType, RowPointer, Operation, etc.)
└── database/         — data directory (created at runtime)
    └── {dbName}/
        ├── tables/   — {fileID}.heap, {tableName}_schema.json
        ├── indexes/  — {tableName}_primary.idx
        ├── logs/     — wal_{segmentID}.log
        └── metadata/ — table_file_mapping.json, next_file_id.json
```


## Quick Start
```bash
go run main.go
```

```sql
CREATE DATABASE demoDB
USE demoDB
CREATE TABLE students ( id int primary key, name string, age int )

INSERT INTO students VALUES (1, "Alice", 20)
INSERT INTO students VALUES (2, "Bob", 21)

SELECT * FROM students
SELECT * FROM students WHERE id = 2

BEGIN
INSERT INTO students VALUES (3, "Carol", 22)
ROLLBACK

SELECT * FROM students   -- Carol not present (rolled back)
```



## Data Flow Example

### INSERT

```
SQL: INSERT INTO mytable VALUES (5)
  ↓ Parser → AST → Compiler → Bytecode
  ↓ VM.ExecuteInsert
  ↓ StorageEngine.InsertRow(txn, "mytable", [5])
      ├── CatalogManager.GetTableSchema
      ├── SerializeRow([5]) → rowBytes
      ├── WAL.AllocateLSN()
      ├── HeapManager.InsertRow(heapFileID, rowBytes, lsn)
      │       └── findSuitablePage → InsertRecord
      │           → RowPointer{file=1, page=0, slot=0}
      ├── WAL.AppendToBuffer(OpInsert, rowBytes, rowPtr)
      ├── BTree.Insertion(pkBytes, rowPtrBytes)
      └── txn.RecordInsert(table, rowPtr, pkBytes)
```

### SELECT with PK lookup

```
SQL: SELECT * FROM mytable WHERE id = 5
  ↓ StorageEngine.ExecuteSelect
      ├── [PK column detected]
      ├── BTree.Search(pkBytes) → rowPtrBytes
      ├── DeserializeRowPointer → RowPointer{file=1, page=0, slot=0}
      ├── HeapManager.GetRow(rowPtr) → rowBytes
      └── DeserializeRow → result row
```

### SELECT full scan

```
SQL: SELECT * FROM mytable
  ↓ StorageEngine.selectFullScan
      ├── HeapManager.GetAllRowPointers()
      │       └── iterate all pages → collect live slots
      └── for each ptr: GetRow → DeserializeRow → result
```


---

## Current Status

| Component | Status | Notes |
|-----------|--------|-------|
| B+ Tree (insert/search/delete/range) | ✅ Complete | Local/global ID encoding for cross-restart correctness |
| Heap file storage | ✅ Complete | Slot directory, page allocation, full scan |
| SQL Parser | ✅ Complete | DDL, DML, joins, transactions |
| Code Generator | ✅ Complete | AST → bytecode |
| INSERT execution | ✅ Complete | Heap + index + WAL |
| SELECT execution | ✅ Complete | PK lookup O(log n) + full scan |
| UPDATE execution | ✅ Complete | Before-image fetch, heap update, index fixup |
| WAL + crash recovery | ✅ Complete | REDO committed, UNDO aborted (before+after image) |
| Transactions (BEGIN/COMMIT/ROLLBACK) | ✅ Complete | Logical undo via WAL |
| Buffer pool (LRU, pin/unpin) | ✅ Complete | Shared across heap + index |
| CatalogManager | ✅ Complete | Stable fileIDs persisted across restarts |

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
- **Storage**: Heap files
- **Indexing**: B+ tree (Index files)
- **Query Language**: SQL with DDL/DML, joins, PK-based WHERE
- **Transactions**: BEGIN/COMMIT/ROLLBACK, WAL-backed durability
- **Concurrency**: Thread-safe with mutex locks
- **Architecture**: Index-organized (B+ tree points to heap file rows)


## Future Work

- [ ] Executor support for DELETE
- [ ] Secondary indexes and non-PK predicates
- [ ] Garbage collection / compaction for tombstoned rows

## License

This project is licensed under the MIT License.

## Contributing

This is an educational project built for learning database internals. Contributions and suggestions are welcome!
