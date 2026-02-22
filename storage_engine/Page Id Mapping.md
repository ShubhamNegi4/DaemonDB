# Page ID Mapping: Local, Global, and File IDs

This document explains how DaemonDB manages page IDs across files, how heap and index files use this system, and how the catalog JSON files tie everything together across restarts.

---

## The Problem

DaemonDB stores data across multiple files:
- `1.heap` — row data for table1
- `2.idx`  — B+ tree index for table1
- `3.heap` — row data for table2
- `4.idx`  — B+ tree index for table2

Each file has its own pages numbered locally: `0, 1, 2, 3...`

The BufferPool needs a single unified address space to cache pages from all files. You can't just use local page numbers because file 1 and file 2 both have a "page 0" — they'd collide in the cache.

**The solution: global page IDs.**

---

## The Encoding

```
globalPageID = int64(fileID) << 32 | localPageNum
```

This packs both the file and local page number into a single 64-bit integer:

```
Bits 63-32: fileID       (which file)
Bits 31-0:  localPageNum (which page within that file)
```

**Examples:**

| fileID | localPage | globalPageID (decimal) | globalPageID (hex) |
|--------|-----------|------------------------|--------------------|
| 1 | 0 | 4294967296 | 0x1_00000000 |
| 1 | 1 | 4294967297 | 0x1_00000001 |
| 2 | 0 | 8589934592 | 0x2_00000000 |
| 2 | 1 | 8589934593 | 0x2_00000001 |
| 3 | 0 | 12884901888 | 0x3_00000000 |

From the logs you can verify:
```
[BufferPool] HIT  pageID=4294967296   ← file 1, page 0 (heap)
[BufferPool] HIT  pageID=8589934593   ← file 2, page 1 (index root)
```

**Converting back:**
```go
localPageNum := globalPageID & 0xFFFFFFFF  // lower 32 bits
fileID       := globalPageID >> 32          // upper 32 bits
```

**Why this works:** The encoding is deterministic — given `fileID` and `localPageNum`, you always get the same global ID. No counter, no state to drift across restarts.

---

## DiskManager Internal Maps

DiskManager maintains two maps for the global ↔ local mapping:

```go
type DiskManager struct {
    files         map[uint32]*FileDescriptor // fileID → OS file handle + NextPageID
    globalPageMap map[int64]uint32           // globalPageID → fileID
    localToGlobal map[PageKey]int64          // (fileID, localPageNum) → globalPageID
}

type PageKey struct {
    FileID   uint32
    LocalNum int64
}
```

These maps are populated in two ways:

**1. When allocating a new page (`AllocatePage`):**
```go
localPageNum := fd.NextPageID
fd.NextPageID++
globalPageID := int64(fileID)<<32 | localPageNum

dm.globalPageMap[globalPageID] = fileID
dm.localToGlobal[PageKey{FileID: fileID, LocalNum: localPageNum}] = globalPageID
```

**2. When reopening existing files on restart (`RegisterPage`):**
```go
// Called for each local page 0..NextPageID-1 when loading an existing file
globalPageID := int64(fileID)<<32 | localPageNum
dm.globalPageMap[globalPageID] = fileID
dm.localToGlobal[PageKey{FileID: fileID, LocalNum: localPageNum}] = globalPageID
```

Both paths produce identical results — `RegisterPage` on restart gives the same global IDs as `AllocatePage` originally did, because the encoding is deterministic.

---

## The fileID Problem and Why CatalogManager Owns It

DiskManager has a `nextFileID` counter that auto-increments when `OpenFile` is called. The problem is this counter resets to 1 on every restart.

If you have two tables and load them in random order (Go maps are unordered):

```
Restart A:
  OpenFile(1.heap) → DiskManager assigns fileID=1  ✓
  OpenFile(3.heap) → DiskManager assigns fileID=2  ✗ (should be 3)

Restart B (different map iteration order):
  OpenFile(3.heap) → DiskManager assigns fileID=1  ✗ (should be 3)
  OpenFile(1.heap) → DiskManager assigns fileID=2  ✗ (should be 1)
```

If fileIDs are wrong, global page IDs are wrong, and the buffer pool maps to the wrong pages → data corruption.

**The fix: `OpenFileWithID`**

```go
func (dm *DiskManager) OpenFileWithID(filePath string, catalogFileID uint32) (uint32, error) {
    // Registers the file under catalogFileID, not dm.nextFileID
    dm.files[catalogFileID] = fd
    if catalogFileID >= dm.nextFileID {
        dm.nextFileID = catalogFileID + 1  // keep nextFileID ahead for WAL files
    }
    return catalogFileID, nil
}
```

Heap files and index files always use `OpenFileWithID` with the ID from `CatalogManager`. WAL files use `OpenFile` with auto-assigned IDs (fine since WAL files dont need stable IDs).

---

## CatalogManager: The Source of Truth for FileIDs

The catalog manager owns the stable fileID assignment. These IDs are persisted to disk and survive restarts.

### JSON Files

**`metadata/table_file_mapping.json`**
```json
{
  "table1": {
    "heap_file_id": 1,
    "index_file_id": 2
  },
  "table2": {
    "heap_file_id": 3,
    "index_file_id": 4
  }
}
```

**`metadata/next_file_id.json`**
```json
5
```

This counter tells the catalog where to assign the next fileID when a new table is created.

### FileID Allocation (CreateTable)

```go
func (cm *CatalogManager) RegisterNewTable(schema TableSchema) (heapFileID, indexFileID uint32, err error) {
    heapFileID  = cm.nextFileID   // e.g. 3
    cm.nextFileID++
    indexFileID = cm.nextFileID   // e.g. 4
    cm.nextFileID++

    cm.TableToFileId[tableName] = TableFileMapping{
        HeapFileID:  heapFileID,
        IndexFileID: indexFileID,
    }

    cm.persistTableMapping()   // write table_file_mapping.json
    cm.persistNextFileID()     // write next_file_id.json
    return heapFileID, indexFileID, nil
}
```

### FileID Loading (UseDatabase / Restart)

```go
func (cm *CatalogManager) LoadTableFileMapping() error {
    data, _ := os.ReadFile("metadata/table_file_mapping.json")
    json.Unmarshal(data, &cm.TableToFileId)  // restore tableName → fileIDs

    counterData, _ := os.ReadFile("metadata/next_file_id.json")
    json.Unmarshal(counterData, &cm.nextFileID)  // restore counter
}
```

---

## Heap File: Full Lifecycle

### First Run (CreateTable)

```
1. CatalogManager.RegisterNewTable("table1")
   → heapFileID=1, indexFileID=2
   → writes table_file_mapping.json + next_file_id.json

2. HeapManager.CreateHeapfile("table1", fileID=1)
   → DiskManager.OpenFileWithID("tables/1.heap", catalogFileID=1)
      → dm.files[1] = {File: os.File, NextPageID: 0}
   → BufferPool.NewPage(fileID=1, PageTypeHeapData)
      → DiskManager.AllocatePage(1)
         → localPageNum=0, globalPageID = 1<<32|0 = 4294967296
         → dm.globalPageMap[4294967296] = 1
         → dm.localToGlobal[{1,0}] = 4294967296
      → returns Page{ID: 4294967296, FileID: 1}
   → InitHeapPage(pg)  ← zero out header, set free pointer
   → UnpinPage(4294967296, dirty=true)
   → hfm.files[1] = HeapFile{fileID: 1, tableName: "table1"}
   → hfm.tableIndex["table1"] = 1
```

### Restart (UseDatabase)

```
1. CatalogManager.LoadTableFileMapping()
   → TableToFileId = {"table1": {HeapFileID:1, IndexFileID:2}}
   → nextFileID = 3

2. for tableName, mapping := range GetAllTableMappings():
   HeapManager.LoadHeapFile(catalogFileID=1, "table1")
   → DiskManager.OpenFileWithID("tables/1.heap", 1)
      → dm.files[1] = {File: os.File, NextPageID: 2}  ← 2 pages on disk
   → GetFileDescriptor(1) → fd.NextPageID = 2
   → RegisterPage(1, localPage=0)
      → globalPageID = 1<<32|0 = 4294967296
      → dm.globalPageMap[4294967296] = 1
      → dm.localToGlobal[{1,0}] = 4294967296
   → RegisterPage(1, localPage=1)
      → globalPageID = 1<<32|1 = 4294967297
      → dm.globalPageMap[4294967297] = 1
      → dm.localToGlobal[{1,1}] = 4294967297
   → hfm.files[1] = HeapFile{fileID:1}
   → hfm.tableIndex["table1"] = 1

3. InsertRow later:
   → findSuitablePage → GetGlobalPageID(fileID=1, localPage=0) = 4294967296
   → BufferPool.FetchPage(4294967296) → HIT or read from disk at offset 0
   → RowPointer{FileID:1, PageNumber:0, SlotIndex:2}  ← local page number stored
```

### Row Pointer: Always Local

`RowPointer.PageNumber` is always the **local** page number. When reading a row:

```go
// getRow
globalPageID, _ := hf.diskManager.GetGlobalPageID(hf.fileID, int64(ptr.PageNumber))
pg, _ := hf.bufferPool.FetchPage(globalPageID)
// read slot ptr.SlotIndex from pg
```

This conversion is done fresh each time — the stored pointer stays local so its valid across restarts.

---

## Index File: Full Lifecycle

### First Run (CreateTable)

```
1. CatalogManager gives indexFileID=2

2. IndexManager.GetOrCreateIndex("table1", indexFileID=2)
   → OpenBPlusTree("indexes/table1_primary.idx", fileID=2, ...)
   → DiskManager.OpenFileWithID("table1_primary.idx", 2)
      → dm.files[2] = {File: os.File, NextPageID: 0}

   → isNew=true:
      AllocatePage(2, PageTypeMetadata)  ← page 0 reserved for root metadata
         → localPage=0, global = 2<<32|0 = 8589934592
         → dm.globalPageMap[8589934592] = 2
         → dm.localToGlobal[{2,0}] = 8589934592

      newNode(NodeLeaf)  ← root leaf node
         → BufferPool.NewPage(fileID=2)
            → AllocatePage(2)
               → localPage=1, global = 2<<32|1 = 8589934593
               → dm.globalPageMap[8589934593] = 2
               → dm.localToGlobal[{2,1}] = 8589934593
            → returns Page{ID: 8589934593}
         → Node{pageID: 8589934593}

      t.root = 8589934593  ← global in memory

      saveRoot():
         localRootID = 8589934593 & 0xFFFFFFFF = 1  ← strip to local
         WriteRootID(fileID=2, localRootID=1)
         → WriteMetadata writes localRootID=1 at bytes 9-16 of page 0 on disk
```

### Restart (UseDatabase)

```
1. IndexManager.LoadIndex("table1", indexFileID=2)
   → OpenBPlusTree("table1_primary.idx", fileID=2, ...)
   → DiskManager.OpenFileWithID("table1_primary.idx", 2)
      → dm.files[2] = {File: os.File, NextPageID: 2}

   → isNew=false:
      RegisterPage(2, localPage=0)  → global = 2<<32|0 = 8589934592
      RegisterPage(2, localPage=1)  → global = 2<<32|1 = 8589934593

      ReadRootID(fileID=2)
      → ReadMetadata reads bytes 9-16 of page 0 → localRootID = 1

      GetGlobalPageID(fileID=2, localRootID=1)
      → globalRootID = 2<<32|1 = 8589934593

      t.root = 8589934593  ← correct global in memory, same as first run
```

### Node Serialization: Local IDs on Disk

When a node is written to disk (`SerializeNode`), all page references are stripped to local:

```go
// pageID: global 8589934593 → local 1
localPageID := node.pageID & 0xFFFFFFFF  // = 1

// parent: global 8589934592 → local 0 (or -1 if root)
localParent := node.parent & 0xFFFFFFFF  // = 0

// next: global 8589934594 → local 2 (or -1 if none)
localNext := node.next & 0xFFFFFFFF      // = 2

// children (internal nodes): same treatment
localChild := childID & 0xFFFFFFFF
```

When read back (`DeserializeNode`), global IDs are reconstructed:

```go
// fileID is passed in from BPlusTree.fileID = 2
node.pageID = int64(fileID)<<32 | (localPageID & 0xFFFFFFFF)
node.parent = int64(fileID)<<32 | (localParent & 0xFFFFFFFF)
node.next   = int64(fileID)<<32 | (localNext   & 0xFFFFFFFF)
// children same
```

Then `fetchNode` always overrides `n.pageID = pageID` (the actual global ID used to fetch) — so even if the stored value is wrong, the in-memory node always has the correct global ID.

---

## Full Restart Sequence

```
go run main.go
  ↓
USE demoDB
  ↓
1. NewDiskManager()          → empty maps, nextFileID=1
2. NewBufferPool(capacity)   → empty cache
3. NewHeapFileManager(dir)   → empty files map
4. NewIndexFileManager(dir)  → empty indexes map
5. NewWALManager(dir)        → recover segments
6. CatalogManager.SetCurrentDatabase("demoDB")
7. CatalogManager.LoadTableFileMapping()
   → reads metadata/table_file_mapping.json
   → TableToFileId = {"table1":{1,2}, "table2":{3,4}}
   → reads metadata/next_file_id.json → nextFileID=5
8. CatalogManager.LoadAllTableSchemas()
   → reads tables/table1_schema.json, tables/table2_schema.json
9. for each table in TableToFileId (random order, but fileIDs are explicit):
   HeapManager.LoadHeapFile(catalogFileID=1, "table1")
     → OpenFileWithID("tables/1.heap", 1) → dm.files[1]
     → RegisterPage(1, 0..N) → rebuild localToGlobal for file 1
     → hfm.files[1] = HeapFile, hfm.tableIndex["table1"]=1

   IndexManager.LoadIndex("table1", indexFileID=2)
     → OpenBPlusTree("indexes/table1_primary.idx", 2)
     → OpenFileWithID(..., 2) → dm.files[2]
     → RegisterPage(2, 0..M) → rebuild localToGlobal for file 2
     → ReadRootID → localRoot=1 → globalRoot=2<<32|1
     → t.root = globalRoot

   HeapManager.LoadHeapFile(catalogFileID=3, "table2")  ← order doesn't matter
     → OpenFileWithID("tables/3.heap", 3) → dm.files[3]
     → RegisterPage(3, 0..P)

   IndexManager.LoadIndex("table2", indexFileID=4)
     → OpenFileWithID(..., 4) → dm.files[4]
     → RegisterPage(4, 0..Q)

10. WAL recovery (redo/undo)
11. "Switched to database: demoDB"
```

After step 9, `dm.files`, `dm.globalPageMap`, and `dm.localToGlobal` are fully rebuilt — identical to what they were at the end of the previous session. The buffer pool starts empty (cold cache) but reads from disk will reconstruct any needed pages.

---

## Why byte 8 is Reserved in Every Page

`WritePage` stamps the page type at `pg.Data[8]` every time a page is flushed:

```go
pg.Data[8] = byte(pg.PageType)
```

This means **every page format must treat byte 8 as reserved** — it will be overwritten on every flush.

- **Heap pages**: byte 8 is part of the header, but the header is written by `InitHeapPage`/accessors — the page type stamp at byte 8 is the `PageType` field itself, which is consistent.
- **B+ tree nodes**: `SerializeNode` skips byte 8 explicitly (`offset += 1` after writing pageID bytes 0-7). The `isLeaf` flag is at byte 9, not byte 8.
- **Metadata page (page 0 of index files)**: `WriteMetadata` constructs a fresh buffer with `metaPage[8] = byte(PageTypeMetadata)` and data starting at byte 9.

If any format accidentally uses byte 8 for its own data, it will be silently corrupted on the next flush — a subtle and hard-to-debug bug.