# DaemonDB – Audit: Broken, Half-Done, and Non-Working Items

This document lists every file and area that is broken, incomplete, or inconsistent so you can fix them before building new components.

---

## 1. **main.go**

| Issue | Severity | Details |
|-------|----------|---------|
| **Hardcoded WAL path** | Medium | `wal_manager.OpenWAL("databases/demoDB/logs")` is fixed; WAL path should depend on current database. |
| **VM uses unused in-memory tree** | Low | `tree := bplus.NewBPlusTree(pager, cache, bytes.Compare)` is passed to VM but **never used** for table data. Table indexes are opened on demand via `GetOrCreateIndex()` and are not cached (see query_executor). |
| **REPL always dumps AST and bytecode** | Low | Every query prints `=== AST ===`, `=== Bytecode ===`, `=== Execution ===`. Fine for debugging; consider a quiet mode for production. |

---

## 2. **query_executor/**

### **executor.go**

| Issue | Severity | Details |
|-------|----------|---------|
| **UPDATE/DELETE not implemented** | High | Parser supports `UPDATE`; VM has **no** `OP_UPDATE` or `OP_DELETE`. UPDATE/DELETE are documented as “parser exists, executor not implemented”. |
| **Hardcoded initial DB** | Low | `currDb: "demoDB"` in `NewVM`; first command should typically be `USE <db>`. |
| **Debug prints left in** | Low | `fmt.Printf("DEBUG: Joined %d rows before filtering\n", ...)` and `"DEBUG: %d rows left after filtering"` in join path (lines ~814, ~823). |
| **println for CREATE DB** | Low | `println("make a db with: ", dbName)` (line 198) – inconsistent with `fmt.Printf` elsewhere. |

### **helpers.go**

| Issue | Severity | Details |
|-------|----------|---------|
| **Index opened every time, never cached** | **Critical** | `GetOrCreateIndex(tableName)` calls `OpenBPlusTree(indexPath)` on **every** INSERT, SELECT (with WHERE), and rollback. Each call opens a new `OnDiskPager` (new file handle) and new `BufferPool`, and **never closes** them. Result: file handle leak and multiple open handles to the same `.idx` file. **Fix:** Cache `map[tableName]*BPlusTree` (or similar) and reuse; close indexes when switching DB or on shutdown. |
| **Outdated TODO** | Low | Comment says “TODO: implement root persistence” (line 791). Root **is** persisted: `NewBPlusTree` in `bplustree/new_bplus_tree.go` loads root from page 0. Remove or update the TODO. |

### **structs.go**

| Issue | Severity | Details |
|-------|----------|---------|
| **No OP_UPDATE / OP_DELETE** | High | `OpCode` has no `OP_UPDATE` or `OP_DELETE`, so UPDATE/DELETE cannot be executed even if codegen emitted them. |

---

## 3. **query_parser/**

### **parser/parser.go**

| Issue | Severity | Details |
|-------|----------|---------|
| **Errors implemented as panic** | High | `expect()`, `parseUseDatabase()`, `parseCreateTable()` (FOREIGN KEY), `parseInsert()`, `parseUpdate()` path use `panic()` on parse errors. Any invalid input kills the process. **Fix:** Return `(Statement, error)` (or similar) and let caller handle errors. |
| **Debug prints** | Low | `print(p.curToken.Kind, p.curToken.Value)` in `parseShowDatabases()` (line 98); `fmt.Print(p.curToken)` in CREATE TABLE path (line 66); `fmt.Println("parsing join")` in `parseJoin()` (line 358). |

### **parser/parser.go – UPDATE**

| Issue | Severity | Details |
|-------|----------|---------|
| **UPDATE has no WHERE** | High | `UpdateStmt` has `Table` and `Assignments` only. Parser does **not** parse `WHERE col = val` for UPDATE. So “UPDATE table SET col=val WHERE id=1” cannot be represented. |

### **parser/parser.go – DROP**

| Issue | Severity | Details |
|-------|----------|---------|
| **DROP and README mismatch** | Medium | Parser expects `DROP <table>` (one token after DROP = table name). README shows `DROP students`. Standard SQL is `DROP TABLE students`. So `DROP TABLE students` would set `table = "TABLE"` (wrong). Either document “DROP &lt;table&gt;” only or add TABLE keyword and parse both. |
| **DROP not executed** | High | Code generator does not handle `*parser.DropStmt`; it falls through to `default` and prints “Unknown statement”. No `OP_DROP` in VM. So DROP is parsed but never executed. |

### **code-generator/code_generator.go**

| Issue | Severity | Details |
|-------|----------|---------|
| **UPDATE/DROP not emitted** | High | `switch stmt` has no `case *parser.UpdateStmt:` or `case *parser.DropStmt:`. Both fall through to `default: fmt.Println("Unknown statement")` and only `OP_END` is emitted. |
| **Panic on schema marshal** | Medium | `json.Marshal(payload)` for CREATE TABLE panics on failure (line 76). Should return error to caller. |
| **Debug prints** | Low | Multiple `fmt.Println` / `fmt.Printf` for CREATE DATABASE, USE, INSERT, SELECT, “Unknown statement”. |

---

## 4. **bplustree/**

### **General (see bplustree/README.md)**

| Issue | Severity | Details |
|-------|----------|---------|
| **Pin/Unpin never used** | Medium | Buffer pool has `Pin`/`Unpin`, but **no** tree code (insertion, deletion, find_leaf, split, iterator) calls them. During a long traversal or split, nodes can be evicted while in use. Recommended: Pin nodes while traversing, Unpin when done. |
| **Direct cache access** | Clarification | README says “direct cache access” bypasses LRU. Grep shows tree code uses `t.cache.Get`, `t.cache.Put`, `t.cache.MarkDirty` (not `cache.pages[id]`). So the main remaining issue is missing Pin/Unpin, not direct `.pages` access in tree logic. |

### **disk_pager.go**

| Issue | Severity | Details |
|-------|----------|---------|
| **DeallocatePage is no-op** | Low | `DeallocatePage` does nothing; deleted pages are never reused. Document as intentional or add free-list later. |

### **new_bplus_tree.go**

| Issue | Severity | Details |
|-------|----------|---------|
| **Root persistence** | None | Root is read from page 0 in `NewBPlusTree` and written in `saveRoot()`. The TODO in `query_executor/helpers.go` about “root persistence” is outdated. |

---

## 5. **heapfile_manager/**

### **heapfile_manager.go**

| Issue | Severity | Details |
|-------|----------|---------|
| **CloseAll logs to stdout** | Low | `fmt.Printf("Error closing heap file %d: %v\n", ...)` – consider returning/collecting errors instead of only printing. |

### **helpers.go**

| Issue | Severity | Details |
|-------|----------|---------|
| **GetAllRowPointers continues on error** | Medium | On `pager.ReadPage` error it does `fmt.Printf` and `continue`, so partial results are returned without error. Caller cannot tell that some pages were unreadable. |

### **Heap file operations (README)**  

| Issue | Severity | Details |
|-------|----------|---------|
| **Delete/Update** | Known | DeleteRow exists (tombstone). No in-executor UPDATE of row in place; README marks “Delete/Update TODO”. |

---

## 6. **wal_manager/wal.go**

| Issue | Severity | Details |
|-------|----------|---------|
| **Recovery log to stdout** | Low | `fmt.Printf("Recovered Successful: %+v\n", w)` (line 116). Consider optional or structured logging. |

---

## 7. **query_executor/wal_replay.go**

| Issue | Severity | Details |
|-------|----------|---------|
| **Verbose recovery logs** | Low | Multiple `fmt.Println` / `fmt.Printf` during replay. Fine for debugging; consider log level or flag. |

---

## 8. **README.md**

| Issue | Severity | Details |
|-------|----------|---------|
| **Image path case** | Low | `<img src="./Sample_Image/Necessary_Image.png" />` but directory is `sample_image/` (lowercase). Fails on case-sensitive filesystems (e.g. typical Linux). Use `./sample_image/Necessary_Image.png`. |

---

## 9. **types/operations.go**

| Issue | Severity | Details |
|-------|----------|---------|
| **OpUpdate / OpDelete unused** | Low | `OpUpdate`, `OpDelete` exist but WAL/executor only use insert/create-table/txn ops. No problem until you implement UPDATE/DELETE and WAL for them. |

---

## 10. **Summary Table by Area**

| Area | Critical | High | Medium | Low |
|------|----------|------|--------|-----|
| **query_executor** | Index open leak (GetOrCreateIndex) | UPDATE/DELETE missing; no OP_UPDATE/OP_DELETE | - | Hardcoded DB, debug prints, outdated TODO |
| **query_parser** | - | Parser panics; UPDATE no WHERE; DROP not executed | DROP vs DROP TABLE | Debug prints |
| **code_generator** | - | UPDATE/DROP not emitted | Panic on marshal | Debug prints |
| **bplustree** | - | - | Pin/Unpin not used | DeallocatePage no-op |
| **heapfile_manager** | - | - | GetAllRowPointers swallows errors | CloseAll prints only |
| **main.go** | - | - | Hardcoded WAL path | Unused tree, REPL dump |
| **README** | - | - | - | Image path case |

---

## 11. **Recommended Fix Order**

1. **query_executor/helpers.go**: Cache per-table B+ trees and close them on DB switch or shutdown; fix index handle leak.
2. **query_parser/parser.go**: Return errors instead of panicking; add proper error type or `(Statement, error)`.
3. **query_parser**: Add WHERE to UPDATE (e.g. `WhereCol`, `WhereVal` in `UpdateStmt`); parse WHERE in `parseUpdate()`.
4. **query_executor**: Add `OP_UPDATE`, `OP_DELETE`, `OP_DROP`; implement `ExecuteUpdate`, `ExecuteDelete`, `ExecuteDrop`; wire codegen for UPDATE/DROP (and DELETE if you add it to parser).
5. **code_generator**: Handle `*parser.UpdateStmt` and `*parser.DropStmt`; return errors instead of panic on JSON marshal.
6. **bplustree**: Use Pin/Unpin in tree operations (e.g. FindLeaf, Insertion, Deletion, iterator) to avoid eviction mid-operation.
7. **Cleanup**: Remove or gate debug prints; fix README image path; remove outdated root-persistence TODO in helpers.

After these, the codebase will be in a consistent state for adding new components (e.g. secondary indexes, non-PK predicates, compaction).
