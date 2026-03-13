## Buffer Pool Benchmarks: LRU vs GDSF

This file captures the key metrics you can cite in your research paper. All times are measured, and all rates are computed using double-precision floating point.

---

### 1. Synthetic Page-Only Workload (LRU, before GDSF switch)

Workload parameters:

- Buffer pool capacity: 64 pages  
- Distinct pages accessed: 256  
- Accesses: 1500 (1000 random + 500 hot-set)

Results:

| Policy | Capacity | Distinct Pages | Total Accesses | Hits | Misses | Hit Rate (%) | Avg Latency (µs) |
|--------|----------|----------------|----------------|------|--------|--------------|------------------|
| LRU    | 64       | 256            | 1500           | 773  | 727    | 51.53        | 6.279            |

Hit rate was computed as `773 / 1500 ≈ 0.5153` → **51.53 %**. Average latency was **6.279 µs** per access.

---

### 2. Join Workload (LRU, before GDSF switch)

Workload:

- Two tables `t1`, `t2`, each with **3000** rows.
- Small buffer pool (capacity 64 pages) to force constant eviction.
- Wide rows so the heap spans many 4KB pages.
- Query:

```sql
SELECT * FROM t1 INNER JOIN t2 ON id = id;
```



Cold run: first execution with an empty cache.
Warm runs: repeated executions with a pre-populated cache (Warm1 = second run, Warm2 = third run).


Metrics (LRU, previous implementation):

| Run  | Rows/Table | Latency (ms) | Hits | Misses | HitRate |
|------|------------|--------------|------|--------|---------|
| Cold | 3000       | 71.324000    | 3000 | 6000   | 0.333333 |
| Warm | 3000       | 72.012000    | 3000 | 6000   | 0.333333 |
| Warm2| 3000       | 64.173000    | 3000 | 6000   | 0.333333 |

The working set is much larger than the buffer pool, so even warm runs remain miss-heavy.

---

### 3. Join Workload (GDSF, current implementation)

Workload:

- Same schema pattern (`t1`, `t2`), buffer pool capacity forced to **64**.
- `pad VARCHAR` column (~2000 bytes) to create many pages.
- **1500** rows per table (chosen to keep test runtime reasonable under GDSF).
- Query:

```sql
SELECT * FROM t1 INNER JOIN t2 ON id = id;
```

Metrics (GDSF, current implementation):

| Run  | Rows/Table | Latency (ms) | Hits | Misses | HitRate |
|------|------------|--------------|------|--------|---------|
| Cold | 1500       | 40.977000    | 1504 | 2996   | 0.334222 |
| Warm1| 1500       | 29.433000    | 1514 | 2986   | 0.336444 |
| Warm2| 1500       | 26.552000    | 1519 | 2981   | 0.337556 |

HitRate values were computed as:

- Cold: `1504 / (1504 + 2996) ≈ 0.334222`  
- Warm1: `1514 / (1514 + 2986) ≈ 0.336444`  
- Warm2: `1519 / (1519 + 2981) ≈ 0.337556`

These rows show how, under a heavily eviction-bound join workload, GDSF slightly increases the fraction of hits across repeated runs, while query latency drops from ~41 ms (cold) to ~27 ms (third warm run) on this test setup.

