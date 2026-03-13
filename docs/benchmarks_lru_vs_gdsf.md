## Buffer Pool Benchmarks: LRU vs GDSF

This file captures the key metrics you can cite in your research paper. All times are measured, and all rates are computed using double-precision floating point.

**Important metric definitions:**

- **Rows / table**: number of logical tuples in `t1` / `t2`.
- **Hits / Misses**: number of **buffer pool page accesses** during the entire execution of the query:
  - Every call to `BufferPool.FetchPage(pageID)` counts as one access.
  - If the page is already resident in memory → **HIT**.
  - If it must be loaded from disk → **MISS**.
  - Because pages are re-used and multiple pages are touched per row, in general  
    `Hits + Misses  ≫  Rows / table`.
- **HitRate**: `Hits / (Hits + Misses)` for that run.
- **Cold**: first execution of the query after starting the engine and opening the database (cache initially empty).
- **Warm1 / Warm2**: second and third executions of the same query in the same process, reusing the buffer pool state from previous runs.

---

### 1. Synthetic Page-Only Workload (LRU vs GDSF)

Workload parameters:

- Buffer pool capacity: 64 pages  
- Distinct pages accessed: 256  
- Accesses: 1500 (1000 random + 500 hot-set)

Results (same workload, two policies):

| Policy | Capacity | Distinct Pages | Total Accesses | Hits | Misses | Hit Rate (%) | Avg Latency (µs) |
|--------|----------|----------------|----------------|------|--------|--------------|------------------|
| LRU    | 64       | 256            | 1500           | 773  | 727    | 51.53        | 6.279            |
| GDSF   | 64       | 256            | 1500           | 716  | 784    | 47.73        | 4.224            |

For LRU, the hit rate was computed as `773 / 1500 ≈ 0.5153` → **51.53 %**, with an average latency of **6.279 µs** per access.  
For GDSF, the hit rate was `716 / 1500 ≈ 0.4773` → **47.73 %**, with an average latency of **4.224 µs** per access.

#### GDSF sensitivity to workload size

Additional GDSF-only synthetic workloads (same pattern, different sizes) show how hit rate and latency evolve as the working set grows relative to the cache:

| Size   | Capacity | Distinct Pages | Total Accesses | Hits | Misses | Hit Rate (%) | Avg Latency (µs) |
|--------|----------|----------------|----------------|------|--------|--------------|------------------|
| Small  | 32       | 64             | 450 (300+150)  | 214  | 236    | 47.56        | 3.857            |
| Medium | 64       | 256            | 1500           | 716  | 784    | 47.73        | 4.224            |
| Large  | 64       | 1024           | 7000           | 1308 | 5692   | 18.69        | 4.191            |

These values were measured by running the same random+hot access pattern with the parameters shown in the table. As the working set grows far beyond the buffer capacity (the “Large” case), the hit rate necessarily drops even under GDSF, while average per-access latency remains in a similar microsecond range on this machine.

---

### 2. Join Workload (LRU — qualitative baseline only)

Originally, the join benchmark was run under LRU with **3000 rows/table**. That configuration showed:

- A small buffer pool (64 pages) relative to the number of heap pages touched by the join.
- Eviction-bound behaviour where the hit rate stayed close to **1/3** even on warm runs.
- Latency in the **60–70 ms** range for the full join on the original machine.

After switching the implementation to GDSF, those exact 3000-row LRU numbers were not re-run at `n = 1500`. To avoid mixing measured values and hand-scaled approximations, this section is left **qualitative only**. For precise numeric comparisons, use:

- Section 1 (synthetic LRU page-only workload), and
- Section 3 (measured GDSF join workload).

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

