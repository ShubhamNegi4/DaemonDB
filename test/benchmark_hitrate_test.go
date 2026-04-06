package main

import (
	storageengine "DaemonDB/storage_engine"
	"DaemonDB/storage_engine/bufferpool"
	"DaemonDB/types"
	"fmt"
	"math/rand"
	"os"
	"testing"
)

// Buffer pool eviction policy benchmark suite: LRU-K (K=2) vs W-TinyLFU.
//
// Hardware  : AMD Ryzen 5 5625U, 12 threads
// Page size : 4 KB (DaemonDB default)
// Pool      : 64 pages × 4 KB = 256 KB  (memory-constrained environment)
//
// Row layout:
//   id  INT PRIMARY KEY   ~4  bytes
//   val VARCHAR           ~45 bytes  (padded to inflate row size)
//   Total                 ~55 bytes/row → ~65 rows per 4 KB heap page
//
// Each PK lookup traverses 3-4 B+ tree index pages before reaching the
// heap page, so effective buffer pool pressure is higher than heap pages alone.
//
// Results (observed):
//
//   Benchmark               lruk                    tinylfu
//   ─────────────────────────────────────────────────────────────────────────
//   ZipfianHotCold          63.98 %hit  13445 ns/op   51.51 %hit  19662 ns/op
//   ScanPollution           97.33 %hit  12178 µs/op   97.43 %hit  14641 µs/op
//   PostScanRecovery        39.07 %hit  11079 µs/op   64.40 %hit   9444 µs/op
//
// Key findings:
//   1. ZipfianHotCold  : LRU-K wins on hit rate (64% vs 52%) and speed.
//      LRU-K's recency signal fits skewed OLTP better than TinyLFU's
//      frequency sketch at this small pool size and iteration count.
//
//   2. ScanPollution   : Both policies are nearly identical (~97%).
//      With 20 warm-up passes the hot set frequency is high enough that
//      TinyLFU protects it as well as LRU-K does.
//
//   3. PostScanRecovery: TinyLFU wins decisively (64% vs 39%).
//      Cumulative frequency counts survive the scan — hot pages retain
//      high frequency scores even after being evicted, so they are
//      re-admitted faster on the next access. LRU-K has no memory of
//      evicted pages so it treats re-fetched hot pages as cold.
//
// Run:
//
//	DAEMONDB_BUFFERPOOL_POLICY=lruk    DAEMONDB_BUFFERPOOL_CAPACITY=64 \
//	  go test -bench=. -benchtime=3s -v . 2>&1 | grep "^Benchmark" > lruk.txt
//
//	DAEMONDB_BUFFERPOOL_POLICY=tinylfu DAEMONDB_BUFFERPOOL_CAPACITY=64 \
//	  go test -bench=. -benchtime=3s -v . 2>&1 | grep "^Benchmark" > tinylfu.txt
//
//	benchstat lruk.txt tinylfu.txt

const benchPoolSize = "64"

func newEngine(b *testing.B) (*storageengine.StorageEngine, func()) {
	b.Helper()
	bufferpool.SilenceLogs = true
	b.Setenv("DAEMONDB_BUFFERPOOL_CAPACITY", benchPoolSize)

	root := fmt.Sprintf("./_bench_%s", b.Name())
	_ = os.RemoveAll(root)

	engine, err := storageengine.NewStorageEngine(root)
	if err != nil {
		b.Fatalf("NewStorageEngine: %v", err)
	}
	if err := engine.CreateDatabase("db"); err != nil {
		b.Fatalf("CreateDatabase: %v", err)
	}
	if err := engine.UseDatabase("db"); err != nil {
		b.Fatalf("UseDatabase: %v", err)
	}
	if err := engine.CreateTable(types.TableSchema{
		TableName: "t",
		Columns: []types.ColumnDef{
			{Name: "id", Type: "INT", IsPrimaryKey: true},
			{Name: "val", Type: "VARCHAR"},
		},
	}); err != nil {
		b.Fatalf("CreateTable: %v", err)
	}

	return engine, func() {
		if engine.BufferPool != nil {
			engine.BufferPool.FlushAllPages()
		}
		if engine.DiskManager != nil {
			engine.DiskManager.CloseAll()
		}
		_ = os.RemoveAll(root)
	}
}

func insertRows(b *testing.B, engine *storageengine.StorageEngine, n int) {
	b.Helper()
	const batchSize = 500
	for start := 0; start < n; start += batchSize {
		txn, err := engine.BeginTransaction()
		if err != nil {
			b.Fatalf("BeginTransaction: %v", err)
		}
		end := start + batchSize
		if end > n {
			end = n
		}
		for i := start; i < end; i++ {
			if err := engine.InsertRow(txn, "t", []any{
				i,
				fmt.Sprintf("val_%06d_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", i),
			}); err != nil {
				b.Fatalf("InsertRow %d: %v", i, err)
			}
		}
		if err := engine.CommitTransaction(txn.ID); err != nil {
			b.Fatalf("CommitTransaction: %v", err)
		}
	}
}

func pkLookup(engine *storageengine.StorageEngine, id int) {
	_, _, _ = engine.ExecuteSelect(types.SelectPayload{
		Table:    "t",
		Columns:  []string{"*"},
		WhereCol: "id",
		WhereVal: fmt.Sprintf("%d", id),
	})
}

func fullScan(engine *storageengine.StorageEngine) {
	_, _, _ = engine.ExecuteSelect(types.SelectPayload{
		Table:   "t",
		Columns: []string{"*"},
	})
}

func reportHitRate(b *testing.B, engine *storageengine.StorageEngine) {
	b.Helper()
	stats := engine.BufferPool.GetStats()
	total := stats.Hits + stats.Misses
	if total == 0 {
		b.Log("WARNING: no buffer pool activity recorded")
		return
	}
	b.ReportMetric(float64(stats.Hits)/float64(total)*100, "%hit")
	b.ReportMetric(float64(stats.Misses), "misses")
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark 1: Zipfian Hot/Cold
//
// Simulates skewed OLTP access (80/20 rule).
// 10% of rows (hot=1000 of 10000) receive 80% of queries.
//
// Dataset : 10,000 rows → ~154 heap pages on disk
// Pool    : 64 pages    → holds 42% of working set
// Hot set : 1,000 rows  → ~16 pages (fits in pool if policy protects them)
//
// Observed:
//
//	lruk   : 63.98 %hit — recency signal keeps hot pages resident
//	tinylfu: 51.51 %hit — frequency sketch needs more iterations to converge
//	           at this pool-to-data ratio
//
// LRU-K wins here because the hot set is recently accessed on every
// iteration — recency is a perfect signal for this workload.
// TinyLFU's frequency counters need a longer warmup to fully converge.
// ─────────────────────────────────────────────────────────────────────────────
func BenchmarkZipfianHotCold(b *testing.B) {
	const total = 10000
	const hot = 1000

	engine, cleanup := newEngine(b)
	defer cleanup()
	insertRows(b, engine, total)
	engine.BufferPool.ResetStats()

	rng := rand.New(rand.NewSource(42))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if rng.Float64() < 0.8 {
			pkLookup(engine, rng.Intn(hot))
		} else {
			pkLookup(engine, hot+rng.Intn(total-hot))
		}
	}

	reportHitRate(b, engine)
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark 2: Scan Pollution Resistance
//
// A full table scan (analytics query) runs repeatedly against a pre-warmed
// OLTP hot set. Each scan touches every heap page once, attempting to evict
// the resident hot pages.
//
// Dataset : 20,000 rows → ~308 heap pages on disk
// Pool    : 64 pages    → holds 21% of data → scan forces real eviction
// Hot set : 500 rows    → ~8 pages, warmed 20× before measurement starts
//
// LRU-K  : scan pages have only 1 access → backward-K-distance = ∞
//
//	→ always evicted before hot pages → hot set survives.
//
// TinyLFU: hot pages have freq≈20 vs scan pages freq=1 → admission filter
//
//	rejects scan pages before they displace hot pages.
//
// Observed: both nearly identical — 97.33% vs 97.43%.
// At 20 warm-up reps the hot set frequency is high enough that both
// policies protect it equally. The difference would widen with fewer
// warm-up reps or a larger scan-to-pool ratio.
// ─────────────────────────────────────────────────────────────────────────────
func BenchmarkScanPollution(b *testing.B) {
	const total = 20000
	const hotRows = 500 // ~8 pages warmed before measurement

	engine, cleanup := newEngine(b)
	defer cleanup()
	insertRows(b, engine, total)

	for rep := 0; rep < 20; rep++ {
		for i := 0; i < hotRows; i++ {
			pkLookup(engine, i)
		}
	}
	engine.BufferPool.ResetStats()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		fullScan(engine)
	}

	reportHitRate(b, engine)
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmark 3: Post-Scan Recovery
//
// Measures how quickly a policy recovers its hot-page hit rate immediately
// after a full scan has flooded and replaced most of the pool.
//
// Each iteration (outside timer): full scan evicts nearly all pool contents.
// Each iteration (timed)        : access hot set — measure what survived.
//
// Dataset : 20,000 rows → ~308 heap pages on disk
// Pool    : 64 pages    → holds 21% of data
// Scan    : 308 pages   → 4.8× pool size → forces near-complete replacement
// Hot set : 500 rows    → ~8 pages, warmed 5× before the timed loop
//
// LRU-K  : 39.07 %hit — once evicted, hot pages have no K-distance history.
//
//	They re-enter as cold pages and must be accessed K times again
//	before competing as hot. Recovery is gradual.
//
// TinyLFU: 64.40 %hit — frequency counts persist in the Count-Min Sketch
//
//	even after eviction. When hot pages are re-fetched they are
//	immediately recognised as high-frequency and re-admitted over
//	lower-frequency scan pages. Recovery is immediate.
//
// This is the benchmark where TinyLFU's persistent frequency memory
// gives it a decisive advantage over recency-only policies.
// ─────────────────────────────────────────────────────────────────────────────
func BenchmarkPostScanRecovery(b *testing.B) {
	const total = 20000
	const hot = 500 // ~8 pages

	engine, cleanup := newEngine(b)
	defer cleanup()
	insertRows(b, engine, total)

	for rep := 0; rep < 5; rep++ {
		for i := 0; i < hot; i++ {
			pkLookup(engine, i)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		fullScan(engine)
		engine.BufferPool.ResetStats()
		b.StartTimer()

		for j := 0; j < hot; j++ {
			pkLookup(engine, j)
		}
	}

	reportHitRate(b, engine)
}

func BenchmarkWorkingSetScaling(b *testing.B) {
	scales := []struct {
		rows  int
		label string
	}{
		{500, "rows=500"},
		{1000, "rows=1k"},
		{2000, "rows=2k"},
		{5000, "rows=5k"},
		{10000, "rows=10k"},
		{20000, "rows=20k"},
	}

	for _, sc := range scales {
		sc := sc
		b.Run(sc.label, func(b *testing.B) {
			engine, cleanup := newEngine(b)
			defer cleanup()
			insertRows(b, engine, sc.rows)
			engine.BufferPool.ResetStats()

			hot := sc.rows / 10 // 10% hot set
			if hot < 1 {
				hot = 1
			}
			rng := rand.New(rand.NewSource(42))
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				if rng.Float64() < 0.8 {
					pkLookup(engine, rng.Intn(hot))
				} else {
					pkLookup(engine, hot+rng.Intn(sc.rows-hot))
				}
			}

			reportHitRate(b, engine)
		})
	}
}
