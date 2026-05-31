# RocksDB LRUCache Native Memory Leak — Reproducer

Standalone tool to reproduce and verify the fix for a native (C-heap) memory leak in
Apache Spark's RocksDB state store wrapper.

**Status:** Not fixed in `apache/master` or `apache/branch-4.2` as of 2026-05-29.
**Related:** [SPARK-56523](https://issues.apache.org/jira/browse/SPARK-56523) (Statistics leak — already fixed). This is a **separate, unfixed** issue.

---

## The Bug

In **unbounded memory mode** (default: `boundedMemoryUsage = false`), Spark's `RocksDB`
state store wrapper creates a new `LRUCache` per instance but never calls
`lruCache.close()` in `RocksDB.close()`. The Java `LRUCache` wrapper holds a C++
`shared_ptr<Cache>`, preventing native memory from being freed until the JVM GC
finalizes the wrapper. Under low heap pressure (e.g., a 4 GB JVM), GC rarely runs,
so unclosed caches accumulate. Over a CI run with ~49 RocksDB-heavy test suites, this
accumulates **4–8 GB of native memory** and causes silent OOM kills.

**Affected file:**
`sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/RocksDB.scala`

```scala
// CURRENT CODE -- lruCache is never closed
def close(): Unit = {
  closeDB()
  readOptions.close()
  writeOptions.close()
  flushOptions.close()
  nativeStats.close()    // added by SPARK-56523 fix
  rocksDbOptions.close()
  dbLogger.close()
  // lruCache.close()  <-- MISSING
}
```

**The fix** — add after `dbLogger.close()`:

```scala
// In unbounded memory mode, each RocksDB instance owns its own LRUCache.
// Without explicit close(), the native C++ cache object is only freed when
// the JVM GC finalizes the Java wrapper -- which rarely happens under low
// heap pressure. Over hundreds of test suite iterations, unclosed caches
// accumulate gigabytes of native memory and cause OOM.
// In bounded mode the cache is a shared singleton managed by
// RocksDBMemoryManager and must not be closed here.
if (!conf.boundedMemoryUsage && lruCache != null) {
  lruCache.close()
}
```

---

## Requirements

- Java 11+
- Maven 3.6+
- Works on **Linux** and **Windows** (no Docker or WSL required)

---

## Build

```bash
mvn clean package -q
```

---

## Run

```bash
# Reproduce the leak (--no-gc simulates low GC pressure as in CI)
java -jar target/rocksdb-leak-reproducer-1.0-SNAPSHOT.jar \
  --mode leak --iterations 60 --delay-ms 100 --no-gc --cache-mb 8

# Verify the fix
java -jar target/rocksdb-leak-reproducer-1.0-SNAPSHOT.jar \
  --mode fixed --iterations 60 --delay-ms 100 --no-gc --cache-mb 8
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--mode <leak\|fixed>` | `leak` | `leak` omits `lruCache.close()`; `fixed` adds it |
| `--iterations <N>` | `80` | Number of RocksDB open/close cycles |
| `--delay-ms <ms>` | `200` | Sleep between iterations |
| `--no-gc` | (GC enabled) | Suppresses `System.gc()` calls — simulates CI low-GC conditions |
| `--cache-mb <MB>` | `8` | LRUCache size in MB (matches Spark default `blockCacheSizeMB`) |

---

## Expected Results

All runs: 60 iterations, `--no-gc`, `--cache-mb 8`, 10,000 key-value pairs per iteration
(~9 MB data, enough to fully populate an 8 MB block cache).

### Leak mode

```
Iter    VmRSS (kB)    Delta (kB)
1       191576        +0
5       341572        +8232         <-- ~8 MB/iter = one LRUCache per cycle
10      382604        +8148
20      464704        +8204
30      525216        +8180
60      771480        +8144

Total growth : +566 MB  (~8 MB x 60 iters after warmup)
```

### Fixed mode

```
Iter    VmRSS (kB)    Delta (kB)
1       214160        +0
5       334060        -24           <-- flat after warmup
20      318488        -15964        (OS page reclaim)
60      311296        -36

Total growth : +95 MB  (all JVM warmup, flat after iter ~5)
```

### Summary

| Metric | Leak | Fixed | Improvement |
|--------|------|-------|-------------|
| Total memory growth | **+566 MB** | **+95 MB** | **-471 MB** |
| Growth per iter (post-warmup) | **~8 MB** | **~0 KB** | **~8 MB saved/iter** |
| Pattern | Linear | Flat | |

The **+8 MB/iter** matches exactly `--cache-mb 8` (Spark default `blockCacheSizeMB`),
confirming `LRUCache` as the sole leak source.

---

## Memory metric

- **Linux:** reads `VmRSS` from `/proc/self/status` — physical RSS including native C-heap.
- **Windows:** reads `WorkingSet64` via `Get-Process` — physical working set, equivalent metric.

Both capture native memory growth and are directly comparable within a single run.

---

## CI-scale impact estimate

Each streaming test creates 1–10 RocksDB instances. Each instance leaks one `LRUCache`
of `blockCacheSizeMB` MB (default: 8 MB). Across ~49 heavy test suites:

| Suite | ~RocksDB instances | Leak |
|-------|--------------------|------|
| TransformWithStateSuite | ~240 | ~1.9 GB |
| RocksDBSuite | ~111 | ~890 MB |
| StreamingJoinSuite | ~140 | ~1.1 GB |
| FlatMapGroupsWithStateSuite | ~54 | ~430 MB |
| StreamingAggregationSuite | ~150 | ~1.2 GB |
| ~44 more suites | ... | ... |
| **Total estimate** | **500–1000** | **4–8 GB** |

Combined with a 4 GB JVM heap + ~2 GB metaspace on a 20 Gi CI pod → OOM kill →
Maven reports "There are test failures" with no visible red tests in XML.

---

## Related

- [SPARK-56523](https://issues.apache.org/jira/browse/SPARK-56523) — Statistics native memory leak (already fixed)
- [planga82/rocksdb-statistics-leak](https://github.com/planga82/rocksdb-statistics-leak) — reproducer for SPARK-56523 that inspired this tool
