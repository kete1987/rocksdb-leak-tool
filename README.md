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

## Test Results

All runs: 60 iterations, `--no-gc`, `--cache-mb 8`, 10,000 key-value pairs per iteration
(~9 MB data, enough to fully populate an 8 MB block cache).
Executed on Windows 11 (WorkingSet64 metric via PowerShell). Java 11.0.28 (Microsoft build).

### Leak mode

```
OS: Windows | Mode: leak | Iterations: 60 | Cache: 8 MB | GC forced: false
Iter    WorkingSet (kB)   Delta (kB)
----------------------------------------
1       279716            +0
2       290096            +10380
3       351364            +61268        (JVM warmup)
4       412212            +60848        (JVM warmup)
5       473944            +61732        (JVM warmup)
6       512584            +38640        (JVM warmup)
7       521804            +9220
8       413856            -107948       (OS page reclaim after warmup spike)
9       433144            +19288
10      441028            +7884         <-- ~8 MB/iter from here: one LRUCache leaked per cycle
11      449552            +8524
12      458476            +8924
13      466584            +8108
20      526976            +8308
30      612436            +8496
40      698708            +8112
50      785688            +8580
60      872120            +8640

Memory at start : 279716 kB
Memory at end   : 872120 kB
Total growth    : +592404 kB  (~+578 MB)
```

### Fixed mode

```
OS: Windows | Mode: fixed | Iterations: 60 | Cache: 8 MB | GC forced: false
Iter    WorkingSet (kB)   Delta (kB)
----------------------------------------
1       214864            +0
2       245356            +30492        (JVM warmup)
3       290568            +45212        (JVM warmup)
4       351376            +60808        (JVM warmup)
5       402980            +51604        (JVM warmup)
6       434392            +31412        (JVM warmup)
7       435048            +656          <-- flat from here
8       435368            +320
10      351696            -85628        (OS page reclaim)
11      345684            -6012
15      346068            +56
20      344552            -1552
30      345548            -220
40      354800            +8744         (OS scheduling noise)
41      345520            -9280         (immediately reclaimed)
50      346424            -124
60      346444            +952

Memory at start : 214864 kB
Memory at end   : 346444 kB
Total growth    : +131580 kB  (~+128 MB, all JVM warmup -- flat after iter 7)
```

### Summary

| Metric | Leak | Fixed | Improvement |
|--------|------|-------|-------------|
| Total memory growth | **+578 MB** | **+128 MB** | **-450 MB** |
| Growth per iter (post-warmup) | **~8.5 MB constant** | **~0 KB (OS noise)** | **~8.5 MB saved/iter** |
| Pattern | Linear growth | Flat | |

The **~8.5 MB/iter** matches `--cache-mb 8` (Spark default `blockCacheSizeMB`), confirming
`LRUCache` as the sole leak source. In fixed mode the occasional +8 MB spikes are immediately
followed by -8 MB reclaims, showing the OS recovering the freed native memory.

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
