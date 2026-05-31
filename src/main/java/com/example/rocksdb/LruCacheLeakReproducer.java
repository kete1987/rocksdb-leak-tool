package com.example.rocksdb;

import org.rocksdb.BloomFilter;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.FlushOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Statistics;
import org.rocksdb.WriteOptions;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

/**
 * Reproduces the native (C-heap) memory leak caused by LRUCache not being explicitly
 * closed in Spark's RocksDB state store wrapper (unbounded memory mode).
 *
 * Root cause: RocksDB.scala creates a new LRUCache per instance in unbounded mode
 * (the default), but RocksDB.close() never calls lruCache.close(). The Java wrapper
 * holds a C++ shared_ptr to the native cache, preventing deallocation until the JVM
 * finalizer runs. With a 4 GB heap and low GC pressure, dozens of unclosed LRUCache
 * objects accumulate during a long test run, exhausting native memory.
 *
 * Affected: apache/master and apache/branch-4.2 as of 2026-05-29
 *   sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/RocksDB.scala
 *
 * Related: SPARK-56523 (Statistics leak, already fixed). This is a separate, unfixed issue.
 *
 * USAGE:
 *   java -jar rocksdb-leak-reproducer-1.0-SNAPSHOT.jar [options]
 *   --mode &lt;leak|fixed&gt;   (default: leak)
 *   --iterations &lt;N&gt;      number of RocksDB open/close cycles (default: 80)
 *   --delay-ms &lt;ms&gt;       sleep between iterations (default: 200)
 *   --no-gc               suppress System.gc() calls between iterations
 *   --cache-mb &lt;MB&gt;       LRUCache size in MB (default: 8, matches Spark default blockCacheSizeMB)
 *
 * Runs on Linux (reads VmRSS from /proc/self/status) and Windows (reads Working Set
 * via PowerShell Get-Process). Both metrics capture native C-heap growth.
 */
public class LruCacheLeakReproducer {

    private static final boolean IS_WINDOWS =
            System.getProperty("os.name", "").toLowerCase().contains("win");

    public static void main(String[] args) throws Exception {
        String mode = "leak";
        int iterations = 80;
        long delayMs = 200;
        boolean forceGc = true;
        long cacheMb = 8;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--mode":       mode = args[++i]; break;
                case "--iterations": iterations = Integer.parseInt(args[++i]); break;
                case "--delay-ms":   delayMs = Long.parseLong(args[++i]); break;
                case "--no-gc":      forceGc = false; break;
                case "--cache-mb":   cacheMb = Long.parseLong(args[++i]); break;
                default: System.err.println("Unknown argument: " + args[i]); System.exit(1);
            }
        }

        RocksDB.loadLibrary();
        String memMetric = IS_WINDOWS ? "WorkingSet (kB)" : "VmRSS (kB)";
        System.out.printf("OS: %s | Mode: %s | Iterations: %d | Cache: %d MB | GC forced: %b%n",
                IS_WINDOWS ? "Windows" : "Linux", mode, iterations, cacheMb, forceGc);
        System.out.printf("%-6s  %-16s  %-14s%n", "Iter", memMetric, "Delta (kB)");
        System.out.println("-".repeat(40));

        long baselineKb = -1, prevKb = -1;

        for (int i = 1; i <= iterations; i++) {
            if (forceGc && i % 10 == 0) {
                System.gc();
                System.runFinalization();
                Thread.sleep(50);
            }

            Path dbDir = Files.createTempDirectory("rocksdb_lrucache_leak_");
            try {
                if (mode.equals("leak")) {
                    runLeakIteration(dbDir.toString(), cacheMb);
                } else {
                    runFixedIteration(dbDir.toString(), cacheMb);
                }
            } finally {
                deleteDirectory(dbDir);
            }

            if (delayMs > 0) Thread.sleep(delayMs);

            long memKb = readMemoryKb();
            if (baselineKb < 0) baselineKb = memKb;
            long delta = (prevKb < 0) ? 0 : (memKb - prevKb);
            prevKb = memKb;
            System.out.printf("%-6d  %-16d  %-+14d%n", i, memKb, delta);
        }

        long totalGrowth = prevKb - baselineKb;
        System.out.printf("%nMemory at start : %d kB%nMemory at end   : %d kB%nTotal growth    : %+d kB  (~%+.1f MB)%n",
                baselineKb, prevKb, totalGrowth, totalGrowth / 1024.0);
    }

    /**
     * Mirrors the current buggy Spark RocksDB.scala (unbounded mode): lruCache is never closed.
     * Each call leaks cacheMb MB of native C-heap memory.
     */
    private static void runLeakIteration(String dbPath, long cacheMb) throws RocksDBException {
        long cacheSizeBytes = cacheMb * 1024 * 1024;
        BloomFilter bloomFilter = new BloomFilter();
        BlockBasedTableConfig tableFormatConfig = new BlockBasedTableConfig();
        LRUCache lruCache = new LRUCache(cacheSizeBytes);
        tableFormatConfig.setBlockCache(lruCache);
        tableFormatConfig.setFilterPolicy(bloomFilter);
        Statistics nativeStats = new Statistics();
        Options options = new Options()
                .setCreateIfMissing(true)
                .setTableFormatConfig(tableFormatConfig)
                .setStatistics(nativeStats);
        ReadOptions ro = new ReadOptions();
        WriteOptions wo = new WriteOptions();
        FlushOptions fo = new FlushOptions().setWaitForFlush(true);
        RocksDB db = null;
        try {
            db = RocksDB.open(options, dbPath);
            doWork(db, ro, wo, fo);
        } finally {
            if (db != null) db.close();
        }
        ro.close();
        wo.close();
        fo.close();
        nativeStats.close();
        options.close();
        // lruCache.close() -- intentionally omitted: this is the bug being reproduced
    }

    /**
     * Fixed version: explicit lruCache.close() added, mirroring the proposed fix for
     * RocksDB.scala: if (!conf.boundedMemoryUsage && lruCache != null) { lruCache.close() }
     */
    private static void runFixedIteration(String dbPath, long cacheMb) throws RocksDBException {
        long cacheSizeBytes = cacheMb * 1024 * 1024;
        BloomFilter bloomFilter = new BloomFilter();
        BlockBasedTableConfig tableFormatConfig = new BlockBasedTableConfig();
        LRUCache lruCache = new LRUCache(cacheSizeBytes);
        tableFormatConfig.setBlockCache(lruCache);
        tableFormatConfig.setFilterPolicy(bloomFilter);
        Statistics nativeStats = new Statistics();
        Options options = new Options()
                .setCreateIfMissing(true)
                .setTableFormatConfig(tableFormatConfig)
                .setStatistics(nativeStats);
        ReadOptions ro = new ReadOptions();
        WriteOptions wo = new WriteOptions();
        FlushOptions fo = new FlushOptions().setWaitForFlush(true);
        RocksDB db = null;
        try {
            db = RocksDB.open(options, dbPath);
            doWork(db, ro, wo, fo);
        } finally {
            if (db != null) db.close();
        }
        ro.close();
        wo.close();
        fo.close();
        nativeStats.close();
        options.close();
        lruCache.close(); // THE FIX
    }

    /**
     * Writes and reads enough data to fully populate an 8 MB block cache.
     * 10,000 entries x ~900 B value = ~9 MB, ensuring the LRUCache is actually used.
     */
    private static void doWork(RocksDB db, ReadOptions ro, WriteOptions wo, FlushOptions fo)
            throws RocksDBException {
        for (int i = 0; i < 10000; i++) {
            byte[] key = ("key-" + String.format("%08d", i)).getBytes();
            byte[] value = ("v" + i + "-" + "x".repeat(880)).getBytes();
            db.put(wo, key, value);
        }
        db.flush(fo);
        for (int i = 0; i < 10000; i++) {
            db.get(ro, ("key-" + String.format("%08d", i)).getBytes());
        }
    }

    /**
     * Returns process memory in kB. Uses VmRSS on Linux, Working Set on Windows.
     * Both metrics capture native C-heap growth and are directly comparable within a run.
     */
    private static long readMemoryKb() {
        return IS_WINDOWS ? readWindowsWorkingSetKb() : readLinuxVmRssKb();
    }

    private static long readLinuxVmRssKb() {
        try {
            for (String line : Files.readAllLines(Paths.get("/proc/self/status"))) {
                if (line.startsWith("VmRSS:")) {
                    return Long.parseLong(line.trim().split("\\s+")[1]);
                }
            }
        } catch (IOException ignored) {}
        return -1;
    }

    private static long readWindowsWorkingSetKb() {
        try {
            long pid = ProcessHandle.current().pid();
            // WorkingSet64 is the physical memory currently used by the process (bytes)
            String psCommand = String.format(
                    "(Get-Process -Id %d).WorkingSet64", pid);
            Process process = new ProcessBuilder(
                    "powershell", "-NonInteractive", "-Command", psCommand)
                    .redirectErrorStream(true)
                    .start();
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream()))) {
                String line = reader.readLine();
                if (line != null && !line.isBlank()) {
                    return Long.parseLong(line.trim()) / 1024;
                }
            }
            process.waitFor();
        } catch (Exception ignored) {}
        return -1;
    }

    private static void deleteDirectory(Path dir) {
        try {
            Files.walk(dir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try { Files.delete(p); } catch (IOException ignored) {}
                    });
        } catch (IOException ignored) {}
    }
}
