package io.memris.benchmarks;

import io.memris.MemrisStore;
import io.memris.storage.RowId;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class MemrisBenchmarks {

    private MemrisStore store;
    private RowId[] rowIds;

    @Setup
    public void setup() {
        store = new MemrisStore();
        rowIds = new RowId[100_000];

        // Pre-populate with 100k rows
        for (int i = 0; i < 100_000; i++) {
            rowIds[i] = store.insert(i, "user" + i, i % 100);
        }
    }

    @Benchmark
    public void insert_100k_rows(Blackhole blackhole) {
        MemrisStore localStore = new MemrisStore();
        for (int i = 0; i < 100_000; i++) {
            localStore.insert(i, "user" + i, i % 100);
        }
        blackhole.consume(localStore);
    }

    @Benchmark
    public void lookup_by_row_id(Blackhole blackhole) {
        for (int i = 0; i < 100_000; i++) {
            Object[] row = store.lookup(rowIds[i]);
            blackhole.consume(row);
        }
    }

    @Benchmark
    public void scan_all_rows(Blackhole blackhole) {
        long count = store.rowCount();
        for (long i = 0; i < count; i++) {
            RowId rowId = new RowId(i >>> 16, (int) (i & 65535));
            Object[] row = store.lookup(rowId);
            blackhole.consume(row);
        }
    }

    @Benchmark
    public void create_hash_index(Blackhole blackhole) {
        store.createIndex("status", 2);
        blackhole.consume(store.hasIndex("status"));
    }

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }
}
