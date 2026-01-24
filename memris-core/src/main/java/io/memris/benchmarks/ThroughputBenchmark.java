package io.memris.benchmarks;

import io.memris.kernel.selection.SelectionVector;
import io.memris.kernel.selection.SelectionVectorFactory;
import io.memris.storage.ffm.FfmTable;

import java.lang.foreign.Arena;
import java.util.List;

/**
 * Throughput benchmark for Memris.
 * CRITICAL: Follows ARRANGE-ACT-MEASURE pattern with pre-cooked inputs.
 * - ARRANGE: All setup (table creation, data insertion, predicates) done before timing
 * - ACT: Only the measured operation occurs during timing
 * - MEASURE: Results collected and reported AFTER timing stops
 * - No I/O during measurement
 * - No object creation during measurement loops
 * - Results consumed to prevent dead code elimination
 */
public final class ThroughputBenchmark {
    public static void main(String[] args) {
        final int SIZE = 10_000_000;
        final int WARMUP = 3;
        final int ITERATIONS = 5;

        System.out.println("=== Memris Throughput Benchmark: " + SIZE + " rows ===");
        System.out.println("Column layout: id(int), customer_id(int), amount(long), status(String)");
        System.out.println("Row size: ~24 bytes, Total: " + (SIZE * 24 / 1024 / 1024) + " MB\n");

        try (Arena arena = Arena.ofConfined()) {
            // ========== ARRANGE: Setup all data structures BEFORE timing ==========
            final FfmTable orders = new FfmTable("orders", arena, List.of(
                    new FfmTable.ColumnSpec("id", int.class),
                    new FfmTable.ColumnSpec("customer_id", int.class),
                    new FfmTable.ColumnSpec("amount", long.class),
                    new FfmTable.ColumnSpec("status", String.class)
            ), SIZE);

            for (int i = 0; i < SIZE; i++) {
                orders.insert(i, i % 100_000, (i % 1000) * 100L,
                    i % 2 == 0 ? "pending" : "completed");
            }

            final SelectionVectorFactory factory = SelectionVectorFactory.defaultFactory();

            // Pre-cook predicates (create ONCE before measurement loop)
            final io.memris.kernel.Predicate pendingPredicate =
                new io.memris.kernel.Predicate.Comparison("status", io.memris.kernel.Predicate.Operator.EQ, "pending");
            final io.memris.kernel.Predicate rangePredicate =
                new io.memris.kernel.Predicate.Between("amount", 10000L, 20000L);

            // Warmup JVM
            for (int w = 0; w < WARMUP; w++) {
                orders.scanAll(factory);
                orders.scan(pendingPredicate, factory);
                orders.scan(rangePredicate, factory);
            }

            // ========== ACT + MEASURE: Benchmark full scan ==========
            System.out.println("--- Full Table Scan ---");
            long[] scanTimes = new long[ITERATIONS];
            long totalScan = 0;
            for (int i = 0; i < ITERATIONS; i++) {
                long start = System.nanoTime();
                SelectionVector all = orders.scanAll(factory);
                long elapsed = System.nanoTime() - start;
                scanTimes[i] = elapsed;
                totalScan += elapsed;
                // Consume result to prevent dead code elimination
                if (all.size() < 0) throw new AssertionError();
            }
            // Print AFTER measurement
            for (int i = 0; i < ITERATIONS; i++) {
                double throughput = (SIZE * 24.0) / (scanTimes[i] / 1_000_000.0) / 1024.0 / 1024.0;
                System.out.printf("  Run %d: %d ms (%.1f GB/s)%n", i + 1, scanTimes[i] / 1_000_000, throughput);
            }
            System.out.printf("  Average: %.1f GB/s%n%n", (SIZE * 24.0 * ITERATIONS) / (totalScan / 1_000_000.0) / 1024.0 / 1024.0);

            // ========== ACT + MEASURE: Benchmark point filter ==========
            System.out.println("--- Point Filter (status='pending', ~50% selectivity) ---");
            long[] filterTimes = new long[ITERATIONS];
            int[] filterSizes = new int[ITERATIONS];
            long totalFilter = 0;
            for (int i = 0; i < ITERATIONS; i++) {
                long start = System.nanoTime();
                SelectionVector filtered = orders.scan(pendingPredicate, factory);
                long elapsed = System.nanoTime() - start;
                filterTimes[i] = elapsed;
                filterSizes[i] = filtered.size();
                totalFilter += elapsed;
                // Consume result to prevent dead code elimination
                if (filtered.size() < 0) throw new AssertionError();
            }
            // Print AFTER measurement
            for (int i = 0; i < ITERATIONS; i++) {
                System.out.printf("  Run %d: %d ms (%d rows)%n", i + 1, filterTimes[i] / 1_000_000, filterSizes[i]);
            }
            System.out.printf("  Average: %.1f ms%n%n", totalFilter / (double) ITERATIONS / 1_000_000.0);

            // ========== ACT + MEASURE: Benchmark range query ==========
            System.out.println("--- Range Query (amount 10k-20k, ~10% selectivity) ---");
            long[] rangeTimes = new long[ITERATIONS];
            int[] rangeSizes = new int[ITERATIONS];
            long totalRange = 0;
            for (int i = 0; i < ITERATIONS; i++) {
                long start = System.nanoTime();
                SelectionVector range = orders.scan(rangePredicate, factory);
                long elapsed = System.nanoTime() - start;
                rangeTimes[i] = elapsed;
                rangeSizes[i] = range.size();
                totalRange += elapsed;
                // Consume result to prevent dead code elimination
                if (range.size() < 0) throw new AssertionError();
            }
            // Print AFTER measurement
            for (int i = 0; i < ITERATIONS; i++) {
                System.out.printf("  Run %d: %d ms (%d rows)%n", i + 1, rangeTimes[i] / 1_000_000, rangeSizes[i]);
            }
            System.out.printf("  Average: %.1f ms%n", totalRange / (double) ITERATIONS / 1_000_000.0);
        }
    }
}
