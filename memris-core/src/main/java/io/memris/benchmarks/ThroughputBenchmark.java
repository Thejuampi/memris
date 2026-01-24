package io.memris.benchmarks;

import io.memris.kernel.selection.SelectionVector;
import io.memris.kernel.selection.SelectionVectorFactory;
import io.memris.storage.ffm.FfmTable;

import java.lang.foreign.Arena;
import java.util.List;

public final class ThroughputBenchmark {
    public static void main(String[] args) {
        final int SIZE = 10_000_000;
        final int WARMUP = 3;
        final int ITERATIONS = 5;
        
        System.out.println("=== Memris Throughput Benchmark: " + SIZE + " rows ===");
        System.out.println("Column layout: id(int), customer_id(int), amount(long), status(String)");
        System.out.println("Row size: ~24 bytes, Total: " + (SIZE * 24 / 1024 / 1024) + " MB\n");
        
        try (Arena arena = Arena.ofConfined()) {
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
            
            // Warmup
            for (int w = 0; w < WARMUP; w++) {
                orders.scanAll(factory);
                orders.scan(new io.memris.kernel.Predicate.Comparison("status", io.memris.kernel.Predicate.Operator.EQ, "pending"), factory);
            }
            
            // Benchmark full scan
            System.out.println("--- Full Table Scan ---");
            long totalScan = 0;
            for (int i = 0; i < ITERATIONS; i++) {
                long start = System.nanoTime();
                SelectionVector all = orders.scanAll(factory);
                long elapsed = System.nanoTime() - start;
                totalScan += elapsed;
                double throughput = (SIZE * 24.0) / (elapsed / 1_000_000.0) / 1024.0 / 1024.0;
                System.out.printf("  Run %d: %d ms (%.1f GB/s)%n", i + 1, elapsed / 1_000_000, throughput);
            }
            System.out.printf("  Average: %.1f GB/s%n%n", (SIZE * 24.0 * ITERATIONS) / (totalScan / 1_000_000.0) / 1024.0 / 1024.0);
            
            // Benchmark point filter
            System.out.println("--- Point Filter (status='pending', ~50% selectivity) ---");
            long totalFilter = 0;
            for (int i = 0; i < ITERATIONS; i++) {
                long start = System.nanoTime();
                SelectionVector filtered = orders.scan(
                        new io.memris.kernel.Predicate.Comparison("status", io.memris.kernel.Predicate.Operator.EQ, "pending"), factory);
                long elapsed = System.nanoTime() - start;
                totalFilter += elapsed;
                System.out.printf("  Run %d: %d ms (%d rows)%n", i + 1, elapsed / 1_000_000, filtered.size());
            }
            System.out.printf("  Average: %.1f ms%n%n", totalFilter / (double) ITERATIONS / 1_000_000.0);
            
            // Benchmark range query
            System.out.println("--- Range Query (amount 10k-20k, ~10% selectivity) ---");
            long totalRange = 0;
            for (int i = 0; i < ITERATIONS; i++) {
                long start = System.nanoTime();
                SelectionVector range = orders.scan(
                        new io.memris.kernel.Predicate.Between("amount", 10000L, 20000L), factory);
                long elapsed = System.nanoTime() - start;
                totalRange += elapsed;
                System.out.printf("  Run %d: %d ms (%d rows)%n", i + 1, elapsed / 1_000_000, range.size());
            }
            System.out.printf("  Average: %.1f ms%n", totalRange / (double) ITERATIONS / 1_000_000.0);
        }
    }
}
