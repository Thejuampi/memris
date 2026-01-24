package io.memris.benchmarks;

import io.memris.kernel.Predicate;
import io.memris.kernel.selection.SelectionVector;
import io.memris.kernel.selection.SelectionVectorFactory;
import io.memris.storage.ffm.FfmTable;

import java.lang.foreign.Arena;
import java.util.List;

public final class FullBenchmark {
    public static void main(String[] args) {
        final int SIZE = 10_000_000;
        
        System.out.println("=== Memris Full Benchmark: " + SIZE + " rows ===");
        
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
            long start, elapsed;
            
            // Full table scan
            start = System.nanoTime();
            SelectionVector all = orders.scanAll(factory);
            elapsed = System.nanoTime() - start;
            System.out.printf("Full scan:          %d ms (%d rows)%n", elapsed / 1_000_000, all.size());
            
            // Point lookup (1% selectivity)
            start = System.nanoTime();
            SelectionVector pending = orders.scan(
                    new Predicate.Comparison("status", Predicate.Operator.EQ, "pending"), factory);
            elapsed = System.nanoTime() - start;
            System.out.printf("Status=pending:     %d ms (%d rows)%n", elapsed / 1_000_000, pending.size());
            
            // Range query (10% selectivity)
            start = System.nanoTime();
            SelectionVector amountRange = orders.scan(
                    new Predicate.Between("amount", 10000L, 20000L), factory);
            elapsed = System.nanoTime() - start;
            System.out.printf("Amount 10k-20k:     %d ms (%d rows)%n", elapsed / 1_000_000, amountRange.size());
            
            // IN predicate
            start = System.nanoTime();
            SelectionVector customers = orders.scan(
                    new Predicate.In("customer_id", List.of(1, 2, 3, 4, 5)), factory);
            elapsed = System.nanoTime() - start;
            System.out.printf("Customer ID in [5]: %d ms (%d rows)%n", elapsed / 1_000_000, customers.size());
            
            System.out.println("\n=== Throughput ===");
            System.out.printf("Scan throughput:    %.0f MB/s%n", (SIZE * 24.0) / (elapsed / 1_000_000.0) / 1024.0 / 1024.0);
        }
    }
}
