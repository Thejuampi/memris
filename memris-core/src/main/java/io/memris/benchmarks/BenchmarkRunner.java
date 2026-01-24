package io.memris.benchmarks;

import io.memris.kernel.Predicate;
import io.memris.kernel.selection.SelectionVector;
import io.memris.kernel.selection.SelectionVectorFactory;
import io.memris.storage.ffm.FfmTable;

import java.lang.foreign.Arena;
import java.util.List;

public class BenchmarkRunner {
    public static void main(String[] args) {
        int[] sizes = {100_000, 1_000_000, 10_000_000};

        for (int size : sizes) {
            System.out.println("\n=== Benchmark: " + size + " rows ===");
            Arena arena = Arena.ofConfined();
            FfmTable table = new FfmTable("test", arena, List.of(
                    new FfmTable.ColumnSpec("id", int.class),
                    new FfmTable.ColumnSpec("value", int.class)
            ), size);

            for (int i = 0; i < size; i++) {
                table.insert(i, i % 100);
            }

            SelectionVectorFactory factory = SelectionVectorFactory.defaultFactory();

            for (int w = 0; w < 3; w++) {
                table.scan(new Predicate.Comparison("value", Predicate.Operator.EQ, 42), factory);
                table.scan(new Predicate.Between("value", 20, 30), factory);
            }

            long start = System.nanoTime();
            SelectionVector all = table.scanAll(factory);
            long scanAllNs = System.nanoTime() - start;
            System.out.printf("scanAll():      %d ms (selected %d rows)%n", scanAllNs / 1_000_000, all.size());

            start = System.nanoTime();
            SelectionVector eq = table.scan(
                    new Predicate.Comparison("value", Predicate.Operator.EQ, 42),
                    factory);
            long scanEqNs = System.nanoTime() - start;
            System.out.printf("scanEquals(42): %d ms (selected %d rows)%n", scanEqNs / 1_000_000, eq.size());

            start = System.nanoTime();
            SelectionVector between = table.scan(
                    new Predicate.Between("value", 20, 30),
                    factory);
            long scanBetweenNs = System.nanoTime() - start;
            System.out.printf("scanBetween(20-30): %d ms (selected %d rows)%n", scanBetweenNs / 1_000_000, between.size());

            arena.close();
        }
    }
}
