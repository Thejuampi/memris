package io.memris.benchmarks;

import io.memris.kernel.selection.IntEnumerator;
import io.memris.kernel.selection.IntSelection;
import io.memris.kernel.selection.MutableSelectionVector;
import io.memris.kernel.selection.SelectionVector;
import io.memris.kernel.selection.SelectionVectorFactory;

import java.lang.foreign.Arena;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.util.List;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorSpecies;

/**
 * SelectionVector benchmark for Memris.
 * CRITICAL: Follows ARRANGE-ACT-MEASURE pattern with pre-cooked inputs.
 * - ARRANGE: All data structures prepared before timing
 * - ACT: Only measured operation during timing
 * - MEASURE: Results collected and reported AFTER timing
 * - No I/O during measurement
 * - No object creation during measurement loops
 * - Results consumed to prevent dead code elimination
 */
public final class SelectionVectorBenchmark {
    private static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_PREFERRED;

    public static void main(String[] args) {
        final int SIZE = 10_000_000;
        final int ITERATIONS = 5;

        System.out.println("=== SelectionVector Benchmark: " + SIZE + " rows ===\n");

        // ========== ARRANGE: Setup factory and pre-cook values ==========
        SelectionVectorFactory factory = SelectionVectorFactory.defaultFactory();
        int upgradePoint = SelectionVectorFactory.defaultFactory().bitSetThreshold();
        int upgradeSize = upgradePoint + 1000;

        // Warmup
        for (int w = 0; w < 3; w++) {
            MutableSelectionVector s = factory.create(SIZE);
            for (int j = 0; j < SIZE; j++) s.add(j);
            s.toIntArray();
            IntEnumerator e = s.enumerator();
            while (e.hasNext()) e.nextInt();
        }

        // ========== ACT + MEASURE: Creating selection vector ==========
        System.out.println("--- Creating SelectionVector (all rows) ---");
        long[] createTimes = new long[ITERATIONS];
        String[] createTypes = new String[ITERATIONS];
        long totalCreate = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            MutableSelectionVector selection = factory.create(SIZE);
            for (int j = 0; j < SIZE; j++) {
                selection.add(j);
            }
            long elapsed = System.nanoTime() - start;
            createTimes[i] = elapsed;
            createTypes[i] = selection.getClass().getSimpleName();
            totalCreate += elapsed;
            // Consume result to prevent dead code elimination
            if (selection.size() < 0) throw new AssertionError();
        }
        // Print AFTER measurement
        for (int i = 0; i < ITERATIONS; i++) {
            System.out.printf("  Run %d: %d ms (%s)%n", i + 1, createTimes[i] / 1_000_000, createTypes[i]);
        }
        System.out.printf("  Average: %.1f ms%n%n", totalCreate / (double) ITERATIONS / 1_000_000.0);

        // ========== ARRANGE: Pre-create selection for array/enum tests ==========
        MutableSelectionVector selection = factory.create(SIZE);
        for (int j = 0; j < SIZE; j++) {
            selection.add(j);
        }

        // ========== ACT + MEASURE: Converting to array ==========
        System.out.println("--- toIntArray() ---");
        long[] arrayTimes = new long[ITERATIONS];
        int[] arraySizes = new int[ITERATIONS];
        long totalArray = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            int[] arr = selection.toIntArray();
            long elapsed = System.nanoTime() - start;
            arrayTimes[i] = elapsed;
            arraySizes[i] = arr.length;
            totalArray += elapsed;
            // Consume result to prevent dead code elimination
            if (arr.length < 0) throw new AssertionError();
        }
        // Print AFTER measurement
        for (int i = 0; i < ITERATIONS; i++) {
            System.out.printf("  Run %d: %d ms (size=%d)%n", i + 1, arrayTimes[i] / 1_000_000, arraySizes[i]);
        }
        System.out.printf("  Average: %.1f ms%n%n", totalArray / (double) ITERATIONS / 1_000_000.0);

        // ========== ACT + MEASURE: Enumeration ==========
        System.out.println("--- Enumeration (iterate all) ---");
        long[] enumTimes = new long[ITERATIONS];
        long[] enumSums = new long[ITERATIONS];
        long totalEnum = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            long sum = 0;
            IntEnumerator e = selection.enumerator();
            while (e.hasNext()) {
                sum += e.nextInt();
            }
            long elapsed = System.nanoTime() - start;
            enumTimes[i] = elapsed;
            enumSums[i] = sum;
            totalEnum += elapsed;
            // Consume result to prevent dead code elimination
            if (sum < 0) throw new AssertionError();
        }
        // Print AFTER measurement
        for (int i = 0; i < ITERATIONS; i++) {
            System.out.printf("  Run %d: %d ms (sum=%d)%n", i + 1, enumTimes[i] / 1_000_000, enumSums[i]);
        }
        System.out.printf("  Average: %.1f ms%n%n", totalEnum / (double) ITERATIONS / 1_000_000.0);

        // ========== ACT + MEASURE: Sparse vs Dense upgrade ==========
        System.out.println("--- Sparse to Dense Upgrade ---");
        System.out.printf("  Upgrade threshold: %d%n", upgradePoint);

        long[] upgradeTimes = new long[ITERATIONS];
        String[] upgradeTypes = new String[ITERATIONS];
        long totalUpgrade = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            MutableSelectionVector sparse = new IntSelection();
            long start = System.nanoTime();
            for (int j = 0; j < upgradeSize; j++) {
                sparse.add(j);
            }
            long elapsed = System.nanoTime() - start;
            upgradeTimes[i] = elapsed;
            upgradeTypes[i] = sparse.getClass().getSimpleName();
            totalUpgrade += elapsed;
            // Consume result to prevent dead code elimination
            if (sparse.size() < 0) throw new AssertionError();
        }
        // Print AFTER measurement
        for (int i = 0; i < ITERATIONS; i++) {
            System.out.printf("  Run %d: %d ms (upgraded to %s)%n", i + 1, upgradeTimes[i] / 1_000_000, upgradeTypes[i]);
        }
        System.out.printf("  Average: %.1f ms%n", totalUpgrade / (double) ITERATIONS / 1_000_000.0);
    }
}
