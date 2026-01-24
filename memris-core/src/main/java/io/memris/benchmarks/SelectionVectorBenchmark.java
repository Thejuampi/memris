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

public final class SelectionVectorBenchmark {
    private static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_PREFERRED;
    
    public static void main(String[] args) {
        final int SIZE = 10_000_000;
        final int ITERATIONS = 5;
        
        System.out.println("=== SelectionVector Benchmark: " + SIZE + " rows ===\n");
        
        // Benchmark: Creating selection vector
        System.out.println("--- Creating SelectionVector (all rows) ---");
        SelectionVectorFactory factory = SelectionVectorFactory.defaultFactory();
        long totalCreate = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            MutableSelectionVector selection = factory.create(SIZE);
            for (int j = 0; j < SIZE; j++) {
                selection.add(j);
            }
            long elapsed = System.nanoTime() - start;
            totalCreate += elapsed;
            System.out.printf("  Run %d: %d ms (%s)%n", i + 1, elapsed / 1_000_000, 
                selection.getClass().getSimpleName());
        }
        System.out.printf("  Average: %.1f ms%n%n", totalCreate / (double) ITERATIONS / 1_000_000.0);
        
        // Benchmark: Converting to array
        System.out.println("--- toIntArray() ---");
        MutableSelectionVector selection = factory.create(SIZE);
        for (int j = 0; j < SIZE; j++) {
            selection.add(j);
        }
        long totalArray = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            int[] arr = selection.toIntArray();
            long elapsed = System.nanoTime() - start;
            totalArray += elapsed;
            System.out.printf("  Run %d: %d ms (size=%d)%n", i + 1, elapsed / 1_000_000, arr.length);
        }
        System.out.printf("  Average: %.1f ms%n%n", totalArray / (double) ITERATIONS / 1_000_000.0);
        
        // Benchmark: Enumeration
        System.out.println("--- Enumeration (iterate all) ---");
        long totalEnum = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            long start = System.nanoTime();
            long sum = 0;
            IntEnumerator e = selection.enumerator();
            while (e.hasNext()) {
                sum += e.nextInt();
            }
            long elapsed = System.nanoTime() - start;
            totalEnum += elapsed;
            System.out.printf("  Run %d: %d ms (sum=%d)%n", i + 1, elapsed / 1_000_000, sum);
        }
        System.out.printf("  Average: %.1f ms%n%n", totalEnum / (double) ITERATIONS / 1_000_000.0);
        
        // Benchmark: Sparse vs Dense upgrade
        System.out.println("--- Sparse to Dense Upgrade ---");
        int upgradePoint = SelectionVectorFactory.defaultFactory().bitSetThreshold();
        System.out.printf("  Upgrade threshold: %d%n", upgradePoint);
        
        MutableSelectionVector sparse = new IntSelection();
        long totalUpgrade = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            sparse = new IntSelection();
            long start = System.nanoTime();
            for (int j = 0; j < upgradePoint + 1000; j++) {
                sparse.add(j);
            }
            long elapsed = System.nanoTime() - start;
            totalUpgrade += elapsed;
            System.out.printf("  Run %d: %d ms (upgraded to %s)%n", i + 1, elapsed / 1_000_000, 
                sparse.getClass().getSimpleName());
        }
        System.out.printf("  Average: %.1f ms%n", totalUpgrade / (double) ITERATIONS / 1_000_000.0);
    }
}
