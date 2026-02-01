package io.memris.benchmarks;

import io.memris.core.MemrisArena;
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.runtime.TestEntity;
import io.memris.runtime.TestEntityRepository;

import java.util.concurrent.TimeUnit;

public class SimpleBenchmarkRunner {

    public static void main(String[] args) throws Exception {
        System.out.println("Running Memris Benchmarks...");
        System.out.println("Java version: " + System.getProperty("java.version"));
        System.out.println("OS: " + System.getProperty("os.name") + " " + System.getProperty("os.version"));
        System.out.println();

        // Warmup JVM
        System.out.println("Warming up JVM...");
        for (int i = 0; i < 3; i++) {
            runInsertBenchmark(10000);
        }
        System.out.println("Warmup complete.\n");

        // Run benchmarks
        System.out.println("=== Benchmark Results ===\n");

        // Insert benchmark
        System.out.println("Insert 100k rows:");
        runInsertBenchmark(100000);

        System.out.println();

        // Lookup benchmark
        System.out.println("Lookup by ID (100k operations):");
        runLookupBenchmark(100000);

        System.out.println();

        // Scan benchmark
        System.out.println("Scan all rows (100k rows):");
        runScanBenchmark(100000);
    }

    private static void runInsertBenchmark(int count) throws Exception {
        try (MemrisRepositoryFactory factory = new MemrisRepositoryFactory()) {
            MemrisArena arena = factory.createArena();
            TestEntityRepository repository = arena.createRepository(TestEntityRepository.class);

            TestEntity[] entities = new TestEntity[count];
            for (int i = 0; i < count; i++) {
                entities[i] = new TestEntity(null, "user" + i, i % 100);
            }

            long start = System.nanoTime();
            for (TestEntity entity : entities) {
                repository.save(entity);
            }
            long duration = System.nanoTime() - start;

            double opsPerSecond = (count * 1_000_000_000.0) / duration;
            double msPerOp = duration / (count * 1_000_000.0);

            System.out.printf("  Total time: %.2f ms%n", duration / 1_000_000.0);
            System.out.printf("  Ops/sec: %.2f%n", opsPerSecond);
            System.out.printf("  Avg time: %.3f ms/op%n", msPerOp);
        }
    }

    private static void runLookupBenchmark(int count) throws Exception {
        try (MemrisRepositoryFactory factory = new MemrisRepositoryFactory()) {
            MemrisArena arena = factory.createArena();
            TestEntityRepository repository = arena.createRepository(TestEntityRepository.class);

            // Pre-populate
            long[] ids = new long[count];
            for (int i = 0; i < count; i++) {
                TestEntity saved = repository.save(new TestEntity(null, "user" + i, i % 100));
                ids[i] = saved.id;
            }

            long start = System.nanoTime();
            for (long id : ids) {
                repository.findById(id);
            }
            long duration = System.nanoTime() - start;

            double opsPerSecond = (count * 1_000_000_000.0) / duration;
            double msPerOp = duration / (count * 1_000_000.0);

            System.out.printf("  Total time: %.2f ms%n", duration / 1_000_000.0);
            System.out.printf("  Ops/sec: %.2f%n", opsPerSecond);
            System.out.printf("  Avg time: %.3f ms/op%n", msPerOp);
        }
    }

    private static void runScanBenchmark(int count) throws Exception {
        try (MemrisRepositoryFactory factory = new MemrisRepositoryFactory()) {
            MemrisArena arena = factory.createArena();
            TestEntityRepository repository = arena.createRepository(TestEntityRepository.class);

            // Pre-populate
            for (int i = 0; i < count; i++) {
                repository.save(new TestEntity(null, "user" + i, i % 100));
            }

            long start = System.nanoTime();
            repository.findAll();
            long duration = System.nanoTime() - start;

            double opsPerSecond = (count * 1_000_000_000.0) / duration;
            double msPerOp = duration / 1_000_000.0;

            System.out.printf("  Total time: %.2f ms%n", duration / 1_000_000.0);
            System.out.printf("  Ops/sec: %.2f%n", opsPerSecond);
            System.out.printf("  Avg time: %.3f ms for full scan%n", msPerOp);
        }
    }
}
