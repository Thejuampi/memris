package io.memris.benchmarks;

import io.memris.repository.MemrisArena;
import io.memris.core.MemrisConfiguration;
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.runtime.TestEntity;
import io.memris.runtime.TestEntityRepository;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark for string pattern matching operations.
 * Tests STARTING_WITH, ENDING_WITH, and CONTAINING performance.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class StringPatternBenchmark {

    @Param({"10000", "100000", "1000000"})
    public int rowCount;

    private MemrisRepositoryFactory factory;
    private MemrisArena arena;
    private TestEntityRepository repository;

    @Setup(Level.Trial)
    public void setup() {
        MemrisConfiguration configuration = MemrisConfiguration.builder()
                .pageSize(4096)
                .maxPages(4096)
                .initialPages(256)
                .build();
        factory = new MemrisRepositoryFactory(configuration);
        arena = factory.createArena();
        repository = arena.createRepository(TestEntityRepository.class);

        // Populate with test data - names with common prefixes/suffixes
        for (int i = 0; i < rowCount; i++) {
            String name = generateName(i);
            repository.save(new TestEntity(null, name, i % 100));
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (factory != null) {
            factory.close();
        }
    }

    private String generateName(int index) {
        String[] prefixes = {"Alice", "Bob", "Charlie", "David", "Emma", "Frank", "Grace", "Henry"};
        String[] suffixes = {"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"};
        
        String prefix = prefixes[index % prefixes.length];
        String suffix = suffixes[(index / prefixes.length) % suffixes.length];
        return prefix + suffix + index;
    }

    @Benchmark
    public void startingWith_1char(Blackhole blackhole) {
        // Query with 1-character prefix (high selectivity)
        List<TestEntity> results = repository.findByNameStartingWith("A");
        blackhole.consume(results);
    }

    @Benchmark
    public void startingWith_3chars(Blackhole blackhole) {
        // Query with 3-character prefix (medium selectivity)
        List<TestEntity> results = repository.findByNameStartingWith("Ali");
        blackhole.consume(results);
    }

    @Benchmark
    public void startingWith_5chars(Blackhole blackhole) {
        // Query with 5-character prefix (low selectivity)
        List<TestEntity> results = repository.findByNameStartingWith("Alice");
        blackhole.consume(results);
    }

    @Benchmark
    public void endingWith_1char(Blackhole blackhole) {
        // Query with 1-character suffix
        List<TestEntity> results = repository.findByNameEndingWith("h");
        blackhole.consume(results);
    }

    @Benchmark
    public void endingWith_4chars(Blackhole blackhole) {
        // Query with 4-character suffix
        List<TestEntity> results = repository.findByNameEndingWith("ith1");
        blackhole.consume(results);
    }

    @Benchmark
    public void containing_2chars(Blackhole blackhole) {
        // Query containing 2 characters
        List<TestEntity> results = repository.findByNameContaining("ic");
        blackhole.consume(results);
    }

    @Benchmark
    public void containing_5chars(Blackhole blackhole) {
        // Query containing 5 characters
        List<TestEntity> results = repository.findByNameContaining("Alice");
        blackhole.consume(results);
    }

    @Benchmark
    public void equals_exact(Blackhole blackhole) {
        // Baseline: exact equality (should be fast with HashIndex)
        List<TestEntity> results = repository.findByName("AliceSmith0");
        blackhole.consume(results);
    }

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }
}
