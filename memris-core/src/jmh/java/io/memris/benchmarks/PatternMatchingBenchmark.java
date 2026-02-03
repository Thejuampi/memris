package io.memris.benchmarks;

import io.memris.core.Index;
import io.memris.core.MemrisArena;
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
 * Benchmark comparing String pattern matching performance with and without indexes.
 * 
 * Tests STARTING_WITH and ENDING_WITH queries to demonstrate the performance improvement
 * from O(n) table scans to O(k) index lookups.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class PatternMatchingBenchmark {

    @Param({"10000", "50000", "100000", "500000"})
    public int rowCount;

    @Param({"true", "false"})
    public boolean useIndex;

    private MemrisRepositoryFactory factory;
    private MemrisArena arena;
    private TestEntityRepository repository;

    @Setup(Level.Trial)
    public void setup() {
        MemrisConfiguration.Builder configBuilder = MemrisConfiguration.builder()
                .pageSize(4096)
                .maxPages(4096)
                .initialPages(256);
        
        if (!useIndex) {
            configBuilder
                .enablePrefixIndex(false)
                .enableSuffixIndex(false);
        }
        
        factory = new MemrisRepositoryFactory(configBuilder.build());
        arena = factory.createArena();
        repository = arena.createRepository(TestEntityRepository.class);

        // Populate with test data - names with common prefixes/suffixes
        String[] prefixes = {"Alice", "Bob", "Charlie", "David", "Emma", "Frank", "Grace", "Henry"};
        String[] suffixes = {"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"};
        
        for (int i = 0; i < rowCount; i++) {
            String prefix = prefixes[i % prefixes.length];
            String suffix = suffixes[(i / prefixes.length) % suffixes.length];
            String name = prefix + suffix + i;
            repository.save(new TestEntity(null, name, i % 100));
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (factory != null) {
            factory.close();
        }
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
    public void endingWith_4chars(Blackhole blackhole) {
        // Query with 4-character suffix
        List<TestEntity> results = repository.findByNameEndingWith("ith1");
        blackhole.consume(results);
    }

    @Benchmark
    public void containing_3chars(Blackhole blackhole) {
        // Query containing 3 characters (no index support - baseline)
        List<TestEntity> results = repository.findByNameContaining("lic");
        blackhole.consume(results);
    }

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }
}
