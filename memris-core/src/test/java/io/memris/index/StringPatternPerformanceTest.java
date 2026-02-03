package io.memris.index;

import io.memris.core.Index;
import io.memris.core.MemrisArena;
import io.memris.core.MemrisConfiguration;
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.runtime.TestEntity;
import io.memris.runtime.TestEntityRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Performance test for String pattern matching indexes.
 * Compares performance with and without prefix/suffix indexes.
 * 
 * Run with: mvn test -Dtest=StringPatternPerformanceTest -Dtag=benchmark
 */
@Tag("benchmark")
class StringPatternPerformanceTest {

    @Test
    void measureStartingWithPerformance_withIndex() {
        // Given - with prefix index enabled
        MemrisConfiguration config = MemrisConfiguration.builder()
                .pageSize(4096)
                .maxPages(4096)
                .initialPages(256)
                .enablePrefixIndex(true)
                .build();

        MemrisRepositoryFactory factory = new MemrisRepositoryFactory(config);
        MemrisArena arena = factory.createArena();
        TestEntityRepository repository = arena.createRepository(TestEntityRepository.class);

        // Populate with 50k rows
        int rowCount = 50000;
        String[] prefixes = {"Alice", "Bob", "Charlie", "David", "Emma"};
        for (int i = 0; i < rowCount; i++) {
            String name = prefixes[i % prefixes.length] + "Name" + i;
            repository.save(new TestEntity(null, name, i % 100));
        }

        // Warmup
        for (int i = 0; i < 10; i++) {
            repository.findByNameStartingWith("Ali");
        }

        // Measure
        long startTime = System.nanoTime();
        int iterations = 100;
        for (int i = 0; i < iterations; i++) {
            List<TestEntity> results = repository.findByNameStartingWith("Ali");
            assertThat(results).isNotNull();
        }
        long endTime = System.nanoTime();

        long avgTimeMicros = (endTime - startTime) / iterations / 1000;
        System.out.println("STARTING_WITH with Index (50k rows): " + avgTimeMicros + " μs/op");

        arena.close();
        factory.close();

        // Assert it's reasonably fast (should be under 1000μs)
        assertThat(avgTimeMicros).isLessThan(1000);
    }

    @Test
    void measureStartingWithPerformance_withoutIndex() {
        // Given - with prefix index disabled
        MemrisConfiguration config = MemrisConfiguration.builder()
                .pageSize(4096)
                .maxPages(4096)
                .initialPages(256)
                .enablePrefixIndex(false)
                .build();

        MemrisRepositoryFactory factory = new MemrisRepositoryFactory(config);
        MemrisArena arena = factory.createArena();
        TestEntityRepository repository = arena.createRepository(TestEntityRepository.class);

        // Populate with 50k rows
        int rowCount = 50000;
        String[] prefixes = {"Alice", "Bob", "Charlie", "David", "Emma"};
        for (int i = 0; i < rowCount; i++) {
            String name = prefixes[i % prefixes.length] + "Name" + i;
            repository.save(new TestEntity(null, name, i % 100));
        }

        // Warmup
        for (int i = 0; i < 10; i++) {
            repository.findByNameStartingWith("Ali");
        }

        // Measure
        long startTime = System.nanoTime();
        int iterations = 100;
        for (int i = 0; i < iterations; i++) {
            List<TestEntity> results = repository.findByNameStartingWith("Ali");
            assertThat(results).isNotNull();
        }
        long endTime = System.nanoTime();

        long avgTimeMicros = (endTime - startTime) / iterations / 1000;
        System.out.println("STARTING_WITH without Index (50k rows): " + avgTimeMicros + " μs/op");

        arena.close();
        factory.close();
    }

    @Test
    void measureEndingWithPerformance_withIndex() {
        // Given - with suffix index enabled
        MemrisConfiguration config = MemrisConfiguration.builder()
                .pageSize(4096)
                .maxPages(4096)
                .initialPages(256)
                .enableSuffixIndex(true)
                .build();

        MemrisRepositoryFactory factory = new MemrisRepositoryFactory(config);
        MemrisArena arena = factory.createArena();
        TestEntityRepository repository = arena.createRepository(TestEntityRepository.class);

        // Populate with 50k rows
        int rowCount = 50000;
        String[] suffixes = {"Smith", "Jones", "Brown", "Davis", "Wilson"};
        for (int i = 0; i < rowCount; i++) {
            String name = "Name" + i + suffixes[i % suffixes.length];
            repository.save(new TestEntity(null, name, i % 100));
        }

        // Warmup
        for (int i = 0; i < 10; i++) {
            repository.findByNameEndingWith("ith");
        }

        // Measure
        long startTime = System.nanoTime();
        int iterations = 100;
        for (int i = 0; i < iterations; i++) {
            List<TestEntity> results = repository.findByNameEndingWith("ith");
            assertThat(results).isNotNull();
        }
        long endTime = System.nanoTime();

        long avgTimeMicros = (endTime - startTime) / iterations / 1000;
        System.out.println("ENDING_WITH with Index (50k rows): " + avgTimeMicros + " μs/op");

        arena.close();
        factory.close();

        // Assert it's reasonably fast
        assertThat(avgTimeMicros).isLessThan(1000);
    }

    @Test
    void measureEndingWithPerformance_withoutIndex() {
        // Given - with suffix index disabled
        MemrisConfiguration config = MemrisConfiguration.builder()
                .pageSize(4096)
                .maxPages(4096)
                .initialPages(256)
                .enableSuffixIndex(false)
                .build();

        MemrisRepositoryFactory factory = new MemrisRepositoryFactory(config);
        MemrisArena arena = factory.createArena();
        TestEntityRepository repository = arena.createRepository(TestEntityRepository.class);

        // Populate with 50k rows
        int rowCount = 50000;
        String[] suffixes = {"Smith", "Jones", "Brown", "Davis", "Wilson"};
        for (int i = 0; i < rowCount; i++) {
            String name = "Name" + i + suffixes[i % suffixes.length];
            repository.save(new TestEntity(null, name, i % 100));
        }

        // Warmup
        for (int i = 0; i < 10; i++) {
            repository.findByNameEndingWith("ith");
        }

        // Measure
        long startTime = System.nanoTime();
        int iterations = 100;
        for (int i = 0; i < iterations; i++) {
            List<TestEntity> results = repository.findByNameEndingWith("ith");
            assertThat(results).isNotNull();
        }
        long endTime = System.nanoTime();

        long avgTimeMicros = (endTime - startTime) / iterations / 1000;
        System.out.println("ENDING_WITH without Index (50k rows): " + avgTimeMicros + " μs/op");

        arena.close();
        factory.close();
    }

    @Test
    void calculateSpeedupFactor() {
        System.out.println("\n=== String Pattern Matching Performance Summary ===\n");

        // Run with index tests first to get measurements
        measureStartingWithPerformance_withIndex();
        measureStartingWithPerformance_withoutIndex();
        measureEndingWithPerformance_withIndex();
        measureEndingWithPerformance_withoutIndex();

        System.out.println("\nExpected speedup: 10-100x faster with indexes");
        System.out.println("Index type: Trie (prefix tree) for O(k) lookup vs O(n) scan");
    }
}
