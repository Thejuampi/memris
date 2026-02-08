package io.memris.index;

import io.memris.core.Index;
import io.memris.core.MemrisArena;
import io.memris.core.MemrisConfiguration;
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.runtime.TestEntity;
import io.memris.runtime.TestEntityRepository;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Performance test for String pattern matching indexes.
 * Compares performance with and without prefix/suffix indexes.
 * 
 * Run with: mvn test -Dtest=StringPatternPerformanceTest -Dtag=benchmark
 */
@Tag("benchmark")
@Disabled("Manual benchmark only")
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

        var avgTimeMicros = runStartsWithBenchmark(config);

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

        assertThat(runStartsWithBenchmark(config)).isPositive();
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

        var avgTimeMicros = runEndsWithBenchmark(config);

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

        assertThat(runEndsWithBenchmark(config)).isPositive();
    }

    @Test
    void calculateSpeedupFactor() {
        assertThat(Arrays.stream(new long[]{
                runStartsWithBenchmark(MemrisConfiguration.builder().enablePrefixIndex(true).build()),
                runStartsWithBenchmark(MemrisConfiguration.builder().enablePrefixIndex(false).build()),
                runEndsWithBenchmark(MemrisConfiguration.builder().enableSuffixIndex(true).build()),
                runEndsWithBenchmark(MemrisConfiguration.builder().enableSuffixIndex(false).build())
        }).allMatch(value -> value > 0)).isTrue();
    }

    private long runStartsWithBenchmark(MemrisConfiguration config) {
        try (var factory = new MemrisRepositoryFactory(config);
             var arena = factory.createArena()) {
            var repository = arena.createRepository(TestEntityRepository.class);
            var rowCount = 50_000;
            var prefixes = new String[]{"Alice", "Bob", "Charlie", "David", "Emma"};
            for (var index = 0; index < rowCount; index++) {
                var name = prefixes[index % prefixes.length] + "Name" + index;
                repository.save(new TestEntity(null, name, index % 100));
            }
            for (var index = 0; index < 10; index++) {
                repository.findByNameStartingWith("Ali");
            }
            var startTime = System.nanoTime();
            var iterations = 100;
            for (var index = 0; index < iterations; index++) {
                var results = repository.findByNameStartingWith("Ali");
                assertThat(results).isNotNull();
            }
            var endTime = System.nanoTime();
            return (endTime - startTime) / iterations / 1000;
        }
    }

    private long runEndsWithBenchmark(MemrisConfiguration config) {
        try (var factory = new MemrisRepositoryFactory(config);
             var arena = factory.createArena()) {
            var repository = arena.createRepository(TestEntityRepository.class);
            var rowCount = 50_000;
            var suffixes = new String[]{"Smith", "Jones", "Brown", "Davis", "Wilson"};
            for (var index = 0; index < rowCount; index++) {
                var name = "Name" + index + suffixes[index % suffixes.length];
                repository.save(new TestEntity(null, name, index % 100));
            }
            for (var index = 0; index < 10; index++) {
                repository.findByNameEndingWith("ith");
            }
            var startTime = System.nanoTime();
            var iterations = 100;
            for (var index = 0; index < iterations; index++) {
                var results = repository.findByNameEndingWith("ith");
                assertThat(results).isNotNull();
            }
            var endTime = System.nanoTime();
            return (endTime - startTime) / iterations / 1000;
        }
    }
}
