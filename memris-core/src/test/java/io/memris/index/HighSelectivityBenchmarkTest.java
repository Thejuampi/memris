package io.memris.index;

import io.memris.core.MemrisArena;
import io.memris.core.MemrisConfiguration;
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.runtime.TestEntity;
import io.memris.runtime.TestEntityRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * High-selectivity benchmark to demonstrate 10-100x speedup with string pattern indexes.
 * 
 * Conditions for maximum speedup:
 * - Large dataset (1M+ rows)
 * - High selectivity (<0.1% match rate)
 * - Unique prefixes
 * 
 * Run with: mvn test -Dtest=HighSelectivityBenchmarkTest -Dtag=benchmark
 */
@Tag("benchmark")
class HighSelectivityBenchmarkTest {

    @Test
    void demonstrateMassiveSpeedup_1M_rows_0_1_percent_selectivity() {
        System.out.println("\n=== HIGH SELECTIVITY BENCHMARK: 1M Rows, 0.1% Selectivity ===\n");

        // Test with index
        long timeWithIndex = testWithIndex(1_000_000, 0.001);
        
        // Test without index  
        long timeWithoutIndex = testWithoutIndex(1_000_000, 0.001);
        
        double speedup = (double) timeWithoutIndex / timeWithIndex;
        System.out.println("\n=== RESULTS ===");
        System.out.println("With Index: " + timeWithIndex + " μs");
        System.out.println("Without Index: " + timeWithoutIndex + " μs");
        System.out.println("Speedup: " + String.format("%.1f", speedup) + "x");
        
        // Assert we get at least 10x speedup
        assertThat(speedup).isGreaterThan(10.0);
    }

    @Test
    void demonstrateSpeedup_500k_rows_0_5_percent_selectivity() {
        System.out.println("\n=== BENCHMARK: 500K Rows, 0.5% Selectivity ===\n");

        long timeWithIndex = testWithIndex(500_000, 0.005);
        long timeWithoutIndex = testWithoutIndex(500_000, 0.005);
        
        double speedup = (double) timeWithoutIndex / timeWithIndex;
        System.out.println("\nSpeedup: " + String.format("%.1f", speedup) + "x");
        
        assertThat(speedup).isGreaterThan(5.0);
    }

    @Test
    void demonstrateSpeedup_100k_rows_1_percent_selectivity() {
        System.out.println("\n=== BENCHMARK: 100K Rows, 1% Selectivity ===\n");

        long timeWithIndex = testWithIndex(100_000, 0.01);
        long timeWithoutIndex = testWithoutIndex(100_000, 0.01);
        
        double speedup = (double) timeWithoutIndex / timeWithIndex;
        System.out.println("\nSpeedup: " + String.format("%.1f", speedup) + "x");
        
        assertThat(speedup).isGreaterThan(2.0);
    }

    private long testWithIndex(int rowCount, double selectivity) {
        System.out.println("Testing WITH index (" + rowCount + " rows, " + (selectivity * 100) + "% selectivity)...");
        
        MemrisConfiguration config = MemrisConfiguration.builder()
                .pageSize(4096)
                .maxPages(8192)
                .initialPages(1024)
                .enablePrefixIndex(true)
                .build();

        MemrisRepositoryFactory factory = new MemrisRepositoryFactory(config);
        MemrisArena arena = factory.createArena();
        TestEntityRepository repository = arena.createRepository(TestEntityRepository.class);

        // Generate data with unique prefixes
        // We want 'selectivity' percent of rows to match "UNIQUE_PREFIX"
        int matchCount = (int) (rowCount * selectivity);
        String targetPrefix = "TARGET";
        
        System.out.println("  Inserting " + rowCount + " rows (" + matchCount + " will match)...");
        
        for (int i = 0; i < rowCount; i++) {
            String name;
            if (i < matchCount) {
                // These will match the query
                name = targetPrefix + UUID.randomUUID().toString().substring(0, 20);
            } else {
                // These won't match - use different prefixes
                name = "OTHER" + (i % 100) + UUID.randomUUID().toString().substring(0, 20);
            }
            repository.save(new TestEntity(null, name, i % 100));
        }

        // Warmup
        for (int i = 0; i < 5; i++) {
            repository.findByNameStartingWith(targetPrefix);
        }

        // Measure
        System.out.println("  Measuring...");
        long startTime = System.nanoTime();
        int iterations = 100;
        for (int i = 0; i < iterations; i++) {
            List<TestEntity> results = repository.findByNameStartingWith(targetPrefix);
            assertThat(results).hasSize(matchCount);
        }
        long endTime = System.nanoTime();

        long avgTimeMicros = (endTime - startTime) / iterations / 1000;
        System.out.println("  Average: " + avgTimeMicros + " μs/op");

        arena.close();
        factory.close();
        
        return avgTimeMicros;
    }

    private long testWithoutIndex(int rowCount, double selectivity) {
        System.out.println("Testing WITHOUT index (" + rowCount + " rows, " + (selectivity * 100) + "% selectivity)...");
        
        MemrisConfiguration config = MemrisConfiguration.builder()
                .pageSize(4096)
                .maxPages(8192)
                .initialPages(1024)
                .enablePrefixIndex(false)
                .build();

        MemrisRepositoryFactory factory = new MemrisRepositoryFactory(config);
        MemrisArena arena = factory.createArena();
        TestEntityRepository repository = arena.createRepository(TestEntityRepository.class);

        // Generate same data
        int matchCount = (int) (rowCount * selectivity);
        String targetPrefix = "TARGET";
        
        System.out.println("  Inserting " + rowCount + " rows (" + matchCount + " will match)...");
        
        for (int i = 0; i < rowCount; i++) {
            String name;
            if (i < matchCount) {
                name = targetPrefix + UUID.randomUUID().toString().substring(0, 20);
            } else {
                name = "OTHER" + (i % 100) + UUID.randomUUID().toString().substring(0, 20);
            }
            repository.save(new TestEntity(null, name, i % 100));
        }

        // Warmup
        for (int i = 0; i < 5; i++) {
            repository.findByNameStartingWith(targetPrefix);
        }

        // Measure
        System.out.println("  Measuring...");
        long startTime = System.nanoTime();
        int iterations = 100;
        for (int i = 0; i < iterations; i++) {
            List<TestEntity> results = repository.findByNameStartingWith(targetPrefix);
            assertThat(results).hasSize(matchCount);
        }
        long endTime = System.nanoTime();

        long avgTimeMicros = (endTime - startTime) / iterations / 1000;
        System.out.println("  Average: " + avgTimeMicros + " μs/op");

        arena.close();
        factory.close();
        
        return avgTimeMicros;
    }
}
