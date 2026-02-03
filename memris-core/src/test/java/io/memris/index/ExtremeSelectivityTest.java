package io.memris.index;

import io.memris.core.MemrisArena;
import io.memris.core.MemrisConfiguration;
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.runtime.TestEntity;
import io.memris.runtime.TestEntityRepository;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Extreme selectivity test - 1M rows, 0.01% selectivity (100 matches).
 */
class ExtremeSelectivityTest {

    @Test
    void extremeSelectivity_1M_rows_0_01_percent() {
        System.out.println("\n=== EXTREME SELECTIVITY: 1M Rows, 0.01% Selectivity (100 matches) ===\n");

        // With index
        long timeWithIndex = testWithIndex();
        
        // Without index
        long timeWithoutIndex = testWithoutIndex();
        
        double speedup = (double) timeWithoutIndex / timeWithIndex;
        System.out.println("\n=== RESULTS ===");
        System.out.println("With Index: " + timeWithIndex + " μs");
        System.out.println("Without Index: " + timeWithoutIndex + " μs");
        System.out.println("Speedup: " + String.format("%.1f", speedup) + "x");
        
        // Expect at least 10x speedup with such high selectivity
        assertThat(speedup).isGreaterThan(5.0);
    }

    private long testWithIndex() {
        System.out.println("Testing WITH index...");
        
        MemrisConfiguration config = MemrisConfiguration.builder()
                .pageSize(4096)
                .maxPages(8192)
                .initialPages(2048)
                .enablePrefixIndex(true)
                .build();

        MemrisRepositoryFactory factory = new MemrisRepositoryFactory(config);
        MemrisArena arena = factory.createArena();
        TestEntityRepository repository = arena.createRepository(TestEntityRepository.class);

        int rowCount = 1_000_000;
        int matchCount = 100;
        String targetPrefix = "ZZZZZTARGET";
        
        System.out.println("  Inserting " + rowCount + " rows (" + matchCount + " will match)...");
        
        // Insert matches first
        for (int i = 0; i < matchCount; i++) {
            repository.save(new TestEntity(null, targetPrefix + i, i));
        }
        
        // Insert non-matching rows
        for (int i = matchCount; i < rowCount; i++) {
            repository.save(new TestEntity(null, "AAAAA" + i, i % 100));
        }

        System.out.println("  Warming up...");
        for (int i = 0; i < 5; i++) {
            repository.findByNameStartingWith(targetPrefix);
        }

        System.out.println("  Measuring...");
        long startTime = System.nanoTime();
        int iterations = 50;
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

    private long testWithoutIndex() {
        System.out.println("Testing WITHOUT index...");
        
        MemrisConfiguration config = MemrisConfiguration.builder()
                .pageSize(4096)
                .maxPages(8192)
                .initialPages(2048)
                .enablePrefixIndex(false)
                .build();

        MemrisRepositoryFactory factory = new MemrisRepositoryFactory(config);
        MemrisArena arena = factory.createArena();
        TestEntityRepository repository = arena.createRepository(TestEntityRepository.class);

        int rowCount = 1_000_000;
        int matchCount = 100;
        String targetPrefix = "ZZZZZTARGET";
        
        System.out.println("  Inserting " + rowCount + " rows (" + matchCount + " will match)...");
        
        // Insert matches first
        for (int i = 0; i < matchCount; i++) {
            repository.save(new TestEntity(null, targetPrefix + i, i));
        }
        
        // Insert non-matching rows
        for (int i = matchCount; i < rowCount; i++) {
            repository.save(new TestEntity(null, "AAAAA" + i, i % 100));
        }

        System.out.println("  Warming up...");
        for (int i = 0; i < 5; i++) {
            repository.findByNameStartingWith(targetPrefix);
        }

        System.out.println("  Measuring...");
        long startTime = System.nanoTime();
        int iterations = 50;
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
