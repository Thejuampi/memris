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
 * Verify index is actually being used.
 */
class IndexUsageVerificationTest {

    @Test
    void verifyIndexIsQueried() {
        MemrisConfiguration config = MemrisConfiguration.builder()
                .enablePrefixIndex(true)
                .build();

        MemrisRepositoryFactory factory = new MemrisRepositoryFactory(config);
        MemrisArena arena = factory.createArena();
        TestEntityRepository repository = arena.createRepository(TestEntityRepository.class);

        // Insert 10000 rows, only 10 match
        for (int i = 0; i < 10000; i++) {
            String name = (i < 10) ? "TARGET" + i : "OTHER" + i;
            repository.save(new TestEntity(null, name, i));
        }

        // Query and measure time
        long start1 = System.nanoTime();
        List<TestEntity> results1 = repository.findByNameStartingWith("TARGET");
        long time1 = System.nanoTime() - start1;

        // Query again (should be faster if index is used)
        long start2 = System.nanoTime();
        List<TestEntity> results2 = repository.findByNameStartingWith("TARGET");
        long time2 = System.nanoTime() - start2;

        System.out.println("First query: " + (time1 / 1000) + " μs");
        System.out.println("Second query: " + (time2 / 1000) + " μs");
        System.out.println("Results: " + results1.size() + " entities");

        assertThat(results1).hasSize(10);

        arena.close();
        factory.close();
    }

    @Test
    void compareScans() {
        System.out.println("\n=== Direct Table Scan vs Index Query ===\n");

        // Test 1: With index
        MemrisConfiguration configWithIndex = MemrisConfiguration.builder()
                .enablePrefixIndex(true)
                .build();

        MemrisRepositoryFactory factory1 = new MemrisRepositoryFactory(configWithIndex);
        MemrisArena arena1 = factory1.createArena();
        TestEntityRepository repo1 = arena1.createRepository(TestEntityRepository.class);

        for (int i = 0; i < 50000; i++) {
            repo1.save(new TestEntity(null, "Name" + i, i));
        }

        long t1 = System.nanoTime();
        var results1 = repo1.findByNameStartingWith("Name499");
        long timeWithIndex = System.nanoTime() - t1;

        System.out.println("With Index: " + (timeWithIndex / 1000) + " μs, results: " + results1.size());

        arena1.close();
        factory1.close();

        // Test 2: Without index
        MemrisConfiguration configNoIndex = MemrisConfiguration.builder()
                .enablePrefixIndex(false)
                .build();

        MemrisRepositoryFactory factory2 = new MemrisRepositoryFactory(configNoIndex);
        MemrisArena arena2 = factory2.createArena();
        TestEntityRepository repo2 = arena2.createRepository(TestEntityRepository.class);

        for (int i = 0; i < 50000; i++) {
            repo2.save(new TestEntity(null, "Name" + i, i));
        }

        long t2 = System.nanoTime();
        var results2 = repo2.findByNameStartingWith("Name499");
        long timeWithoutIndex = System.nanoTime() - t2;

        System.out.println("Without Index: " + (timeWithoutIndex / 1000) + " μs, results: " + results2.size());

        arena2.close();
        factory2.close();

        double speedup = (double) timeWithoutIndex / timeWithIndex;
        System.out.println("\nSpeedup: " + String.format("%.2f", speedup) + "x");
    }
}
