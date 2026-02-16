package io.memris.index;

import io.memris.core.Index;
import io.memris.repository.MemrisArena;
import io.memris.core.MemrisConfiguration;
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.runtime.TestEntity;
import io.memris.runtime.TestEntityRepository;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Debug test to verify index usage.
 */
class StringPatternIndexDebugTest {

    @Test
    void verifyPrefixIndexIsUsed() {
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

        // Populate with 10k rows
        for (int i = 0; i < 10000; i++) {
            String name = (i % 2 == 0 ? "Alice" : "Bob") + "Name" + i;
            repository.save(new TestEntity(null, name, i % 100));
        }

        // Query
        List<TestEntity> results = repository.findByNameStartingWith("Ali");
        
        // Should find ~5000 Alice entries
        assertThat(results).hasSize(5000);
        
        // All results should start with "Ali"
        for (TestEntity entity : results) {
            assertThat(entity.name).startsWith("Ali");
        }

        arena.close();
        factory.close();
    }

    @Test
    void compareWithAndWithoutIndex() {
        // With index
        MemrisConfiguration configWith = MemrisConfiguration.builder()
                .enablePrefixIndex(true)
                .build();
        
        MemrisRepositoryFactory factory1 = new MemrisRepositoryFactory(configWith);
        MemrisArena arena1 = factory1.createArena();
        TestEntityRepository repo1 = arena1.createRepository(TestEntityRepository.class);
        
        for (int i = 0; i < 5000; i++) {
            repo1.save(new TestEntity(null, "TestName" + i, i));
        }
        
        long start1 = System.nanoTime();
        List<TestEntity> results1 = repo1.findByNameStartingWith("Test");
        long time1 = System.nanoTime() - start1;
        
        arena1.close();
        factory1.close();

        // Without index
        MemrisConfiguration configWithout = MemrisConfiguration.builder()
                .enablePrefixIndex(false)
                .build();
        
        MemrisRepositoryFactory factory2 = new MemrisRepositoryFactory(configWithout);
        MemrisArena arena2 = factory2.createArena();
        TestEntityRepository repo2 = arena2.createRepository(TestEntityRepository.class);
        
        for (int i = 0; i < 5000; i++) {
            repo2.save(new TestEntity(null, "TestName" + i, i));
        }
        
        long start2 = System.nanoTime();
        List<TestEntity> results2 = repo2.findByNameStartingWith("Test");
        long time2 = System.nanoTime() - start2;
        
        arena2.close();
        factory2.close();

        var withIndex = ResultSnapshot.from(results1);
        var withoutIndex = ResultSnapshot.from(results2);
        assertThat(withoutIndex).usingRecursiveComparison().isEqualTo(withIndex);
    }

    private record ResultSnapshot(int size, List<String> firstThree, List<String> lastThree) {
        private static ResultSnapshot from(List<TestEntity> rows) {
            var names = rows.stream()
                    .map(row -> row.name)
                    .sorted(Comparator.naturalOrder())
                    .toList();
            var head = names.subList(0, Math.min(3, names.size()));
            var tailStart = Math.max(0, names.size() - 3);
            var tail = names.subList(tailStart, names.size());
            return new ResultSnapshot(names.size(), head, tail);
        }
    }
}
