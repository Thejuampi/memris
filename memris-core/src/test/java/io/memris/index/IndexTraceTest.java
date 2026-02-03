package io.memris.index;

import io.memris.core.Index;
import io.memris.core.MemrisArena;
import io.memris.core.MemrisConfiguration;
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.runtime.TestEntity;
import io.memris.runtime.TestEntityRepository;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Trace test to understand index query execution.
 */
class IndexTraceTest {

    @Test
    void traceIndexQueryExecution() {
        System.out.println("\n=== Tracing Index Query Execution ===\n");

        MemrisConfiguration config = MemrisConfiguration.builder()
                .pageSize(4096)
                .maxPages(4096)
                .initialPages(256)
                .enablePrefixIndex(true)
                .build();

        MemrisRepositoryFactory factory = new MemrisRepositoryFactory(config);
        MemrisArena arena = factory.createArena();
        TestEntityRepository repository = arena.createRepository(TestEntityRepository.class);

        // Check if indexes were created
        var indexes = arena.getIndexes(TestEntity.class);
        System.out.println("Indexes for TestEntity: " + (indexes != null ? indexes.keySet() : "null"));
        if (indexes != null) {
            for (var entry : indexes.entrySet()) {
                System.out.println("  " + entry.getKey() + " -> " + entry.getValue().getClass().getName());
            }
        }

        // Insert test data
        System.out.println("\nInserting 10 test entities...");
        for (int i = 0; i < 10; i++) {
            String name = (i < 3) ? "TARGET" + i : "OTHER" + i;
            repository.save(new TestEntity(null, name, i));
        }

        // Query
        System.out.println("\nQuerying findByNameStartingWith(\"TARGET\")...");
        List<TestEntity> results = repository.findByNameStartingWith("TARGET");
        System.out.println("Results: " + results.size() + " entities");
        for (TestEntity e : results) {
            System.out.println("  - " + e.name);
        }

        assertThat(results).hasSize(3);

        arena.close();
        factory.close();
    }
}
