package io.memris.index;

import io.memris.repository.MemrisArena;
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
        MemrisConfiguration config = MemrisConfiguration.builder()
                .pageSize(4096)
                .maxPages(4096)
                .initialPages(256)
                .enablePrefixIndex(true)
                .build();

        MemrisRepositoryFactory factory = new MemrisRepositoryFactory(config);
        MemrisArena arena = factory.createArena();
        TestEntityRepository repository = arena.createRepository(TestEntityRepository.class);

        // Insert test data
        for (int i = 0; i < 10; i++) {
            String name = (i < 3) ? "TARGET" + i : "OTHER" + i;
            repository.save(new TestEntity(null, name, i));
        }

        // Query
        List<TestEntity> results = repository.findByNameStartingWith("TARGET");

        assertThat(results).hasSize(3);

        arena.close();
        factory.close();
    }
}
