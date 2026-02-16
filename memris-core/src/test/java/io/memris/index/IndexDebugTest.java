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
 * Debug test to verify index is being created and used.
 */
class IndexDebugTest {

    @Test
    void debugIndexCreation() {
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
        repository.save(new TestEntity(null, "TARGET123", 1));
        repository.save(new TestEntity(null, "TARGET456", 2));
        repository.save(new TestEntity(null, "OTHER789", 3));

        // Query
        List<TestEntity> results = repository.findByNameStartingWith("TARGET");
        
        assertThat(results).hasSize(2);
        
        arena.close();
        factory.close();
    }

    @Test
    void verifyIndexTypeViaReflection() throws Exception {
        MemrisConfiguration config = MemrisConfiguration.builder()
                .enablePrefixIndex(true)
                .build();

        MemrisRepositoryFactory factory = new MemrisRepositoryFactory(config);
        MemrisArena arena = factory.createArena();
        
        // Create repository to trigger index creation
        TestEntityRepository repository = arena.createRepository(TestEntityRepository.class);
        
        // Access the arena's indexes map via reflection
        java.lang.reflect.Field indexesField = MemrisArena.class.getDeclaredField("indexes");
        indexesField.setAccessible(true);
        java.util.Map<Class<?>, java.util.Map<String, Object>> indexes = 
            (java.util.Map<Class<?>, java.util.Map<String, Object>>) indexesField.get(arena);
        
        // Check if index was created
        java.util.Map<String, Object> entityIndexes = indexes.get(TestEntity.class);
        assertThat(entityIndexes).isNotNull();
        
        Object nameIndex = entityIndexes.get("name");
        
        assertThat(nameIndex).isInstanceOf(io.memris.index.StringPrefixIndex.class);
        
        arena.close();
        factory.close();
    }
}
