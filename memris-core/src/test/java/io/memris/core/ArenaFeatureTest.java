package io.memris.core;

import io.memris.repository.MemrisRepositoryFactory;
import io.memris.repository.MemrisArena;
import io.memris.repository.MemrisRepository;
import io.memris.core.Entity;
import io.memris.core.GeneratedValue;
import io.memris.core.GenerationType;
import io.memris.core.Index;
import io.memris.core.Id;

import io.memris.core.Entity;
import io.memris.core.GeneratedValue;
import io.memris.core.GenerationType;
import io.memris.core.Index;

import io.memris.repository.MemrisRepositoryFactory;
import io.memris.repository.MemrisArena;
import io.memris.repository.MemrisRepository;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/**
 * TDD Tests for Arena Feature
 * 
 * RED Phase: Write failing tests that define the expected behavior
 */
class ArenaFeatureTest {

    private MemrisRepositoryFactory factory;

    @BeforeEach
    void setUp() {
        factory = new MemrisRepositoryFactory();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (factory != null) {
            factory.close();
        }
    }

    @Test
    void shouldCreateArenaWithUniqueId() {
        // Given
        
        // When
        MemrisArena arena1 = factory.createArena();
        MemrisArena arena2 = factory.createArena();
        
        // Then
        assertThat(arena1.getArenaId()).isGreaterThan(0);
        assertThat(arena2.getArenaId()).isGreaterThan(arena1.getArenaId());
    }

    @Test
    void shouldCreateRepositoryInArena() {
        // Given
        MemrisArena arena = factory.createArena();
        
        // When
        SimpleEntityRepository repo = arena.createRepository(SimpleEntityRepository.class);
        
        // Then
        assertThat(repo).isNotNull();
    }

    @Test
    void shouldIsolateDataBetweenArenas() {
        // Given
        MemrisArena arena1 = factory.createArena();
        MemrisArena arena2 = factory.createArena();
        
        SimpleEntityRepository repo1 = arena1.createRepository(SimpleEntityRepository.class);
        SimpleEntityRepository repo2 = arena2.createRepository(SimpleEntityRepository.class);
        
        // When - Save entity in arena1
        SimpleEntity entity = new SimpleEntity();
        entity.name = "Test";
        SimpleEntity saved = repo1.save(entity);
        
        // Then - Arena2 should not see the entity
        assertThat(repo1.findById(saved.id)).isPresent();
        assertThat(repo2.findById(saved.id)).isEmpty();
    }

    @Test
    void shouldFindRepositoryByEntityClassInArena() {
        // Given
        MemrisArena arena = factory.createArena();
        SimpleEntityRepository repo = arena.createRepository(SimpleEntityRepository.class);
        
        // When
        MemrisRepository<SimpleEntity> found = arena.getRepository(SimpleEntity.class);
        
        // Then
        assertThat(found).isSameAs(repo);
    }

    @Test
    void shouldDefaultToSharedArena() {
        // Given - Factory without explicit arena
        
        // When - Create repository directly on factory
        SimpleEntityRepository repo = factory.createRepository(SimpleEntityRepository.class);
        
        // Then - Should use default arena
        assertThat(repo).isNotNull();
    }

    @Test
    void shouldSupportCloseArena() {
        // Given
        MemrisArena arena = factory.createArena();
        SimpleEntityRepository repo = arena.createRepository(SimpleEntityRepository.class);
        
        SimpleEntity entity = new SimpleEntity();
        entity.name = "Test";
        repo.save(entity);
        
        // When
        arena.close();

        // Then - After close, arena should reject new operations
        assertThatThrownBy(() -> arena.getRepository(SimpleEntity.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
        assertThatThrownBy(() -> arena.createRepository(SimpleEntityRepository.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
    }

    // Test entity and repository
    public static class SimpleEntity {
        @Id
        public Long id;
        public String name;
    }

    public interface SimpleEntityRepository extends MemrisRepository<SimpleEntity> {
        SimpleEntity save(SimpleEntity entity);
        java.util.Optional<SimpleEntity> findById(Long id);
    }
}
