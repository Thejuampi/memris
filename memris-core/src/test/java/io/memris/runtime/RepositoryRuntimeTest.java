package io.memris.runtime;

import io.memris.core.MemrisArena;
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.runtime.IndexedEntity;
import io.memris.runtime.IndexedEntityRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for RepositoryRuntime operations.
 * Focuses on CRUD operations, batch operations, and query execution.
 */
class RepositoryRuntimeTest {

    private MemrisRepositoryFactory factory;
    private MemrisArena arena;

    @BeforeEach
    void setUp() {
        factory = new MemrisRepositoryFactory();
        arena = factory.createArena();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (factory != null) {
            factory.close();
        }
    }

    @Test
    @DisplayName("Should save single entity and assign ID")
    void shouldSaveSingleEntityAndAssignId() {
        // Given
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        TestEntity entity = new TestEntity(null, "Test Name", 25);

        // When
        TestEntity saved = repo.save(entity);

        // Then
        assertThat(saved.id).isNotNull();
        assertThat(saved).usingRecursiveComparison().ignoringFields("id").isEqualTo(entity);
    }

    @Test
    @DisplayName("Should save multiple entities")
    void shouldSaveMultipleEntities() {
        // Given
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        TestEntity entity1 = new TestEntity(null, "Entity 1", 20);
        TestEntity entity2 = new TestEntity(null, "Entity 2", 30);
        TestEntity entity3 = new TestEntity(null, "Entity 3", 40);

        // When
        List<TestEntity> saved = repo.saveAll(Arrays.asList(entity1, entity2, entity3));

        // Then
        assertThat(saved).hasSize(3);
        assertThat(saved).extracting(e -> e.id).doesNotContainNull();
        assertThat(saved).usingRecursiveFieldByFieldElementComparatorIgnoringFields("id").containsExactly(
                new TestEntity(null, "Entity 1", 20),
                new TestEntity(null, "Entity 2", 30),
                new TestEntity(null, "Entity 3", 40)
        );
    }

    @Test
    @DisplayName("Should find entity by ID")
    void shouldFindEntityById() {
        // Given
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        TestEntity saved = repo.save(new TestEntity(null, "Findable", 30));

        // When
        Optional<TestEntity> found = repo.findById(saved.id);

        // Then
        assertThat(found).isPresent();
        assertThat(found.orElseThrow()).usingRecursiveComparison().ignoringFields("id").isEqualTo(saved);
    }

    @Test
    @DisplayName("Should return empty optional when entity not found")
    void shouldReturnEmptyOptionalWhenEntityNotFound() {
        // Given
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);

        // When
        Optional<TestEntity> found = repo.findById(999L);

        // Then
        assertThat(found).isEmpty();
    }

    @Test
    @DisplayName("Should find all entities")
    void shouldFindAllEntities() {
        // Given
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        repo.save(new TestEntity(null, "A", 10));
        repo.save(new TestEntity(null, "B", 20));
        repo.save(new TestEntity(null, "C", 30));

        // When
        List<TestEntity> all = repo.findAll();

        // Then
        assertThat(all).hasSize(3);
    }

    @Test
    @DisplayName("Should check if entity exists by ID")
    void shouldCheckIfEntityExistsById() {
        // Given
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        TestEntity saved = repo.save(new TestEntity(null, "Test", 25));

        // When & Then
        assertThat(repo.existsById(saved.id)).isTrue();
        assertThat(repo.existsById(999L)).isFalse();
    }

    @Test
    @DisplayName("Should count all entities")
    void shouldCountAllEntities() {
        // Given
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        repo.save(new TestEntity(null, "A", 10));
        repo.save(new TestEntity(null, "B", 20));

        // When
        long count = repo.count();

        // Then
        assertThat(count).isEqualTo(2);
    }

    @Test
    @DisplayName("Should delete entity by ID")
    void shouldDeleteEntityById() {
        // Given
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        TestEntity saved = repo.save(new TestEntity(null, "To Delete", 25));

        // When
        repo.deleteById(saved.id);

        // Then
        assertThat(repo.existsById(saved.id)).isFalse();
        assertThat(repo.findAll()).isEmpty();
    }

    @Test
    @DisplayName("Should delete single entity")
    void shouldDeleteSingleEntity() {
        // Given
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        TestEntity saved = repo.save(new TestEntity(null, "To Delete", 25));

        // When
        repo.delete(saved);

        // Then
        assertThat(repo.existsById(saved.id)).isFalse();
    }

    @Test
    @DisplayName("Should delete all entities")
    void shouldDeleteAllEntities() {
        // Given
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        repo.save(new TestEntity(null, "A", 10));
        repo.save(new TestEntity(null, "B", 20));
        repo.save(new TestEntity(null, "C", 30));

        // When
        repo.deleteAll();

        // Then
        assertThat(repo.count()).isEqualTo(0);
        assertThat(repo.findAll()).isEmpty();
    }

    @Test
    @DisplayName("Should delete multiple entities by ID")
    void shouldDeleteMultipleEntitiesById() {
        // Given
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        TestEntity e1 = repo.save(new TestEntity(null, "A", 10));
        TestEntity e2 = repo.save(new TestEntity(null, "B", 20));
        TestEntity e3 = repo.save(new TestEntity(null, "C", 30));

        // When
        repo.deleteAllById(Arrays.asList(e1.id, e3.id));

        // Then
        assertThat(repo.count()).isEqualTo(1);
        assertThat(repo.existsById(e2.id)).isTrue();
    }

    @Test
    @DisplayName("Should find by name with equals")
    void shouldFindByNameWithEquals() {
        // Given
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        repo.save(new TestEntity(null, "Alice", 25));
        repo.save(new TestEntity(null, "Bob", 30));
        repo.save(new TestEntity(null, "Alice", 35));

        // When
        List<TestEntity> results = repo.findByName("Alice");

        // Then
        assertThat(results).hasSize(2);
        assertThat(results).allMatch(e -> e.name.equals("Alice"));
    }

    @Test
    @DisplayName("Should find by name in list")
    void shouldFindByNameInList() {
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        repo.save(new TestEntity(null, "Alice", 25));
        repo.save(new TestEntity(null, "Bob", 30));
        repo.save(new TestEntity(null, "Charlie", 35));

        List<TestEntity> results = repo.findByNameIn(List.of("Alice", "Charlie"));

        assertThat(results).usingRecursiveFieldByFieldElementComparatorIgnoringFields("id").containsExactlyInAnyOrder(
                new TestEntity(null, "Alice", 25),
                new TestEntity(null, "Charlie", 35)
        );
    }

    @Test
    @DisplayName("Should find by name in array")
    void shouldFindByNameInArray() {
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        repo.save(new TestEntity(null, "Alice", 25));
        repo.save(new TestEntity(null, "Bob", 30));
        repo.save(new TestEntity(null, "Charlie", 35));

        List<TestEntity> results = repo.findByNameIn(new String[] { "Bob" });

        assertThat(results).hasSize(1);
        assertThat(results.get(0)).usingRecursiveComparison().ignoringFields("id").isEqualTo(
                new TestEntity(null, "Bob", 30)
        );
    }

    @Test
    @DisplayName("Should find by age greater than")
    void shouldFindByAgeGreaterThan() {
        // Given
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        repo.save(new TestEntity(null, "A", 20));
        repo.save(new TestEntity(null, "B", 30));
        repo.save(new TestEntity(null, "C", 40));

        // When
        List<TestEntity> results = repo.findByAgeGreaterThan(25);

        // Then
        assertThat(results).hasSize(2);
        assertThat(results).usingRecursiveFieldByFieldElementComparatorIgnoringFields("id").containsExactly(
                new TestEntity(null, "B", 30),
                new TestEntity(null, "C", 40)
        );
    }

    @Test
    @DisplayName("Should find by age between")
    void shouldFindByAgeBetween() {
        // Given
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        repo.save(new TestEntity(null, "A", 20));
        repo.save(new TestEntity(null, "B", 30));
        repo.save(new TestEntity(null, "C", 40));
        repo.save(new TestEntity(null, "D", 50));

        // When
        List<TestEntity> results = repo.findByAgeBetween(25, 45);

        // Then
        assertThat(results).usingRecursiveFieldByFieldElementComparatorIgnoringFields("id").containsExactly(
                new TestEntity(null, "B", 30),
                new TestEntity(null, "C", 40)
        );
    }

    @Test
    @DisplayName("Should count by name")
    void shouldCountByName() {
        // Given
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        repo.save(new TestEntity(null, "Alice", 25));
        repo.save(new TestEntity(null, "Alice", 30));
        repo.save(new TestEntity(null, "Bob", 35));

        // When
        long count = repo.countByName("Alice");

        // Then
        assertThat(count).isEqualTo(2);
    }

    @Test
    @DisplayName("Should check existence by name")
    void shouldCheckExistenceByName() {
        // Given
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        repo.save(new TestEntity(null, "Alice", 25));

        // When & Then
        assertThat(repo.existsByName("Alice")).isTrue();
        assertThat(repo.existsByName("Bob")).isFalse();
    }

    @Test
    @DisplayName("Should find with multiple conditions")
    void shouldFindWithMultipleConditions() {
        // Given
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        repo.save(new TestEntity(null, "Alice", 25));
        repo.save(new TestEntity(null, "Alice", 35));
        repo.save(new TestEntity(null, "Bob", 25));

        // When
        List<TestEntity> results = repo.findByNameAndAge("Alice", 25);

        // Then
        assertThat(results).hasSize(1);
        assertThat(results.get(0)).usingRecursiveComparison().ignoringFields("id").isEqualTo(
                new TestEntity(null, "Alice", 25)
        );
    }

    @Test
    @DisplayName("Should find with OR condition")
    void shouldFindWithOrCondition() {
        // Given
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        repo.save(new TestEntity(null, "Alice", 20));
        repo.save(new TestEntity(null, "Bob", 30));
        repo.save(new TestEntity(null, "Charlie", 40));

        // When
        List<TestEntity> results = repo.findByNameOrAge("Alice", 40);

        // Then
        assertThat(results).usingRecursiveFieldByFieldElementComparatorIgnoringFields("id").containsExactlyInAnyOrder(
                new TestEntity(null, "Alice", 20),
                new TestEntity(null, "Charlie", 40)
        );
    }

    @Test
    @DisplayName("Should find with order by")
    void shouldFindWithOrderBy() {
        // Given
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        repo.save(new TestEntity(null, "Charlie", 30));
        repo.save(new TestEntity(null, "Alice", 20));
        repo.save(new TestEntity(null, "Bob", 25));

        // When
        List<TestEntity> results = repo.findByOrderByAgeAsc();

        // Then
        assertThat(results).usingRecursiveFieldByFieldElementComparatorIgnoringFields("id").containsExactly(
                new TestEntity(null, "Alice", 20),
                new TestEntity(null, "Bob", 25),
                new TestEntity(null, "Charlie", 30)
        );
    }


    @Test
    @DisplayName("Should find top N results")
    void shouldFindTopNResults() {
        // Given
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        repo.save(new TestEntity(null, "A", 30));
        repo.save(new TestEntity(null, "B", 20));
        repo.save(new TestEntity(null, "C", 10));
        repo.save(new TestEntity(null, "D", 40));

        // When
        List<TestEntity> results = repo.findTop2ByOrderByAgeAsc();

        // Then
        assertThat(results).hasSize(2);
        assertThat(results).usingRecursiveFieldByFieldElementComparatorIgnoringFields("id").containsExactly(
                new TestEntity(null, "C", 10),
                new TestEntity(null, "B", 20)
        );
    }

    @Test
    @DisplayName("Should handle empty results gracefully")
    void shouldHandleEmptyResultsGracefully() {
        // Given
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);

        // When
        List<TestEntity> results = repo.findByName("NonExistent");
        long count = repo.countByName("NonExistent");
        boolean exists = repo.existsByName("NonExistent");

        // Then
        assertThat(results).isEmpty();
        assertThat(count).isEqualTo(0);
        assertThat(exists).isFalse();
    }

    @Test
    @DisplayName("Should find by id in set")
    void shouldFindByIdInSet() {
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        TestEntity first = repo.save(new TestEntity(null, "A", 10));
        TestEntity second = repo.save(new TestEntity(null, "B", 20));
        repo.save(new TestEntity(null, "C", 30));

        Set<TestEntity> results = repo.findByIdIn(Set.of(first.id, second.id));

        assertThat(results).hasSize(2);
        assertThat(results).extracting(e -> e.id).containsExactlyInAnyOrder(first.id, second.id);
    }

    @Test
    @DisplayName("Should find by id in varargs")
    void shouldFindByIdInVarargs() {
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        TestEntity first = repo.save(new TestEntity(null, "A", 10));
        TestEntity second = repo.save(new TestEntity(null, "B", 20));
        repo.save(new TestEntity(null, "C", 30));

        Set<TestEntity> results = repo.findByIdIn(first.id, second.id);

        assertThat(results).hasSize(2);
        assertThat(results).extracting(e -> e.id).containsExactlyInAnyOrder(first.id, second.id);
    }

    @Test
    @DisplayName("Should update existing entity")
    void shouldUpdateExistingEntity() {
        // Given
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        TestEntity saved = repo.save(new TestEntity(null, "Original", 25));
        Long id = saved.id;

        // When
        saved.name = "Updated";
        saved.age = 30;
        TestEntity updated = repo.save(saved);

        // Then
        assertThat(updated.id).isEqualTo(id);
        assertThat(updated).usingRecursiveComparison().ignoringFields("id").isEqualTo(
                new TestEntity(null, "Updated", 30)
        );

        // Verify it was actually updated
        Optional<TestEntity> found = repo.findById(id);
        assertThat(found).isPresent();
        assertThat(found.orElseThrow()).usingRecursiveComparison().ignoringFields("id").isEqualTo(
                new TestEntity(null, "Updated", 30)
        );
    }

    @Test
    @DisplayName("Should handle null values in entities")
    void shouldHandleNullValuesInEntities() {
        // Given
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        TestEntity entity = new TestEntity();
        entity.name = null;
        entity.age = 25;

        // When
        TestEntity saved = repo.save(entity);

        // Then
        assertThat(saved.id).isNotNull();
        assertThat(saved.name).isNull();
    }

    @Test
    @DisplayName("Should maintain index entries on save, update, and delete")
    void shouldMaintainIndexEntriesOnSaveUpdateDelete() {
        IndexedEntityRepository repo = arena.createRepository(IndexedEntityRepository.class);
        IndexedEntity first = repo.save(new IndexedEntity(null, "alpha", 10));
        IndexedEntity second = repo.save(new IndexedEntity(null, "beta", 20));

        assertThat(repo.countByCategory("alpha")).isEqualTo(1);
        assertThat(repo.findByCategory("beta")).hasSize(1);

        first.category = "beta";
        repo.save(first);

        assertThat(repo.countByCategory("alpha")).isEqualTo(0);
        assertThat(repo.countByCategory("beta")).isEqualTo(2);

        repo.delete(second);

        assertThat(repo.countByCategory("beta")).isEqualTo(1);
    }

    @Test
    @DisplayName("Should group entities by department")
    void shouldGroupEntitiesByDepartment() {
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        repo.save(new TestEntity(null, "Alice", 25, "Engineering"));
        repo.save(new TestEntity(null, "Bob", 30, "Engineering"));
        repo.save(new TestEntity(null, "Charlie", 35, "Sales"));
        repo.save(new TestEntity(null, "David", 40, "Sales"));
        repo.save(new TestEntity(null, "Eve", 45, "HR"));

        Map<String, List<TestEntity>> grouped = repo.findAllGroupingByDepartment();

        assertThat(grouped).hasSize(3);
        assertThat(grouped.get("Engineering")).hasSize(2);
        assertThat(grouped.get("Sales")).hasSize(2);
        assertThat(grouped.get("HR")).hasSize(1);
        assertThat(grouped.get("Engineering")).extracting(e -> e.name).containsExactlyInAnyOrder("Alice", "Bob");
        assertThat(grouped.get("Sales")).extracting(e -> e.name).containsExactlyInAnyOrder("Charlie", "David");
        assertThat(grouped.get("HR")).extracting(e -> e.name).containsExactlyInAnyOrder("Eve");
    }

    @Test
    @DisplayName("Should count entities by department")
    void shouldCountEntitiesByDepartment() {
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        repo.save(new TestEntity(null, "Alice", 25, "Engineering"));
        repo.save(new TestEntity(null, "Bob", 30, "Engineering"));
        repo.save(new TestEntity(null, "Charlie", 35, "Sales"));
        repo.save(new TestEntity(null, "David", 40, "Sales"));
        repo.save(new TestEntity(null, "Eve", 45, "HR"));

        Map<String, Long> counts = repo.countByDepartment();

        assertThat(counts).hasSize(3);
        assertThat(counts.get("Engineering")).isEqualTo(2L);
        assertThat(counts.get("Sales")).isEqualTo(2L);
        assertThat(counts.get("HR")).isEqualTo(1L);
    }
}
