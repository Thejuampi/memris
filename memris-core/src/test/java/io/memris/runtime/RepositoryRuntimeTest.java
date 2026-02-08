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
import static io.memris.testutil.EntityAssertions.assertEntitiesMatchAnyOrder;
import static io.memris.testutil.EntityAssertions.assertEntitiesMatchExactOrder;
import static io.memris.testutil.EntityAssertions.assertEntityMatches;

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
        var actual = new SaveSnapshot(saved.id != null, view(saved));
        var expected = new SaveSnapshot(true, view(new TestEntity(null, "Test Name", 25)));
        assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
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
        var actual = new SaveAllSnapshot(
                saved.stream().allMatch(entity -> entity.id != null),
                views(saved)
        );
        var expected = new SaveAllSnapshot(
                true,
                List.of(
                        view(new TestEntity(null, "Entity 1", 20)),
                        view(new TestEntity(null, "Entity 2", 30)),
                        view(new TestEntity(null, "Entity 3", 40))
                )
        );
        assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
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
        var actual = new FindByIdSnapshot(found.isPresent(), found.map(RepositoryRuntimeTest::view).orElse(null));
        var expected = new FindByIdSnapshot(true, view(saved));
        assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
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
        assertThat(new ExistsSnapshot(repo.existsById(saved.id), repo.existsById(999L))).usingRecursiveComparison()
                .isEqualTo(new ExistsSnapshot(true, false));
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
        assertThat(new DeleteByIdSnapshot(repo.existsById(saved.id), repo.findAll().size())).usingRecursiveComparison()
                .isEqualTo(new DeleteByIdSnapshot(false, 0));
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
        assertThat(new DeleteAllSnapshot(repo.count(), repo.findAll().size())).usingRecursiveComparison()
                .isEqualTo(new DeleteAllSnapshot(0L, 0));
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
        assertThat(new DeleteManySnapshot(repo.count(), repo.existsById(e2.id))).usingRecursiveComparison()
                .isEqualTo(new DeleteManySnapshot(1L, true));
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
        assertThat(new NameQuerySnapshot(results.size(), results.stream().allMatch(entity -> entity.name.equals("Alice"))))
                .usingRecursiveComparison()
                .isEqualTo(new NameQuerySnapshot(2, true));
    }

    @Test
    @DisplayName("Should find by name in list")
    void shouldFindByNameInList() {
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        repo.save(new TestEntity(null, "Alice", 25));
        repo.save(new TestEntity(null, "Bob", 30));
        repo.save(new TestEntity(null, "Charlie", 35));

        List<TestEntity> results = repo.findByNameIn(List.of("Alice", "Charlie"));

        assertEntitiesMatchAnyOrder(results, List.of(
                new TestEntity(null, "Alice", 25),
                new TestEntity(null, "Charlie", 35)
        ), "id");
    }

    @Test
    @DisplayName("Should find by name in array")
    void shouldFindByNameInArray() {
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        repo.save(new TestEntity(null, "Alice", 25));
        repo.save(new TestEntity(null, "Bob", 30));
        repo.save(new TestEntity(null, "Charlie", 35));

        List<TestEntity> results = repo.findByNameIn(new String[] { "Bob" });

        var actual = new NameInArraySnapshot(results.size(), results.isEmpty() ? null : view(results.get(0)));
        var expected = new NameInArraySnapshot(1, view(new TestEntity(null, "Bob", 30)));
        assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
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
        assertEntitiesMatchExactOrder(results, List.of(
                new TestEntity(null, "B", 30),
                new TestEntity(null, "C", 40)
        ), "id");
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
        assertEntitiesMatchExactOrder(results, List.of(
                new TestEntity(null, "B", 30),
                new TestEntity(null, "C", 40)
        ), "id");
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
        assertThat(new ExistsSnapshot(repo.existsByName("Alice"), repo.existsByName("Bob"))).usingRecursiveComparison()
                .isEqualTo(new ExistsSnapshot(true, false));
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
        var actual = new NameInArraySnapshot(results.size(), results.isEmpty() ? null : view(results.get(0)));
        var expected = new NameInArraySnapshot(1, view(new TestEntity(null, "Alice", 25)));
        assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
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
        assertEntitiesMatchAnyOrder(results, List.of(
                new TestEntity(null, "Alice", 20),
                new TestEntity(null, "Charlie", 40)
        ), "id");
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
        assertEntitiesMatchExactOrder(results, List.of(
                new TestEntity(null, "Alice", 20),
                new TestEntity(null, "Bob", 25),
                new TestEntity(null, "Charlie", 30)
        ), "id");
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
        assertEntitiesMatchExactOrder(results, List.of(
                new TestEntity(null, "C", 10),
                new TestEntity(null, "B", 20)
        ), "id");
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
        assertThat(new EmptyResultSnapshot(results.size(), count, exists)).usingRecursiveComparison()
                .isEqualTo(new EmptyResultSnapshot(0, 0L, false));
    }

    @Test
    @DisplayName("Should find by id in set")
    void shouldFindByIdInSet() {
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        TestEntity first = repo.save(new TestEntity(null, "A", 10));
        TestEntity second = repo.save(new TestEntity(null, "B", 20));
        repo.save(new TestEntity(null, "C", 30));

        Set<TestEntity> results = repo.findByIdIn(Set.of(first.id, second.id));

        assertThat(results.stream().map(entity -> entity.id).toList()).containsExactlyInAnyOrder(first.id, second.id);
    }

    @Test
    @DisplayName("Should find by id in varargs")
    void shouldFindByIdInVarargs() {
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        TestEntity first = repo.save(new TestEntity(null, "A", 10));
        TestEntity second = repo.save(new TestEntity(null, "B", 20));
        repo.save(new TestEntity(null, "C", 30));

        Set<TestEntity> results = repo.findByIdIn(first.id, second.id);

        assertThat(results.stream().map(entity -> entity.id).toList()).containsExactlyInAnyOrder(first.id, second.id);
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
        Optional<TestEntity> found = repo.findById(id);
        var actual = new UpdateSnapshot(
                updated.id,
                view(updated),
                found.isPresent(),
                found.map(RepositoryRuntimeTest::view).orElse(null)
        );
        var expected = new UpdateSnapshot(id, view(new TestEntity(null, "Updated", 30)), true,
                view(new TestEntity(null, "Updated", 30)));
        assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
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
        assertThat(new SaveNullSnapshot(saved.id != null, saved.name)).usingRecursiveComparison()
                .isEqualTo(new SaveNullSnapshot(true, null));
    }

    @Test
    @DisplayName("Should maintain index entries on save, update, and delete")
    void shouldMaintainIndexEntriesOnSaveUpdateDelete() {
        IndexedEntityRepository repo = arena.createRepository(IndexedEntityRepository.class);
        IndexedEntity first = repo.save(new IndexedEntity(null, "alpha", 10));
        IndexedEntity second = repo.save(new IndexedEntity(null, "beta", 20));

        var alphaBefore = repo.countByCategory("alpha");
        var betaBefore = repo.findByCategory("beta").size();

        first.category = "beta";
        repo.save(first);

        var alphaAfterUpdate = repo.countByCategory("alpha");
        var betaAfterUpdate = repo.countByCategory("beta");

        repo.delete(second);

        var betaAfterDelete = repo.countByCategory("beta");
        assertThat(new IndexMaintenanceSnapshot(alphaBefore, betaBefore, alphaAfterUpdate, betaAfterUpdate, betaAfterDelete))
                .usingRecursiveComparison()
                .isEqualTo(new IndexMaintenanceSnapshot(1L, 1, 0L, 2L, 1L));
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

        assertThat(groupedNames(grouped)).isEqualTo(Map.of(
                "Engineering", Set.of("Alice", "Bob"),
                "Sales", Set.of("Charlie", "David"),
                "HR", Set.of("Eve")
        ));
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

        assertThat(counts).isEqualTo(Map.of("Engineering", 2L, "Sales", 2L, "HR", 1L));
    }

    @Test
    @DisplayName("Should group entities by department as set")
    void shouldGroupEntitiesByDepartmentAsSet() {
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        repo.save(new TestEntity(null, "Alice", 25, "Engineering"));
        repo.save(new TestEntity(null, "Bob", 30, "Engineering"));
        repo.save(new TestEntity(null, "Charlie", 35, "Sales"));
        repo.save(new TestEntity(null, "David", 40, "Sales"));
        repo.save(new TestEntity(null, "Eve", 45, "HR"));

        Map<String, Set<TestEntity>> grouped = repo.findAllGroupingByDepartmentAsSet();

        assertThat(groupedNamesFromSet(grouped)).isEqualTo(Map.of(
                "Engineering", Set.of("Alice", "Bob"),
                "Sales", Set.of("Charlie", "David"),
                "HR", Set.of("Eve")
        ));
    }

    @Test
    @DisplayName("Should group entities by department and age")
    void shouldGroupEntitiesByDepartmentAndAge() {
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        repo.save(new TestEntity(null, "Alice", 25, "Engineering"));
        repo.save(new TestEntity(null, "Bob", 25, "Engineering"));
        repo.save(new TestEntity(null, "Charlie", 35, "Sales"));
        repo.save(new TestEntity(null, "David", 35, "Sales"));
        repo.save(new TestEntity(null, "Eve", 45, "HR"));

        Map<DepartmentAgeKey, List<TestEntity>> grouped = repo.findAllGroupingByDepartmentAndAge();

        assertThat(groupedNamesByDepartmentAge(grouped)).isEqualTo(Map.of(
                new DepartmentAgeKey("Engineering", 25), Set.of("Alice", "Bob"),
                new DepartmentAgeKey("Sales", 35), Set.of("Charlie", "David"),
                new DepartmentAgeKey("HR", 45), Set.of("Eve")
        ));
    }

    @Test
    @DisplayName("Should count entities by department and age")
    void shouldCountEntitiesByDepartmentAndAge() {
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        repo.save(new TestEntity(null, "Alice", 25, "Engineering"));
        repo.save(new TestEntity(null, "Bob", 25, "Engineering"));
        repo.save(new TestEntity(null, "Charlie", 35, "Sales"));
        repo.save(new TestEntity(null, "David", 35, "Sales"));
        repo.save(new TestEntity(null, "Eve", 45, "HR"));

        Map<DepartmentAgeKey, Long> counts = repo.countByDepartmentAndAge();

        assertThat(counts).isEqualTo(Map.of(
                new DepartmentAgeKey("Engineering", 25), 2L,
                new DepartmentAgeKey("Sales", 35), 2L,
                new DepartmentAgeKey("HR", 45), 1L
        ));
    }

    @Test
    @DisplayName("Should count entities by name grouped by department and age")
    void shouldCountEntitiesByNameGroupedByDepartmentAndAge() {
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        repo.save(new TestEntity(null, "Alice", 25, "Engineering"));
        repo.save(new TestEntity(null, "Alice", 30, "Engineering"));
        repo.save(new TestEntity(null, "Bob", 25, "Engineering"));
        repo.save(new TestEntity(null, "Alice", 35, "Sales"));
        repo.save(new TestEntity(null, "Eve", 45, "HR"));

        Map<DepartmentAgeKey, Long> counts = repo.countByNameGroupingByDepartmentAndAge("Alice");

        assertThat(counts).isEqualTo(Map.of(
                new DepartmentAgeKey("Engineering", 25), 1L,
                new DepartmentAgeKey("Engineering", 30), 1L,
                new DepartmentAgeKey("Sales", 35), 1L
        ));
    }

    @Test
    @DisplayName("Should group entities by department and age via JPQL")
    void shouldGroupEntitiesByDepartmentAndAgeViaJpql() {
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        repo.save(new TestEntity(null, "Alice", 25, "Engineering"));
        repo.save(new TestEntity(null, "Bob", 25, "Engineering"));
        repo.save(new TestEntity(null, "Charlie", 35, "Sales"));
        repo.save(new TestEntity(null, "David", 35, "Sales"));
        repo.save(new TestEntity(null, "Eve", 45, "HR"));

        Map<DepartmentAgeKey, List<TestEntity>> grouped = repo.findAllGroupedByDepartmentAndAgeJpql();

        assertThat(grouped.entrySet().stream().collect(
                java.util.stream.Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().size())))
                .isEqualTo(Map.of(
                        new DepartmentAgeKey("Engineering", 25), 2,
                        new DepartmentAgeKey("Sales", 35), 2,
                        new DepartmentAgeKey("HR", 45), 1
                ));
    }

    @Test
    @DisplayName("Should count entities by department and age via JPQL having")
    void shouldCountEntitiesByDepartmentAndAgeViaJpqlHaving() {
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);
        repo.save(new TestEntity(null, "Alice", 25, "Engineering"));
        repo.save(new TestEntity(null, "Bob", 25, "Engineering"));
        repo.save(new TestEntity(null, "Charlie", 35, "Sales"));
        repo.save(new TestEntity(null, "David", 35, "Sales"));
        repo.save(new TestEntity(null, "Eve", 45, "HR"));

        Map<DepartmentAgeKey, Long> counts = repo.countByDepartmentAndAgeHavingMin(1);

        assertThat(counts).isEqualTo(Map.of(
                new DepartmentAgeKey("Engineering", 25), 2L,
                new DepartmentAgeKey("Sales", 35), 2L
        ));
    }

    private static TestEntityView view(TestEntity entity) {
        return new TestEntityView(entity.name, entity.age, entity.department);
    }

    private static List<TestEntityView> views(List<TestEntity> entities) {
        return entities.stream().map(RepositoryRuntimeTest::view).toList();
    }

    private static Map<String, Set<String>> groupedNames(Map<String, List<TestEntity>> grouped) {
        return grouped.entrySet().stream().collect(
                java.util.stream.Collectors.toMap(Map.Entry::getKey,
                        entry -> entry.getValue().stream().map(entity -> entity.name).collect(java.util.stream.Collectors.toSet())));
    }

    private static Map<String, Set<String>> groupedNamesFromSet(Map<String, Set<TestEntity>> grouped) {
        return grouped.entrySet().stream().collect(
                java.util.stream.Collectors.toMap(Map.Entry::getKey,
                        entry -> entry.getValue().stream().map(entity -> entity.name).collect(java.util.stream.Collectors.toSet())));
    }

    private static Map<DepartmentAgeKey, Set<String>> groupedNamesByDepartmentAge(Map<DepartmentAgeKey, List<TestEntity>> grouped) {
        return grouped.entrySet().stream().collect(
                java.util.stream.Collectors.toMap(Map.Entry::getKey,
                        entry -> entry.getValue().stream().map(entity -> entity.name).collect(java.util.stream.Collectors.toSet())));
    }

    private record SaveSnapshot(boolean idAssigned, TestEntityView entity) {
    }

    private record SaveAllSnapshot(boolean allIdsAssigned, List<TestEntityView> entities) {
    }

    private record FindByIdSnapshot(boolean present, TestEntityView entity) {
    }

    private record ExistsSnapshot(boolean first, boolean second) {
    }

    private record DeleteByIdSnapshot(boolean exists, int count) {
    }

    private record DeleteAllSnapshot(long count, int listSize) {
    }

    private record DeleteManySnapshot(long count, boolean survivorExists) {
    }

    private record NameQuerySnapshot(int size, boolean allMatch) {
    }

    private record NameInArraySnapshot(int size, TestEntityView entity) {
    }

    private record EmptyResultSnapshot(int size, long count, boolean exists) {
    }

    private record UpdateSnapshot(Long id, TestEntityView updated, boolean found, TestEntityView reloaded) {
    }

    private record SaveNullSnapshot(boolean idAssigned, String name) {
    }

    private record IndexMaintenanceSnapshot(long alphaBefore, int betaBefore, long alphaAfterUpdate,
                                            long betaAfterUpdate, long betaAfterDelete) {
    }

    private record TestEntityView(String name, int age, String department) {
    }
}
