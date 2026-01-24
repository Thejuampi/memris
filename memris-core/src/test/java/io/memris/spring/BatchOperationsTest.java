package io.memris.spring;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

/**
 * TDD tests for batch operations (saveAll, deleteAllById, deleteAll, etc.).
 * Target: O(1) operations, no ArrayList growth, blazing fast.
 */
class BatchOperationsTest {

    // Repository interface for JPA-style repository creation
    interface TestEntityRepository extends MemrisRepository<TestEntity> {
        void save(TestEntity entity);
        void saveAll(List<TestEntity> entities);
        long count();
        Optional<TestEntity> findById(Integer id);
        void deleteAllById(List<Integer> ids);
        List<TestEntity> findAllById(List<Integer> ids);
        void deleteAll();
    }

    @Test
    void saveAll_multipleEntities_savesAll() {
        try (var factory = new MemrisRepositoryFactory()) {
            TestEntityRepository repo = factory.createJPARepository(TestEntityRepository.class);

            TestEntity e1 = new TestEntity();
            e1.id = 1;
            e1.name = "Alice";

            TestEntity e2 = new TestEntity();
            e2.id = 2;
            e2.name = "Bob";

            TestEntity e3 = new TestEntity();
            e3.id = 3;
            e3.name = "Charlie";

            // Batch save
            repo.saveAll(List.of(e1, e2, e3));

            // Verify all saved
            assertThat(repo.count()).isEqualTo(3);
            assertThat(repo.findById(1)).isPresent().hasValueSatisfying(e -> assertThat(e.name).isEqualTo("Alice"));
            assertThat(repo.findById(2)).isPresent().hasValueSatisfying(e -> assertThat(e.name).isEqualTo("Bob"));
            assertThat(repo.findById(3)).isPresent().hasValueSatisfying(e -> assertThat(e.name).isEqualTo("Charlie"));
        }
    }

    @Test
    void saveAll_emptyList_returnsEmpty() {
        try (var factory = new MemrisRepositoryFactory()) {
            TestEntityRepository repo = factory.createJPARepository(TestEntityRepository.class);

            // Batch save empty list
            repo.saveAll(List.of());

            assertThat(repo.count()).isZero();
        }
    }

    @Test
    void saveAll_singleEntity_savesCorrectly() {
        try (var factory = new MemrisRepositoryFactory()) {
            TestEntityRepository repo = factory.createJPARepository(TestEntityRepository.class);

            TestEntity e = new TestEntity();
            e.id = 42;
            e.name = "Test";

            // Batch save single entity
            repo.saveAll(List.of(e));

            assertThat(repo.findById(42)).isPresent().hasValueSatisfying(entity -> assertThat(entity.name).isEqualTo("Test"));
        }
    }

    // ==================== deleteAllById Tests ====================

    @Test
    void deleteAllById_multipleIds_deletesAll() {
        try (var factory = new MemrisRepositoryFactory()) {
            TestEntityRepository repo = factory.createJPARepository(TestEntityRepository.class);

            // Save 5 entities
            TestEntity e1 = new TestEntity();
            e1.id = 1;
            e1.name = "Alice";
            TestEntity e2 = new TestEntity();
            e2.id = 2;
            e2.name = "Bob";
            TestEntity e3 = new TestEntity();
            e3.id = 3;
            e3.name = "Charlie";
            TestEntity e4 = new TestEntity();
            e4.id = 4;
            e4.name = "David";
            TestEntity e5 = new TestEntity();
            e5.id = 5;
            e5.name = "Eve";

            repo.saveAll(List.of(e1, e2, e3, e4, e5));
            assertThat(repo.count()).isEqualTo(5);

            // Delete entities with IDs 1, 2, 4
            repo.deleteAllById(List.of(1, 2, 4));

            // Verify deleted
            assertThat(repo.count()).isEqualTo(2);
            assertThat(repo.findById(1)).isEmpty();
            assertThat(repo.findById(2)).isEmpty();
            assertThat(repo.findById(3)).isPresent().hasValueSatisfying(e -> assertThat(e.name).isEqualTo("Charlie"));
            assertThat(repo.findById(4)).isEmpty();
            assertThat(repo.findById(5)).isPresent().hasValueSatisfying(e -> assertThat(e.name).isEqualTo("Eve"));
        }
    }

    @Test
    void deleteAllById_emptyList_doesNothing() {
        try (var factory = new MemrisRepositoryFactory()) {
            TestEntityRepository repo = factory.createJPARepository(TestEntityRepository.class);

            TestEntity e = new TestEntity();
            e.id = 1;
            e.name = "Test";
            repo.save(e);

            // Delete empty list - should not throw
            repo.deleteAllById(List.of());

            assertThat(repo.count()).isEqualTo(1);
            assertThat(repo.findById(1)).isPresent();
        }
    }

    @Test
    void deleteAllById_singleId_deletesOne() {
        try (var factory = new MemrisRepositoryFactory()) {
            TestEntityRepository repo = factory.createJPARepository(TestEntityRepository.class);

            TestEntity e1 = new TestEntity();
            e1.id = 1;
            e1.name = "Alice";
            TestEntity e2 = new TestEntity();
            e2.id = 2;
            e2.name = "Bob";

            repo.saveAll(List.of(e1, e2));

            // Delete single ID
            repo.deleteAllById(List.of(1));

            assertThat(repo.count()).isEqualTo(1);
            assertThat(repo.findById(1)).isEmpty();
            assertThat(repo.findById(2)).isPresent().hasValueSatisfying(e -> assertThat(e.name).isEqualTo("Bob"));
        }
    }

    @Test
    void deleteAllById_nonExistentIds_doesNothing() {
        try (var factory = new MemrisRepositoryFactory()) {
            TestEntityRepository repo = factory.createJPARepository(TestEntityRepository.class);

            TestEntity e = new TestEntity();
            e.id = 1;
            e.name = "Test";
            repo.save(e);

            // Delete non-existent IDs - should not throw (JPA spec)
            repo.deleteAllById(List.of(999, 1000));

            assertThat(repo.count()).isEqualTo(1);
            assertThat(repo.findById(1)).isPresent();
        }
    }

    @Test
    void deleteAllById_mixedExistence_deletesOnlyExisting() {
        try (var factory = new MemrisRepositoryFactory()) {
            TestEntityRepository repo = factory.createJPARepository(TestEntityRepository.class);

            TestEntity e1 = new TestEntity();
            e1.id = 1;
            e1.name = "Alice";
            TestEntity e2 = new TestEntity();
            e2.id = 2;
            e2.name = "Bob";

            repo.saveAll(List.of(e1, e2));

            // Delete mix of existing and non-existent IDs
            repo.deleteAllById(List.of(1, 999, 2, 1000));

            assertThat(repo.count()).isZero();
        }
    }

    // ==================== findAllById Tests ====================

    @Test
    void findAllById_multipleIds_returnsAll() {
        try (var factory = new MemrisRepositoryFactory()) {
            TestEntityRepository repo = factory.createJPARepository(TestEntityRepository.class);

            // Save 5 entities
            TestEntity e1 = new TestEntity();
            e1.id = 1;
            e1.name = "Alice";
            TestEntity e2 = new TestEntity();
            e2.id = 2;
            e2.name = "Bob";
            TestEntity e3 = new TestEntity();
            e3.id = 3;
            e3.name = "Charlie";
            TestEntity e4 = new TestEntity();
            e4.id = 4;
            e4.name = "David";
            TestEntity e5 = new TestEntity();
            e5.id = 5;
            e5.name = "Eve";

            repo.saveAll(List.of(e1, e2, e3, e4, e5));

            // Find entities with IDs 1, 2, 4
            List<TestEntity> found = repo.findAllById(List.of(1, 2, 4));

            assertThat(found).hasSize(3);
            assertThat(found).extracting(e -> e.id).containsExactlyInAnyOrder(1, 2, 4);
            assertThat(found).extracting(e -> e.name).containsExactlyInAnyOrder("Alice", "Bob", "David");
        }
    }

    @Test
    void findAllById_emptyList_returnsEmpty() {
        try (var factory = new MemrisRepositoryFactory()) {
            TestEntityRepository repo = factory.createJPARepository(TestEntityRepository.class);

            List<TestEntity> found = repo.findAllById(List.of());

            assertThat(found).isEmpty();
        }
    }

    @Test
    void findAllById_singleId_returnsOne() {
        try (var factory = new MemrisRepositoryFactory()) {
            TestEntityRepository repo = factory.createJPARepository(TestEntityRepository.class);

            TestEntity e = new TestEntity();
            e.id = 42;
            e.name = "Test";
            repo.save(e);

            List<TestEntity> found = repo.findAllById(List.of(42));

            assertThat(found).hasSize(1);
            assertThat(found.get(0).id).isEqualTo(42);
            assertThat(found.get(0).name).isEqualTo("Test");
        }
    }

    @Test
    void findAllById_nonExistentIds_returnsEmpty() {
        try (var factory = new MemrisRepositoryFactory()) {
            TestEntityRepository repo = factory.createJPARepository(TestEntityRepository.class);

            List<TestEntity> found = repo.findAllById(List.of(999, 1000));

            assertThat(found).isEmpty();
        }
    }

    @Test
    void findAllById_mixedExistence_returnsOnlyExisting() {
        try (var factory = new MemrisRepositoryFactory()) {
            TestEntityRepository repo = factory.createJPARepository(TestEntityRepository.class);

            TestEntity e1 = new TestEntity();
            e1.id = 1;
            e1.name = "Alice";
            TestEntity e2 = new TestEntity();
            e2.id = 2;
            e2.name = "Bob";

            repo.saveAll(List.of(e1, e2));

            // Find mix of existing and non-existent IDs
            List<TestEntity> found = repo.findAllById(List.of(1, 999, 2, 1000));

            assertThat(found).hasSize(2);
            assertThat(found).extracting(e -> e.id).containsExactlyInAnyOrder(1, 2);
        }
    }

    // ==================== deleteAll Tests ====================

    @Test
    void deleteAll_deletesAllEntities() {
        try (var factory = new MemrisRepositoryFactory()) {
            TestEntityRepository repo = factory.createJPARepository(TestEntityRepository.class);

            // Save 5 entities
            TestEntity e1 = new TestEntity();
            e1.id = 1;
            e1.name = "Alice";
            TestEntity e2 = new TestEntity();
            e2.id = 2;
            e2.name = "Bob";
            TestEntity e3 = new TestEntity();
            e3.id = 3;
            e3.name = "Charlie";
            TestEntity e4 = new TestEntity();
            e4.id = 4;
            e4.name = "David";
            TestEntity e5 = new TestEntity();
            e5.id = 5;
            e5.name = "Eve";

            repo.saveAll(List.of(e1, e2, e3, e4, e5));
            assertThat(repo.count()).isEqualTo(5);

            // Delete all
            repo.deleteAll();

            assertThat(repo.count()).isZero();
            assertThat(repo.findById(1)).isEmpty();
            assertThat(repo.findById(2)).isEmpty();
            assertThat(repo.findById(3)).isEmpty();
            assertThat(repo.findById(4)).isEmpty();
            assertThat(repo.findById(5)).isEmpty();
        }
    }

    @Test
    void deleteAll_emptyRepository_doesNothing() {
        try (var factory = new MemrisRepositoryFactory()) {
            TestEntityRepository repo = factory.createJPARepository(TestEntityRepository.class);

            assertThat(repo.count()).isZero();

            // Delete all from empty repo - should not throw
            repo.deleteAll();

            assertThat(repo.count()).isZero();
        }
    }

    // Test entity class
    static class TestEntity {
        @jakarta.persistence.Id
        @GeneratedValue(strategy = GenerationType.IDENTITY)
        Integer id;
        String name;

        TestEntity() {}
    }
}
