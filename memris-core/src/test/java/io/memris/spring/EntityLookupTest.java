package io.memris.spring;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;

import org.junit.jupiter.api.Test;

/**
 * TDD tests for single entity lookup operations.
 * Target: O(1) hash index lookup, 10M lookups < 100ms
 */
class EntityLookupTest {

    // Repository interface for JPA-style repository creation
    interface TestEntityRepository extends MemrisRepository<TestEntity> {
        void save(TestEntity entity);
        Optional<TestEntity> findById(Integer id);
        boolean existsById(Integer id);
        void deleteById(Integer id);
        long count();
    }

    @Test
    void findById_existingEntity_returnsOptional() {
        try (var factory = new MemrisRepositoryFactory()) {
            TestEntityRepository repo = factory.createJPARepository(TestEntityRepository.class);

            // Save an entity
            TestEntity entity = new TestEntity();
            entity.id = 100;
            entity.name = "Alice";
            repo.save(entity);

            // Find by ID should return Optional with the entity
            Optional<TestEntity> found = repo.findById(100);
            assertThat(found).isPresent();
            assertThat(found.get().id).isEqualTo(100);
            assertThat(found.get().name).isEqualTo("Alice");
        }
    }

    @Test
    void findById_notFound_returnsEmpty() {
        try (var factory = new MemrisRepositoryFactory()) {
            TestEntityRepository repo = factory.createJPARepository(TestEntityRepository.class);

            // Try to find non-existent ID
            Optional<TestEntity> found = repo.findById(999);
            assertThat(found).isEmpty();
        }
    }

    @Test
    void findById_multipleEntities_findsCorrectOne() {
        try (var factory = new MemrisRepositoryFactory()) {
            TestEntityRepository repo = factory.createJPARepository(TestEntityRepository.class);

            TestEntity e1 = new TestEntity();
            e1.id = 1;
            e1.name = "Alice";
            repo.save(e1);

            TestEntity e2 = new TestEntity();
            e2.id = 2;
            e2.name = "Bob";
            repo.save(e2);

            TestEntity e3 = new TestEntity();
            e3.id = 3;
            e3.name = "Charlie";
            repo.save(e3);

            // Find each entity by ID
            assertThat(repo.findById(1)).isPresent().hasValueSatisfying(e -> assertThat(e.name).isEqualTo("Alice"));
            assertThat(repo.findById(2)).isPresent().hasValueSatisfying(e -> assertThat(e.name).isEqualTo("Bob"));
            assertThat(repo.findById(3)).isPresent().hasValueSatisfying(e -> assertThat(e.name).isEqualTo("Charlie"));
        }
    }

    @Test
    void existsById_existingEntity_returnsTrue() {
        try (var factory = new MemrisRepositoryFactory()) {
            TestEntityRepository repo = factory.createJPARepository(TestEntityRepository.class);

            TestEntity entity = new TestEntity();
            entity.id = 42;
            entity.name = "Alice";
            repo.save(entity);

            assertThat(repo.existsById(42)).isTrue();
        }
    }

    @Test
    void existsById_notFound_returnsFalse() {
        try (var factory = new MemrisRepositoryFactory()) {
            TestEntityRepository repo = factory.createJPARepository(TestEntityRepository.class);

            assertThat(repo.existsById(999)).isFalse();
        }
    }

    @Test
    void deleteById_existingEntity_removesEntity() {
        try (var factory = new MemrisRepositoryFactory()) {
            TestEntityRepository repo = factory.createJPARepository(TestEntityRepository.class);

            // Save an entity
            TestEntity entity = new TestEntity();
            entity.id = 100;
            entity.name = "Alice";
            repo.save(entity);

            // Delete by ID
            repo.deleteById(100);

            // Verify it's gone
            assertThat(repo.findById(100)).isEmpty();
            assertThat(repo.existsById(100)).isFalse();
        }
    }

    @Test
    void deleteById_notFound_doesNothing() {
        try (var factory = new MemrisRepositoryFactory()) {
            TestEntityRepository repo = factory.createJPARepository(TestEntityRepository.class);

            // Try to delete non-existent ID - should not throw
            repo.deleteById(999);

            // Should still work (no-op)
            assertThat(repo.count()).isZero();
        }
    }

    @Test
    void deleteById_deletesFromHashIndex() {
        try (var factory = new MemrisRepositoryFactory()) {
            TestEntityRepository repo = factory.createJPARepository(TestEntityRepository.class);

            TestEntity entity = new TestEntity();
            entity.id = 42;
            entity.name = "Test";
            repo.save(entity);

            // Verify it exists
            assertThat(repo.existsById(42)).isTrue();

            // Delete
            repo.deleteById(42);

            // Verify it's gone from index
            assertThat(repo.existsById(42)).isFalse();
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
