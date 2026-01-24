package io.memris.spring;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;
import java.util.Optional;

import org.junit.jupiter.api.Test;

/**
 * TDD tests for UUID storage optimization using 2 long columns (128 bits).
 * Instead of storing UUID as String, store as 2 longs (MSB + LSB) for better performance.
 */
class UuidStorageOptimizationTest {

    @Test
    void uuid_id_stored_and_retrieved_correctly() {
        try (var factory = new MemrisRepositoryFactory()) {
            UuidEntityRepository repo = factory.createJPARepository(UuidEntityRepository.class);

            UUID testId = UUID.randomUUID();
            UuidEntity entity = new UuidEntity();
            entity.id = testId;
            entity.name = "Test";

            repo.save(entity);

            UuidEntity found = repo.findById(testId).orElseThrow();
            assertThat(found.id).isEqualTo(testId);
            assertThat(found.name).isEqualTo("Test");
        }
    }

    @Test
    void uuid_field_stored_and_retrieved_correctly() {
        try (var factory = new MemrisRepositoryFactory()) {
            EntityWithUuidFieldRepository repo = factory.createJPARepository(EntityWithUuidFieldRepository.class);

            UUID testUuid = UUID.randomUUID();
            EntityWithUuidField entity = new EntityWithUuidField();
            entity.id = 1;
            entity.myUuid = testUuid;

            repo.save(entity);

            EntityWithUuidField found = repo.findById(1).orElseThrow();
            assertThat(found.myUuid).isEqualTo(testUuid);
        }
    }

    @Test
    void multiple_entities_with_uuid_ids() {
        try (var factory = new MemrisRepositoryFactory()) {
            UuidEntityRepository repo = factory.createJPARepository(UuidEntityRepository.class);

            UUID id1 = UUID.randomUUID();
            UUID id2 = UUID.randomUUID();
            UUID id3 = UUID.randomUUID();

            UuidEntity e1 = new UuidEntity();
            e1.id = id1;
            e1.name = "Alice";
            UuidEntity e2 = new UuidEntity();
            e2.id = id2;
            e2.name = "Bob";
            UuidEntity e3 = new UuidEntity();
            e3.id = id3;
            e3.name = "Charlie";

            repo.saveAll(List.of(e1, e2, e3));

            assertThat(repo.count()).isEqualTo(3);
            assertThat(repo.findById(id1)).isPresent().hasValueSatisfying(e -> assertThat(e.name).isEqualTo("Alice"));
            assertThat(repo.findById(id2)).isPresent().hasValueSatisfying(e -> assertThat(e.name).isEqualTo("Bob"));
            assertThat(repo.findById(id3)).isPresent().hasValueSatisfying(e -> assertThat(e.name).isEqualTo("Charlie"));
        }
    }

    @Test
    void findAllById_with_uuid_ids() {
        try (var factory = new MemrisRepositoryFactory()) {
            UuidEntityRepository repo = factory.createJPARepository(UuidEntityRepository.class);

            UUID id1 = UUID.randomUUID();
            UUID id2 = UUID.randomUUID();

            UuidEntity e1 = new UuidEntity();
            e1.id = id1;
            e1.name = "Alice";
            UuidEntity e2 = new UuidEntity();
            e2.id = id2;
            e2.name = "Bob";

            repo.saveAll(List.of(e1, e2));

            List<UuidEntity> found = repo.findAllById(List.of(id1, id2));
            assertThat(found).hasSize(2);
            assertThat(found).extracting(e -> e.id).containsExactlyInAnyOrder(id1, id2);
        }
    }

    // Test entity classes
    static class UuidEntity {
        @jakarta.persistence.Id
        @GeneratedValue(strategy = GenerationType.UUID)
        UUID id;
        String name;

        UuidEntity() {}
    }

    static class EntityWithUuidField {
        @jakarta.persistence.Id
        Integer id;
        UUID myUuid;

        EntityWithUuidField() {}
    }

    // Repository interfaces for JPA-style creation
    interface UuidEntityRepository extends MemrisRepository<UuidEntity> {
        void save(UuidEntity e);
        void saveAll(List<UuidEntity> entities);
        long count();
        java.util.Optional<UuidEntity> findById(UUID id);
        java.util.List<UuidEntity> findAllById(List<UUID> ids);
    }

    interface EntityWithUuidFieldRepository extends MemrisRepository<EntityWithUuidField> {
        void save(EntityWithUuidField e);
        java.util.Optional<EntityWithUuidField> findById(Integer id);
    }
}
