package io.memris.spring;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

class IdGenerationTest {

    // Repository interfaces for JPA-style repository creation
    interface IntEntityRepository extends MemrisRepository<IntEntity> {
        void save(IntEntity entity);
        List<IntEntity> findAll();
        long count();
    }

    interface LongEntityRepository extends MemrisRepository<LongEntity> {
        void save(LongEntity entity);
        List<LongEntity> findAll();
        long count();
    }

    interface UuidEntityRepository extends MemrisRepository<UuidEntity> {
        void save(UuidEntity entity);
        List<UuidEntity> findAll();
        long count();
    }

    interface CustomEntityRepository extends MemrisRepository<CustomEntity> {
        void save(CustomEntity entity);
        List<CustomEntity> findAll();
        long count();
    }

    @Test
    void auto_numeric_int_should_generate_incremental_ids() {
        try (var factory = new MemrisRepositoryFactory()) {
            IntEntityRepository repo = factory.createJPARepository(IntEntityRepository.class);

            IntEntity e1 = new IntEntity(null, "Alice");
            IntEntity e2 = new IntEntity(null, "Bob");

            repo.save(e1);
            repo.save(e2);

            assertThat(e1.id).isNotNull().isEqualTo(1);
            assertThat(e2.id).isNotNull().isEqualTo(2);
        }
    }

    @Test
    void auto_numeric_long_should_generate_incremental_ids() {
        try (var factory = new MemrisRepositoryFactory()) {
            LongEntityRepository repo = factory.createJPARepository(LongEntityRepository.class);

            LongEntity e1 = new LongEntity(null, "Alice");
            LongEntity e2 = new LongEntity(null, "Bob");

            repo.save(e1);
            repo.save(e2);

            assertThat(e1.id).isNotNull().isEqualTo(1L);
            assertThat(e2.id).isNotNull().isEqualTo(2L);
        }
    }

    @Test
    void auto_uuid_should_generate_random_uuids() {
        try (var factory = new MemrisRepositoryFactory()) {
            UuidEntityRepository repo = factory.createJPARepository(UuidEntityRepository.class);

            UuidEntity e1 = new UuidEntity(null, "Alice");
            UuidEntity e2 = new UuidEntity(null, "Bob");

            repo.save(e1);
            repo.save(e2);

            assertThat(e1.id).isNotNull().isNotEqualTo(e2.id);
            assertThat(e2.id).isNotNull();
        }
    }

    @Test
    void custom_generator_should_use_provided_implementation() {
        try (var factory = new MemrisRepositoryFactory()) {
            // Register custom generator
            factory.registerIdGenerator("test-generator", new TestIdGenerator());

            CustomEntityRepository repo = factory.createJPARepository(CustomEntityRepository.class);

            CustomEntity e1 = new CustomEntity(null, "Alice");
            CustomEntity e2 = new CustomEntity(null, "Bob");

            repo.save(e1);
            repo.save(e2);

            assertThat(e1.id).isNotNull().isEqualTo("ID-1");
            assertThat(e2.id).isNotNull().isEqualTo("ID-2");
        }
    }

    @Test
    void explicit_id_should_not_be_overwritten() {
        try (var factory = new MemrisRepositoryFactory()) {
            IntEntityRepository repo = factory.createJPARepository(IntEntityRepository.class);

            IntEntity e = new IntEntity(999, "Alice");

            repo.save(e);

            assertThat(e.id).isEqualTo(999);
        }
    }

    @Test
    void save_with_existing_id_should_update() {
        try (var factory = new MemrisRepositoryFactory()) {
            IntEntityRepository repo = factory.createJPARepository(IntEntityRepository.class);

            // Insert with generated ID
            IntEntity e1 = new IntEntity(null, "Alice");
            repo.save(e1);
            assertThat(e1.id).isEqualTo(1);

            // Update with same ID
            IntEntity e1Updated = new IntEntity(1, "Alice Updated");
            repo.save(e1Updated);
            assertThat(e1Updated.id).isEqualTo(1);

            // Verify update worked
            IntEntity result = repo.findAll().get(0);
            assertThat(result.name).isEqualTo("Alice Updated");
        }
    }

    @Test
    void save_with_new_id_should_insert() {
        try (var factory = new MemrisRepositoryFactory()) {
            IntEntityRepository repo = factory.createJPARepository(IntEntityRepository.class);

            // Insert with explicit ID 100
            IntEntity e = new IntEntity(100, "Alice");
            repo.save(e);
            assertThat(e.id).isEqualTo(100);

            // Insert with explicit ID 200
            IntEntity e2 = new IntEntity(200, "Bob");
            repo.save(e2);
            assertThat(e2.id).isEqualTo(200);

            // Verify both exist
            assertThat(repo.count()).isEqualTo(2);
        }
    }

    // Test entity classes
    static class IntEntity {
        @jakarta.persistence.Id
        @GeneratedValue(strategy = GenerationType.IDENTITY)
        Integer id;
        String name;

        IntEntity() {}
        IntEntity(Integer id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    static class LongEntity {
        @jakarta.persistence.Id
        @GeneratedValue(strategy = GenerationType.IDENTITY)
        Long id;
        String name;

        LongEntity() {}
        LongEntity(Long id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    static class UuidEntity {
        @jakarta.persistence.Id
        @GeneratedValue(strategy = GenerationType.UUID)
        UUID id;
        String name;

        UuidEntity() {}
        UuidEntity(UUID id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    static class CustomEntity {
        @jakarta.persistence.Id
        @GeneratedValue(strategy = GenerationType.CUSTOM, generator = "test-generator")
        String id;
        String name;

        CustomEntity() {}
        CustomEntity(String id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    // Custom test generator
    static class TestIdGenerator implements IdGenerator<String> {
        private final AtomicInteger counter = new AtomicInteger(0);

        @Override
        public String generate() {
            return "ID-" + (counter.incrementAndGet());
        }
    }
}
