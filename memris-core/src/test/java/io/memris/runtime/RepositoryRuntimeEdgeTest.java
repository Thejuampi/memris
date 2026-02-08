package io.memris.runtime;

import io.memris.repository.MemrisRepositoryFactory;
import io.memris.repository.MemrisRepository;
import io.memris.core.MemrisArena;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class RepositoryRuntimeEdgeTest {

    private MemrisRepositoryFactory factory;
    private MemrisArena arena;
    private TestEntityRepository repo;

    // Interface with looser types to allow testing mismatched IDs
    public interface MismatchedRepository extends MemrisRepository<TestEntity> {
        Optional<TestEntity> findById(Object id);

        boolean existsById(Object id);

        void deleteById(Object id);
    }

    @BeforeEach
    void setUp() {
        factory = new MemrisRepositoryFactory();
        arena = factory.createArena();
        repo = arena.createRepository(TestEntityRepository.class);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (factory != null) {
            factory.close();
        }
    }

    @Test
    @DisplayName("findById should safely handle null ID")
    void findByIdShouldHandleNull() {
        // Needs the mismatched repo interface to pass null/WRONG TYPE without compiler
        // error
        MismatchedRepository laxRepo = arena.createRepository(MismatchedRepository.class);

        Optional<TestEntity> result = laxRepo.findById(null);
        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("findById should return empty for mismatched ID type")
    void findByIdShouldHandleMismatchedType() {
        repo.save(new TestEntity(null, "Test", 25));

        MismatchedRepository laxRepo = arena.createRepository(MismatchedRepository.class);

        // Passing String ID to Long ID entity repo
        Optional<TestEntity> result = laxRepo.findById("some-string-id");
        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("existsById should return false for null ID")
    void existsByIdShouldHandleNull() {
        MismatchedRepository laxRepo = arena.createRepository(MismatchedRepository.class);
        boolean exists = laxRepo.existsById(null);
        assertThat(exists).isFalse();
    }

    @Test
    @DisplayName("existsById should return false for mismatched ID type")
    void existsByIdShouldHandleMismatchedType() {
        repo.save(new TestEntity(null, "Test", 25));

        MismatchedRepository laxRepo = arena.createRepository(MismatchedRepository.class);

        boolean exists = laxRepo.existsById("some-string-id");
        assertThat(exists).isFalse();
    }

    @Test
    @DisplayName("deleteById should ignore mismatched ID type")
    void deleteByIdShouldHandleMismatchedType() {
        TestEntity saved = repo.save(new TestEntity(null, "Test", 25));

        MismatchedRepository laxRepo = arena.createRepository(MismatchedRepository.class);

        // Should not throw and not delete anything
        laxRepo.deleteById("some-string-id");

        assertThat(repo.existsById(saved.id)).isTrue();
    }

    @Test
    @DisplayName("deleteById should safely ignore null ID")
    void deleteByIdShouldHandleNull() {
        repo.save(new TestEntity(null, "Test", 25));

        MismatchedRepository laxRepo = arena.createRepository(MismatchedRepository.class);

        // Should not throw
        laxRepo.deleteById(null);

        assertThat(repo.count()).isEqualTo(1);
    }
}
