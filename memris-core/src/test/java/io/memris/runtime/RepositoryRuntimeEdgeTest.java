package io.memris.runtime;

import io.memris.repository.MemrisRepositoryFactory;
import io.memris.repository.MemrisRepository;
import io.memris.core.MemrisArena;
import io.memris.core.Modifying;
import io.memris.core.Param;
import io.memris.core.Query;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    public interface PrimitiveUpdateRepository extends MemrisRepository<TestEntity> {
        @Modifying
        @Query("update TestEntity t set t.age = :age where t.id = :id")
        long updateAgeById(@Param("age") Integer age, @Param("id") Long id);
    }

    public interface FindAllByIdRepository extends MemrisRepository<TestEntity> {
        List<TestEntity> findAllById(Iterable<Long> ids);
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
    @DisplayName("findById should throw ClassCastException for mismatched ID type")
    void findByIdShouldHandleMismatchedType() {
        repo.save(new TestEntity(null, "Test", 25));

        MismatchedRepository laxRepo = arena.createRepository(MismatchedRepository.class);

        // Passing String ID to Long ID entity repo is a programmer error
        assertThatThrownBy(() -> laxRepo.findById("some-string-id"))
                .isInstanceOf(ClassCastException.class);
    }

    @Test
    @DisplayName("existsById should return false for null ID")
    void existsByIdShouldHandleNull() {
        MismatchedRepository laxRepo = arena.createRepository(MismatchedRepository.class);
        boolean exists = laxRepo.existsById(null);
        assertThat(exists).isFalse();
    }

    @Test
    @DisplayName("existsById should throw ClassCastException for mismatched ID type")
    void existsByIdShouldHandleMismatchedType() {
        repo.save(new TestEntity(null, "Test", 25));

        MismatchedRepository laxRepo = arena.createRepository(MismatchedRepository.class);

        // Passing String ID to Long ID entity repo is a programmer error
        assertThatThrownBy(() -> laxRepo.existsById("some-string-id"))
                .isInstanceOf(ClassCastException.class);
    }

    @Test
    @DisplayName("deleteById should throw ClassCastException for mismatched ID type")
    void deleteByIdShouldHandleMismatchedType() {
        TestEntity saved = repo.save(new TestEntity(null, "Test", 25));

        MismatchedRepository laxRepo = arena.createRepository(MismatchedRepository.class);

        // Passing String ID to Long ID entity repo is a programmer error
        assertThatThrownBy(() -> laxRepo.deleteById("some-string-id"))
                .isInstanceOf(ClassCastException.class);

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

    @Test
    @DisplayName("update query should reject null for primitive column")
    void updateQueryShouldRejectNullPrimitiveValue() {
        TestEntity saved = repo.save(new TestEntity(null, "Test", 25));
        PrimitiveUpdateRepository primitiveRepo = arena.createRepository(PrimitiveUpdateRepository.class);

        assertThatThrownBy(() -> primitiveRepo.updateAgeById(null, saved.id))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("findAllById should return matching entities for iterable IDs")
    void findAllByIdShouldReturnMatchingEntities() {
        TestEntity first = repo.save(new TestEntity(null, "A", 20));
        TestEntity second = repo.save(new TestEntity(null, "B", 30));
        repo.save(new TestEntity(null, "C", 40));

        FindAllByIdRepository findAllByIdRepo = arena.createRepository(FindAllByIdRepository.class);
        List<TestEntity> found = findAllByIdRepo.findAllById(List.of(first.id, second.id, 999_999L));

        assertThat(found)
                .extracting(entity -> entity.id)
                .containsExactlyInAnyOrder(first.id, second.id);
    }
}
