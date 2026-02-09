package io.memris.runtime;

import io.memris.core.MemrisArena;
import io.memris.repository.MemrisRepositoryFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RepositoryRuntimeFloatDoubleOrderTest {

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
    @DisplayName("should order float values ascending")
    void shouldOrderFloatValuesAscending() {
        var repo = arena.createRepository(FloatTestRepository.class);
        repo.save(new FloatTestEntity(null, 3.5f));
        repo.save(new FloatTestEntity(null, 1.5f));
        repo.save(new FloatTestEntity(null, 2.5f));

        var values = repo.findByOrderByValueAsc().stream().map(entity -> entity.value).toList();

        assertThat(values).containsExactly(1.5f, 2.5f, 3.5f);
    }

    @Test
    @DisplayName("should return top2 float values ascending")
    void shouldReturnTop2FloatValuesAscending() {
        var repo = arena.createRepository(FloatTestRepository.class);
        repo.save(new FloatTestEntity(null, 9.5f));
        repo.save(new FloatTestEntity(null, 1.5f));
        repo.save(new FloatTestEntity(null, 4.5f));
        repo.save(new FloatTestEntity(null, 2.5f));

        var values = repo.findTop2ByOrderByValueAsc().stream().map(entity -> entity.value).toList();

        assertThat(values).containsExactly(1.5f, 2.5f);
    }

    @Test
    @DisplayName("should order double values ascending")
    void shouldOrderDoubleValuesAscending() {
        var repo = arena.createRepository(DoubleTestRepository.class);
        repo.save(new DoubleTestEntity(null, 7.5));
        repo.save(new DoubleTestEntity(null, 3.5));
        repo.save(new DoubleTestEntity(null, 5.5));

        var values = repo.findByOrderByValueAsc().stream().map(entity -> entity.value).toList();

        assertThat(values).containsExactly(3.5, 5.5, 7.5);
    }

    @Test
    @DisplayName("should return top2 double values ascending")
    void shouldReturnTop2DoubleValuesAscending() {
        var repo = arena.createRepository(DoubleTestRepository.class);
        repo.save(new DoubleTestEntity(null, 8.0));
        repo.save(new DoubleTestEntity(null, 2.0));
        repo.save(new DoubleTestEntity(null, 6.0));
        repo.save(new DoubleTestEntity(null, 4.0));

        var values = repo.findTop2ByOrderByValueAsc().stream().map(entity -> entity.value).toList();

        assertThat(values).containsExactly(2.0, 4.0);
    }
}
