package io.memris.index;

import io.memris.core.MemrisConfiguration;
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.runtime.TestEntity;
import io.memris.runtime.TestEntityRepository;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TombstoneValidatorPrefixSuffixTest {

    @Test
    void prefixIndexShouldNotReturnDeletedRows() {
        var config = MemrisConfiguration.builder()
                .enablePrefixIndex(true)
                .build();

        try (var factory = new MemrisRepositoryFactory(config);
             var arena = factory.createArena()) {
            var repo = arena.createRepository(TestEntityRepository.class);

            var e1 = repo.save(new TestEntity(null, "Alpha", 10));
            var e2 = repo.save(new TestEntity(null, "AlphaBeta", 20));
            var e3 = repo.save(new TestEntity(null, "AlphaGamma", 30));

            List<TestEntity> beforeDelete = repo.findByNameStartingWith("Alpha");
            assertThat(beforeDelete).hasSize(3);

            repo.delete(e2);

            List<TestEntity> afterDelete = repo.findByNameStartingWith("Alpha");
            assertThat(afterDelete).hasSize(2);
            assertThat(afterDelete.stream().map(e -> e.name).toList())
                    .containsExactlyInAnyOrder("Alpha", "AlphaGamma");
        }
    }

    @Test
    void suffixIndexShouldNotReturnDeletedRows() {
        var config = MemrisConfiguration.builder()
                .enablePrefixIndex(true)
                .build();

        try (var factory = new MemrisRepositoryFactory(config);
             var arena = factory.createArena()) {
            var repo = arena.createRepository(TestEntityRepository.class);

            var e1 = repo.save(new TestEntity(null, "TestSmith", 10));
            var e2 = repo.save(new TestEntity(null, "AnotherSmith", 20));
            var e3 = repo.save(new TestEntity(null, "TestJones", 30));

            List<TestEntity> beforeDelete = repo.findByNameEndingWith("Smith");
            assertThat(beforeDelete).hasSize(2);

            repo.delete(e1);

            List<TestEntity> afterDelete = repo.findByNameEndingWith("Smith");
            assertThat(afterDelete).hasSize(1);
            assertThat(afterDelete.get(0).name).isEqualTo("AnotherSmith");
        }
    }
}
