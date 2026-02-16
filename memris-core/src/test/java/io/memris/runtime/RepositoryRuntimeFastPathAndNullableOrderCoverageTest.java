package io.memris.runtime;

import io.memris.core.Entity;
import io.memris.core.GeneratedValue;
import io.memris.core.Id;
import io.memris.repository.MemrisArena;
import io.memris.repository.MemrisRepository;
import io.memris.repository.MemrisRepositoryFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class RepositoryRuntimeFastPathAndNullableOrderCoverageTest {

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
    void shouldSortNullableNumericAndStringColumnsIncludingTopK() {
        var repo = arena.createRepository(SortableEntityRepository.class);
        seed(repo);

        assertThat(repo.findByOrderByRankAsc()).extracting(entity -> entity.name)
                .containsExactly("c", "b", "d", "a");
        assertThat(repo.findTop2ByOrderByRankAsc()).extracting(entity -> entity.name)
                .containsExactly("c", "b");
        assertThat(repo.findByOrderByScoreAsc()).extracting(entity -> entity.name)
                .containsExactly("c", "d", "a", "b");
        assertThat(repo.findTop2ByOrderByScoreAsc()).extracting(entity -> entity.name)
                .containsExactly("c", "d");
        assertThat(repo.findByOrderByRatioAsc()).extracting(entity -> entity.name)
                .containsExactly("c", "a", "d", "b");
        assertThat(repo.findByOrderByWeightAsc()).extracting(entity -> entity.name)
                .containsExactly("b", "a", "d", "c");
        assertThat(repo.findByOrderByTagAsc()).extracting(entity -> entity.name)
                .containsExactly("c", "d", "a", "b");
        assertThat(repo.findTop2ByOrderByTagAsc()).extracting(entity -> entity.name)
                .containsExactly("c", "d");

        assertThat(repo.findTop2ByOrderByRankDesc()).hasSize(2);
        assertThat(repo.findTop2ByOrderByScoreDesc()).hasSize(2);
        assertThat(repo.findTop2ByOrderByTagDesc()).hasSize(2);
    }

    @Test
    void shouldExerciseFastExistsAndCountAcrossAndOrGroups() {
        var repo = arena.createRepository(SortableEntityRepository.class);
        seed(repo);

        assertThat(repo.existsByNameAndRank("c", 1)).isTrue();
        assertThat(repo.existsByNameAndRank("ghost", 1)).isFalse();
        assertThat(repo.existsByNameOrRank("ghost", 2)).isTrue();
        assertThat(repo.existsByNameOrRank("ghost", 99)).isFalse();
        assertThat(repo.existsByNameAndRankAndScore("c", 1, 10L)).isTrue();
        assertThat(repo.existsByNameAndRankAndScore("a", 1, 30L)).isFalse();

        assertThat(repo.countByNameAndRank("c", 1)).isEqualTo(1);
        assertThat(repo.countByNameAndRank("ghost", 1)).isZero();
        assertThat(repo.countByNameOrRank("a", 2)).isEqualTo(2);
        assertThat(repo.countByNameOrRank("ghost", 99)).isZero();
    }

    private void seed(SortableEntityRepository repo) {
        repo.save(new SortableEntity("a", null, 30L, 1.5f, 2.2d, "z"));
        repo.save(new SortableEntity("b", 2, null, null, 1.1d, null));
        repo.save(new SortableEntity("c", 1, 10L, 0.5f, null, "a"));
        repo.save(new SortableEntity("d", 3, 20L, 2.0f, 3.3d, "m"));
    }

    @Entity
    public static class SortableEntity {
        @Id
        @GeneratedValue
        public Long id;
        public String name;
        public Integer rank;
        public Long score;
        public Float ratio;
        public Double weight;
        public String tag;

        public SortableEntity() {
        }

        public SortableEntity(String name, Integer rank, Long score, Float ratio, Double weight, String tag) {
            this.name = name;
            this.rank = rank;
            this.score = score;
            this.ratio = ratio;
            this.weight = weight;
            this.tag = tag;
        }
    }

    public interface SortableEntityRepository extends MemrisRepository<SortableEntity> {
        SortableEntity save(SortableEntity entity);

        List<SortableEntity> findByOrderByRankAsc();

        List<SortableEntity> findTop2ByOrderByRankAsc();

        List<SortableEntity> findTop2ByOrderByRankDesc();

        List<SortableEntity> findByOrderByScoreAsc();

        List<SortableEntity> findTop2ByOrderByScoreAsc();

        List<SortableEntity> findTop2ByOrderByScoreDesc();

        List<SortableEntity> findByOrderByRatioAsc();

        List<SortableEntity> findByOrderByWeightAsc();

        List<SortableEntity> findByOrderByTagAsc();

        List<SortableEntity> findTop2ByOrderByTagAsc();

        List<SortableEntity> findTop2ByOrderByTagDesc();

        boolean existsByNameAndRank(String name, Integer rank);

        boolean existsByNameOrRank(String name, Integer rank);

        boolean existsByNameAndRankAndScore(String name, Integer rank, Long score);

        long countByNameAndRank(String name, Integer rank);

        long countByNameOrRank(String name, Integer rank);
    }
}
