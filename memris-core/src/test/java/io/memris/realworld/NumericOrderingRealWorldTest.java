package io.memris.realworld;



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

class NumericOrderingRealWorldTest {
    private MemrisRepositoryFactory factory;
    private MemrisArena arena;

    @BeforeEach
    void setUp() {
        factory = new MemrisRepositoryFactory(RealWorldSupport.configuration().build());
        arena = factory.createArena();
    }

    @AfterEach
    void tearDown() throws Exception {
        factory.close();
    }

    @Test
    void shouldOrderPrimitiveExtremesAndPreserveRowTieBreaks() {
        var repo = arena.createRepository(PrimitiveOrderRepository.class);
        repo.save(new PrimitiveOrderEntity("min", Integer.MIN_VALUE, Long.MIN_VALUE));
        repo.save(new PrimitiveOrderEntity("zero-early", 0, 0L));
        repo.save(new PrimitiveOrderEntity("max", Integer.MAX_VALUE, Long.MAX_VALUE));
        repo.save(new PrimitiveOrderEntity("negative", -1, -1L));
        repo.save(new PrimitiveOrderEntity("zero-late", 0, 0L));
        repo.save(new PrimitiveOrderEntity("positive", 1, 1L));

        var ascending = new String[] { "min", "negative", "zero-early", "zero-late", "positive", "max" };
        var descending = new String[] { "max", "positive", "zero-late", "zero-early", "negative", "min" };

        assertThat(repo.findByOrderByRankAsc()).extracting(entity -> entity.name).containsExactly(ascending);
        assertThat(repo.findByOrderByRankDesc()).extracting(entity -> entity.name).containsExactly(descending);
        assertThat(repo.findByOrderByScoreAsc()).extracting(entity -> entity.name).containsExactly(ascending);
        assertThat(repo.findByOrderByScoreDesc()).extracting(entity -> entity.name).containsExactly(descending);
    }

    @Test
    void shouldSelectPrimitiveTopKAcrossTheFullNumericRange() {
        var repo = arena.createRepository(PrimitiveOrderRepository.class);
        repo.save(new PrimitiveOrderEntity("max", Integer.MAX_VALUE, Long.MAX_VALUE));
        for (int value = -32; value < 32; value++) {
            repo.save(new PrimitiveOrderEntity("value-" + value, value, value));
        }
        repo.save(new PrimitiveOrderEntity("min", Integer.MIN_VALUE, Long.MIN_VALUE));

        assertThat(repo.findTop5ByOrderByRankAsc()).extracting(entity -> entity.rank)
                .containsExactly(Integer.MIN_VALUE, -32, -31, -30, -29);
        assertThat(repo.findTop5ByOrderByRankDesc()).extracting(entity -> entity.rank)
                .containsExactly(Integer.MAX_VALUE, 31, 30, 29, 28);
        assertThat(repo.findTop5ByOrderByScoreAsc()).extracting(entity -> entity.score)
                .containsExactly(Long.MIN_VALUE, -32L, -31L, -30L, -29L);
        assertThat(repo.findTop5ByOrderByScoreDesc()).extracting(entity -> entity.score)
                .containsExactly(Long.MAX_VALUE, 31L, 30L, 29L, 28L);
    }

    @Entity
    public static class PrimitiveOrderEntity {
        @Id
        @GeneratedValue
        public Long id;
        public String name;
        public int rank;
        public long score;

        public PrimitiveOrderEntity() {
        }

        public PrimitiveOrderEntity(String name, int rank, long score) {
            this.name = name;
            this.rank = rank;
            this.score = score;
        }
    }

    public interface PrimitiveOrderRepository extends MemrisRepository<PrimitiveOrderEntity> {
        PrimitiveOrderEntity save(PrimitiveOrderEntity entity);

        List<PrimitiveOrderEntity> findByOrderByRankAsc();

        List<PrimitiveOrderEntity> findByOrderByRankDesc();

        List<PrimitiveOrderEntity> findByOrderByScoreAsc();

        List<PrimitiveOrderEntity> findByOrderByScoreDesc();

        List<PrimitiveOrderEntity> findTop5ByOrderByRankAsc();

        List<PrimitiveOrderEntity> findTop5ByOrderByRankDesc();

        List<PrimitiveOrderEntity> findTop5ByOrderByScoreAsc();

        List<PrimitiveOrderEntity> findTop5ByOrderByScoreDesc();
    }
}
