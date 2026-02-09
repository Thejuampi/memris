package io.memris.runtime;

import io.memris.core.Entity;
import io.memris.core.GeneratedValue;
import io.memris.core.Id;
import io.memris.core.Index;
import io.memris.core.Indexes;
import io.memris.core.MemrisArena;
import io.memris.repository.MemrisRepository;
import io.memris.repository.MemrisRepositoryFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CompositeIndexRuntimeTest {

    private MemrisRepositoryFactory factory;
    private MemrisArena arena;

    @BeforeEach
    void setUp() {
        factory = new MemrisRepositoryFactory();
        arena = factory.createArena();
    }

    @AfterEach
    void tearDown() throws Exception {
        arena.close();
        factory.close();
    }

    @Test
    void shouldQueryByCompositeHashIndex() {
        var repository = arena.createRepository(CompositeIndexedRepository.class);
        repository.save(new CompositeIndexedRecord("us", 10, 11));
        repository.save(new CompositeIndexedRecord("us", 20, 22));
        repository.save(new CompositeIndexedRecord("eu", 20, 33));

        var rows = repository.findByRegionAndCode("us", 20);

        assertThat(rows).extracting(row -> row.score).containsExactly(22);
    }

    @Test
    void shouldQueryByCompositeRangePrefixWithTrailingRange() {
        var repository = arena.createRepository(CompositeIndexedRepository.class);
        repository.save(new CompositeIndexedRecord("us", 10, 5));
        repository.save(new CompositeIndexedRecord("us", 11, 15));
        repository.save(new CompositeIndexedRecord("us", 12, 25));
        repository.save(new CompositeIndexedRecord("eu", 99, 50));

        var rows = repository.findByRegionAndScoreGreaterThan("us", 10);

        assertThat(rows).extracting(row -> row.code).containsExactlyInAnyOrder(11, 12);
    }

    @Test
    void shouldEvaluateExistsWithCompositeHashConditions() {
        var repository = arena.createRepository(CompositeIndexedRepository.class);
        repository.save(new CompositeIndexedRecord("us", 10, 11));
        repository.save(new CompositeIndexedRecord("us", 20, 22));

        var actual = new ExistsPair(
                repository.existsByRegionAndCode("us", 20),
                repository.existsByRegionAndCode("eu", 20));

        assertThat(actual).usingRecursiveComparison().isEqualTo(new ExistsPair(true, false));
    }

    @Test
    void shouldEvaluateCountWithCompositeHashConditions() {
        var repository = arena.createRepository(CompositeIndexedRepository.class);
        repository.save(new CompositeIndexedRecord("us", 20, 11));
        repository.save(new CompositeIndexedRecord("us", 20, 22));
        repository.save(new CompositeIndexedRecord("eu", 20, 33));

        var actual = new CountPair(
                repository.countByRegionAndCode("us", 20),
                repository.countByRegionAndCode("apac", 20));

        assertThat(actual).usingRecursiveComparison().isEqualTo(new CountPair(2L, 0L));
    }

    @Entity
    @Indexes({
            @Index(name = "idx_region_code", fields = { "region", "code" }, type = Index.IndexType.HASH),
            @Index(name = "idx_region_score", fields = { "region", "score" }, type = Index.IndexType.BTREE)
    })
    public static class CompositeIndexedRecord {
        @Id
        @GeneratedValue
        public Long id;
        public String region;
        public int code;
        public int score;

        public CompositeIndexedRecord() {
        }

        public CompositeIndexedRecord(String region, int code, int score) {
            this.region = region;
            this.code = code;
            this.score = score;
        }
    }

    public interface CompositeIndexedRepository extends MemrisRepository<CompositeIndexedRecord> {
        CompositeIndexedRecord save(CompositeIndexedRecord record);

        List<CompositeIndexedRecord> findByRegionAndCode(String region, int code);

        List<CompositeIndexedRecord> findByRegionAndScoreGreaterThan(String region, int score);

        boolean existsByRegionAndCode(String region, int code);

        long countByRegionAndCode(String region, int code);
    }

    private record ExistsPair(boolean presentMatch, boolean missingMatch) {
    }

    private record CountPair(long matched, long missing) {
    }
}
