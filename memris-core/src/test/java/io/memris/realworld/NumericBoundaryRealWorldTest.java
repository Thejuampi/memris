package io.memris.realworld;

import io.memris.runtime.HeapRuntimeKernel;

import io.memris.core.Entity;
import io.memris.core.Id;
import io.memris.core.Index;
import io.memris.core.MemrisConfiguration;
import io.memris.core.MetadataExtractor;
import io.memris.core.TypeCodes;
import io.memris.query.CompiledQuery;
import io.memris.query.LogicalQuery;
import io.memris.repository.MemrisArena;
import io.memris.repository.MemrisRepository;
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.runtime.codegen.RuntimeExecutorGenerator;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class NumericBoundaryRealWorldTest {
    private static final List<Integer> INT_VALUES = List.of(
            Integer.MIN_VALUE, Integer.MIN_VALUE + 1, -1, 0, 1, Integer.MAX_VALUE - 1, Integer.MAX_VALUE);
    private static final List<Long> LONG_VALUES = List.of(
            Long.MIN_VALUE, Long.MIN_VALUE + 1, -1L, 0L, 1L, Long.MAX_VALUE - 1, Long.MAX_VALUE);

    @ParameterizedTest(name = "int boundaries: {0}")
    @MethodSource("executionModes")
    void shouldPreserveIntBoundaries(ExecutionMode mode) {
        try (var factory = new MemrisRepositoryFactory(configuration()); var arena = factory.createArena()) {
            var repo = populate(arena);
            RangeQueries<Integer> queries = switch (mode) {
                case SCAN -> new RangeQueries<>(repo::findByIntValueGreaterThan, repo::findByIntValueGreaterThanEqual,
                        repo::findByIntValueLessThan, repo::findByIntValueLessThanEqual, repo::findByIntValueBetween);
                case RANGE_INDEX -> new RangeQueries<>(repo::findByIndexedIntGreaterThan, repo::findByIndexedIntGreaterThanEqual,
                        repo::findByIndexedIntLessThan, repo::findByIndexedIntLessThanEqual, repo::findByIndexedIntBetween);
                case INDEXED_RESIDUAL -> new RangeQueries<>(
                        value -> repo.findByBucketAndIntValueGreaterThan("all", value),
                        value -> repo.findByBucketAndIntValueGreaterThanEqual("all", value),
                        value -> repo.findByBucketAndIntValueLessThan("all", value),
                        value -> repo.findByBucketAndIntValueLessThanEqual("all", value),
                        (min, max) -> repo.findByBucketAndIntValueBetween("all", min, max));
            };
            assertComparisons(INT_VALUES, queries);
        }
    }

    @ParameterizedTest(name = "long boundaries: {0}")
    @MethodSource("executionModes")
    void shouldPreserveLongBoundaries(ExecutionMode mode) {
        try (var factory = new MemrisRepositoryFactory(configuration()); var arena = factory.createArena()) {
            var repo = populate(arena);
            RangeQueries<Long> queries = switch (mode) {
                case SCAN -> new RangeQueries<>(repo::findByLongValueGreaterThan, repo::findByLongValueGreaterThanEqual,
                        repo::findByLongValueLessThan, repo::findByLongValueLessThanEqual, repo::findByLongValueBetween);
                case RANGE_INDEX -> new RangeQueries<>(repo::findByIndexedLongGreaterThan, repo::findByIndexedLongGreaterThanEqual,
                        repo::findByIndexedLongLessThan, repo::findByIndexedLongLessThanEqual, repo::findByIndexedLongBetween);
                case INDEXED_RESIDUAL -> new RangeQueries<>(
                        value -> repo.findByBucketAndLongValueGreaterThan("all", value),
                        value -> repo.findByBucketAndLongValueGreaterThanEqual("all", value),
                        value -> repo.findByBucketAndLongValueLessThan("all", value),
                        value -> repo.findByBucketAndLongValueLessThanEqual("all", value),
                        (min, max) -> repo.findByBucketAndLongValueBetween("all", min, max));
            };
            assertComparisons(LONG_VALUES, queries);
        }
    }

    @Test
    void shouldPreserveCountAndExistsBoundaries() {
        try (var factory = new MemrisRepositoryFactory(configuration()); var arena = factory.createArena()) {
            var repo = populate(arena);

            assertThat(repo.countByIntValueGreaterThan(Integer.MAX_VALUE)).isZero();
            assertThat(repo.countByIntValueLessThan(Integer.MIN_VALUE)).isZero();
            assertThat(repo.countByLongValueGreaterThan(Long.MAX_VALUE)).isZero();
            assertThat(repo.countByLongValueLessThan(Long.MIN_VALUE)).isZero();
            assertThat(repo.existsByIntValueGreaterThan(Integer.MAX_VALUE)).isFalse();
            assertThat(repo.existsByIntValueLessThan(Integer.MIN_VALUE)).isFalse();
            assertThat(repo.existsByLongValueGreaterThan(Long.MAX_VALUE)).isFalse();
            assertThat(repo.existsByLongValueLessThan(Long.MIN_VALUE)).isFalse();

            assertThat(repo.countByIntValueGreaterThan(Integer.MAX_VALUE - 1)).isEqualTo(1);
            assertThat(repo.countByLongValueLessThan(Long.MIN_VALUE + 1)).isEqualTo(1);
            assertThat(repo.existsByIntValueLessThan(Integer.MIN_VALUE + 1)).isTrue();
            assertThat(repo.existsByLongValueGreaterThan(Long.MAX_VALUE - 1)).isTrue();
        }
    }

    @Test
    void shouldPreserveKernelBoundaries() {
        var config = configuration();
        try (var factory = new MemrisRepositoryFactory(config); var arena = factory.createArena()) {
            populate(arena);
            var table = arena.getTable(NumericEntity.class);
            var kernel = new HeapRuntimeKernel(table, new RuntimeExecutorGenerator(config));
            var metadata = MetadataExtractor.extractEntityMetadata(NumericEntity.class);

            assertKernelComparisons(kernel, metadata.resolvePropertyPosition("intValue"), TypeCodes.TYPE_INT, INT_VALUES);
            assertKernelComparisons(kernel, metadata.resolvePropertyPosition("longValue"), TypeCodes.TYPE_LONG, LONG_VALUES);
        }
    }

    private static Stream<ExecutionMode> executionModes() {
        return Stream.of(ExecutionMode.values());
    }

    private static MemrisConfiguration configuration() {
        return RealWorldSupport.configuration().pageSize(64).maxPages(4).initialPages(1).build();
    }

    private static NumericRepository populate(MemrisArena arena) {
        var repo = arena.createRepository(NumericRepository.class);
        for (int i = 0; i < INT_VALUES.size(); i++) {
            repo.save(new NumericEntity(i + 1L, INT_VALUES.get(i), LONG_VALUES.get(i)));
        }
        return repo;
    }

    private static <T extends Comparable<T>> void assertComparisons(List<T> values, RangeQueries<T> queries) {
        for (var bound : values) {
            assertMatches(queries.gt().apply(bound), values, value -> value.compareTo(bound) > 0, "GT " + bound);
            assertMatches(queries.gte().apply(bound), values, value -> value.compareTo(bound) >= 0, "GTE " + bound);
            assertMatches(queries.lt().apply(bound), values, value -> value.compareTo(bound) < 0, "LT " + bound);
            assertMatches(queries.lte().apply(bound), values, value -> value.compareTo(bound) <= 0, "LTE " + bound);
            assertMatches(queries.between().apply(bound, bound), values, value -> value.compareTo(bound) == 0,
                    "BETWEEN " + bound + " AND " + bound);
        }
        var min = values.getFirst();
        var max = values.getLast();
        assertMatches(queries.between().apply(min, max), values, value -> true, "BETWEEN min AND max");
        assertThat(queries.between().apply(max, min)).as("inverted BETWEEN").isEmpty();
    }

    private static <T> void assertMatches(List<NumericEntity> actual, List<T> values, Predicate<T> matches, String description) {
        var expectedIds = IntStream.range(0, values.size()).filter(i -> matches.test(values.get(i)))
                .mapToObj(i -> i + 1L).toList();
        assertThat(actual).as(description).extracting(entity -> entity.id).containsExactlyInAnyOrderElementsOf(expectedIds);
    }

    private static <T extends Comparable<T>> void assertKernelComparisons(HeapRuntimeKernel kernel, int columnIndex,
            byte typeCode, List<T> values) {
        for (var operator : List.of(LogicalQuery.Operator.GT, LogicalQuery.Operator.GTE,
                LogicalQuery.Operator.LT, LogicalQuery.Operator.LTE)) {
            var condition = CompiledQuery.CompiledCondition.of(columnIndex, typeCode, operator, 0);
            for (var bound : values) {
                int[] expected = IntStream.range(0, values.size()).filter(i -> {
                    int comparison = values.get(i).compareTo(bound);
                    return switch (operator) {
                        case GT -> comparison > 0;
                        case GTE -> comparison >= 0;
                        case LT -> comparison < 0;
                        case LTE -> comparison <= 0;
                        default -> throw new AssertionError(operator);
                    };
                }).toArray();
                assertThat(kernel.executeCondition(condition, new Object[] { bound }).toIntArray())
                        .as("kernel type=%s %s %s", typeCode, operator, bound).containsExactlyInAnyOrder(expected);
            }
        }
    }

    private enum ExecutionMode {
        SCAN, RANGE_INDEX, INDEXED_RESIDUAL
    }

    private record RangeQueries<T>(Function<T, List<NumericEntity>> gt, Function<T, List<NumericEntity>> gte,
            Function<T, List<NumericEntity>> lt, Function<T, List<NumericEntity>> lte,
            BiFunction<T, T, List<NumericEntity>> between) {
    }

    @Entity
    public static class NumericEntity {
        @Id
        public Long id;
        @Index(type = Index.IndexType.HASH)
        public String bucket;
        public int intValue;
        @Index(type = Index.IndexType.BTREE)
        public int indexedInt;
        public long longValue;
        @Index(type = Index.IndexType.BTREE)
        public long indexedLong;

        public NumericEntity() {
        }

        public NumericEntity(Long id, int intValue, long longValue) {
            this.id = id;
            this.bucket = "all";
            this.intValue = intValue;
            this.indexedInt = intValue;
            this.longValue = longValue;
            this.indexedLong = longValue;
        }
    }

    public interface NumericRepository extends MemrisRepository<NumericEntity> {
        NumericEntity save(NumericEntity entity);

        List<NumericEntity> findByIntValueGreaterThan(int value);
        List<NumericEntity> findByIntValueGreaterThanEqual(int value);
        List<NumericEntity> findByIntValueLessThan(int value);
        List<NumericEntity> findByIntValueLessThanEqual(int value);
        List<NumericEntity> findByIntValueBetween(int min, int max);

        List<NumericEntity> findByIndexedIntGreaterThan(int value);
        List<NumericEntity> findByIndexedIntGreaterThanEqual(int value);
        List<NumericEntity> findByIndexedIntLessThan(int value);
        List<NumericEntity> findByIndexedIntLessThanEqual(int value);
        List<NumericEntity> findByIndexedIntBetween(int min, int max);

        List<NumericEntity> findByLongValueGreaterThan(long value);
        List<NumericEntity> findByLongValueGreaterThanEqual(long value);
        List<NumericEntity> findByLongValueLessThan(long value);
        List<NumericEntity> findByLongValueLessThanEqual(long value);
        List<NumericEntity> findByLongValueBetween(long min, long max);

        List<NumericEntity> findByIndexedLongGreaterThan(long value);
        List<NumericEntity> findByIndexedLongGreaterThanEqual(long value);
        List<NumericEntity> findByIndexedLongLessThan(long value);
        List<NumericEntity> findByIndexedLongLessThanEqual(long value);
        List<NumericEntity> findByIndexedLongBetween(long min, long max);

        List<NumericEntity> findByBucketAndIntValueGreaterThan(String bucket, int value);
        List<NumericEntity> findByBucketAndIntValueGreaterThanEqual(String bucket, int value);
        List<NumericEntity> findByBucketAndIntValueLessThan(String bucket, int value);
        List<NumericEntity> findByBucketAndIntValueLessThanEqual(String bucket, int value);
        List<NumericEntity> findByBucketAndIntValueBetween(String bucket, int min, int max);

        List<NumericEntity> findByBucketAndLongValueGreaterThan(String bucket, long value);
        List<NumericEntity> findByBucketAndLongValueGreaterThanEqual(String bucket, long value);
        List<NumericEntity> findByBucketAndLongValueLessThan(String bucket, long value);
        List<NumericEntity> findByBucketAndLongValueLessThanEqual(String bucket, long value);
        List<NumericEntity> findByBucketAndLongValueBetween(String bucket, long min, long max);

        long countByIntValueGreaterThan(int value);
        long countByIntValueLessThan(int value);
        long countByLongValueGreaterThan(long value);
        long countByLongValueLessThan(long value);
        boolean existsByIntValueGreaterThan(int value);
        boolean existsByIntValueLessThan(int value);
        boolean existsByLongValueGreaterThan(long value);
        boolean existsByLongValueLessThan(long value);
    }
}
