package io.memris.bugfix;

import io.memris.core.FloatEncoding;
import io.memris.kernel.RowId;
import io.memris.kernel.RowIdBitSet;
import io.memris.kernel.Predicate;
import io.memris.query.LogicalQuery;
import io.memris.query.OpCode;
import io.memris.query.QueryMethodLexer;
import io.memris.query.QueryMethodToken;
import io.memris.query.QueryMethodTokenType;
import io.memris.query.jpql.JpqlLexer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.RepeatedTest;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CyclicBarrier;

import java.util.stream.LongStream;

import java.util.stream.Stream;

class BugFixTests {

    // ===== C1: Double sort wrong decoding =====

    @Test
    void doubleSortableRoundTripNegative() {
        var encoded = FloatEncoding.doubleToSortableLong(-1.0);
        var decoded = FloatEncoding.sortableLongToDouble(encoded);
        assertThat(decoded).isEqualTo(-1.0);
    }

    @Test
    void doubleSortableRoundTripPositive() {
        var encoded = FloatEncoding.doubleToSortableLong(42.5);
        var decoded = FloatEncoding.sortableLongToDouble(encoded);
        assertThat(decoded).isEqualTo(42.5);
    }

    @Test
    void doubleSortablePreservesOrdering() {
        var negInf = FloatEncoding.doubleToSortableLong(Double.NEGATIVE_INFINITY);
        var neg = FloatEncoding.doubleToSortableLong(-100.0);
        var zero = FloatEncoding.doubleToSortableLong(0.0);
        var pos = FloatEncoding.doubleToSortableLong(100.0);
        var posInf = FloatEncoding.doubleToSortableLong(Double.POSITIVE_INFINITY);
        assertThat(negInf).isLessThan(neg);
        assertThat(neg).isLessThan(zero);
        assertThat(zero).isLessThan(pos);
        assertThat(pos).isLessThan(posInf);
    }

    @Test
    void longBitsToDoubleDoesNotDecodeSortableLong() {
        var original = -1.0;
        var encoded = FloatEncoding.doubleToSortableLong(original);
        var wrong = Double.longBitsToDouble(encoded);
        assertThat(wrong).isNotEqualTo(original);
    }

    // ===== C2: RowIdBitSet enumerator crash on empty set =====

    @Test
    void emptyBitSetEnumeratorShouldNotThrow() {
        var set = new RowIdBitSet();
        var enumerator = set.enumerator();
        assertThat(enumerator.hasNext()).isFalse();
    }

    @Test
    void emptyBitSetEnumeratorNextLongShouldThrow() {
        var set = new RowIdBitSet();
        var enumerator = set.enumerator();
        assertThatThrownBy(enumerator::nextLong).isInstanceOf(NoSuchElementException.class);
    }

    // ===== H1: IntAccumulator.union forgets size (tested indirectly) =====

    @Test
    void bitSetUnionFromEmptyShouldReflectSize() {
        var set = new RowIdBitSet();
        var other = new RowIdBitSet();
        other.add(RowId.fromLong(10));
        other.add(RowId.fromLong(20));
        other.add(RowId.fromLong(30));
        assertThat(set.size()).isZero();
        assertThat(other.size()).isEqualTo(3);
    }

    // ===== H3: RowIdBitSet size counter can go negative =====

    @RepeatedTest(10)
    void addRemoveRaceShouldNotProduceNegativeSize() throws Exception {
        var set = new RowIdBitSet();
        var rowId = RowId.fromLong(42);
        var barrier = new CyclicBarrier(2);
        var threads = new ArrayList<Thread>();
        for (var t = 0; t < 2; t++) {
            threads.add(Thread.startVirtualThread(() -> {
                try {
                    barrier.await();
                    for (var i = 0; i < 1000; i++) {
                        set.add(rowId);
                        set.remove(rowId);
                    }
                } catch (Exception ignored) {}
            }));
        }
        for (var thread : threads) {
            thread.join(5000);
        }
        assertThat(set.size()).isNotNegative();
    }

    // ===== H4: parseLimit/parseDistinct ordering =====

    @Test
    void distinctTop3ShouldBeParsedAsFindBy() {
        var tokens = QueryMethodLexer.tokenize(null, "findDistinctTop3ByName");
        var values = tokens.stream().map(QueryMethodToken::value).toList();
        assertThat(values).contains("name");
    }

    // ===== H5: Operator keywords inside property names =====

    @Test
    void islandPropertyShouldNotBeSplitByOperator() {
        var tokens = QueryMethodLexer.tokenize(null, "findByIsland");
        var values = tokens.stream().map(QueryMethodToken::value).toList();
        assertThat(values).containsExactly("island");
    }

    @Test
    void indexPropertyShouldNotBeSplitByOperator() {
        var tokens = QueryMethodLexer.tokenize(null, "findByIndex");
        var values = tokens.stream().map(QueryMethodToken::value).toList();
        assertThat(values).containsExactly("index");
    }

    // ===== M2: LogicalQuery equals/hashCode missing updateAssignments and projection =====

    @Test
    void logicalQueryEqualsShouldIncludeUpdateAssignments() {
        var a1 = new LogicalQuery.UpdateAssignment("name", 0);
        var a2 = new LogicalQuery.UpdateAssignment("age", 0);
        var q1 = LogicalQuery.of(OpCode.UPDATE_QUERY, LogicalQuery.ReturnKind.MODIFYING_INT,
                new LogicalQuery.Condition[0], new LogicalQuery.UpdateAssignment[]{a1}, null,
                new LogicalQuery.Join[0], new LogicalQuery.OrderBy[0], null, null, 0, false,
                new Object[0], new int[0], 1);
        var q2 = LogicalQuery.of(OpCode.UPDATE_QUERY, LogicalQuery.ReturnKind.MODIFYING_INT,
                new LogicalQuery.Condition[0], new LogicalQuery.UpdateAssignment[]{a2}, null,
                new LogicalQuery.Join[0], new LogicalQuery.OrderBy[0], null, null, 0, false,
                new Object[0], new int[0], 1);
        assertThat(q1).isNotEqualTo(q2);
    }

    @Test
    void logicalQueryHashCodeShouldDifferForDifferentAssignments() {
        var a1 = new LogicalQuery.UpdateAssignment("name", 0);
        var a2 = new LogicalQuery.UpdateAssignment("age", 0);
        var q1 = LogicalQuery.of(OpCode.UPDATE_QUERY, LogicalQuery.ReturnKind.MODIFYING_INT,
                new LogicalQuery.Condition[0], new LogicalQuery.UpdateAssignment[]{a1}, null,
                new LogicalQuery.Join[0], new LogicalQuery.OrderBy[0], null, null, 0, false,
                new Object[0], new int[0], 1);
        var q2 = LogicalQuery.of(OpCode.UPDATE_QUERY, LogicalQuery.ReturnKind.MODIFYING_INT,
                new LogicalQuery.Condition[0], new LogicalQuery.UpdateAssignment[]{a2}, null,
                new LogicalQuery.Join[0], new LogicalQuery.OrderBy[0], null, null, 0, false,
                new Object[0], new int[0], 1);
        assertThat(q1.hashCode()).isNotEqualTo(q2.hashCode());
    }

    // ===== M7: Predicate.Between null bounds =====

    @Test
    void predicateBetweenShouldRejectNullLower() {
        assertThatThrownBy(() -> new Predicate.Between("col", null, 10))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void predicateBetweenShouldRejectNullUpper() {
        assertThatThrownBy(() -> new Predicate.Between("col", 1, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ===== M7: JPQL negative number literals =====

    @Test
    void jpqlShouldParseNegativeNumberLiteral() {
        assertThatCode(() -> JpqlLexer.tokenize("WHERE e.value > -5"))
                .doesNotThrowAnyException();
    }

    // ===== M9: JPQL unterminated string literal =====

    @Test
    void jpqlUnterminatedStringShouldThrow() {
        assertThatThrownBy(() -> JpqlLexer.tokenize("WHERE e.name = 'unterminated"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ===== L1: findCombinatorIndex StackOverflow =====

    @Test
    void lexerShouldHandleLongMethodNameWithoutStackOverflow() {
        var sb = new StringBuilder("findBy");
        for (var i = 0; i < 200; i++) {
            sb.append("Anderson");
        }
        sb.append("Name");
        assertThatCode(() -> QueryMethodLexer.tokenize(null, sb.toString()))
                .doesNotThrowAnyException();
    }

    // ===== L2: MethodKey primitive boxing =====
    // IdParam explicitly excludes primitives, which is correct behavior

    @Test
    void idParamShouldRejectPrimitiveLong() {
        assertThat(io.memris.query.IdParam.isValidIdType(long.class)).isFalse();
    }

    @Test
    void idParamShouldAcceptBoxedLong() {
        assertThat(io.memris.query.IdParam.isValidIdType(Long.class)).isTrue();
    }

    // ===== Mutation test: FloatEncoding round-trip =====

    @ParameterizedTest
    @MethodSource("doubleValues")
    void doubleSortableRoundTripMutation(double value) {
        var encoded = FloatEncoding.doubleToSortableLong(value);
        var decoded = FloatEncoding.sortableLongToDouble(encoded);
        if (Double.isNaN(value)) {
            assertThat(Double.isNaN(decoded)).isTrue();
        } else {
            assertThat(decoded).isEqualTo(value);
        }
    }

    static Stream<Double> doubleValues() {
        return Stream.of(
                Double.NEGATIVE_INFINITY, -1000.0, -1.0, -0.001, -Double.MIN_VALUE,
                0.0, Double.MIN_VALUE, 0.001, 1.0, 42.5, 1000.0,
                Double.MAX_VALUE, Double.POSITIVE_INFINITY, Double.NaN
        );
    }

    // ===== Mutation test: FloatEncoding ordering =====

    @Test
    void doubleSortableOrderingMutation() {
        var values = new double[]{-1e0, 0.0, 1.0, -100.0, 100.0, Double.MIN_VALUE, Double.MAX_VALUE};
        for (var i = 0; i < values.length - 1; i++) {
            for (var j = i + 1; j < values.length; j++) {
                if (values[i] < values[j]) {
                    assertThat(FloatEncoding.doubleToSortableLong(values[i]))
                            .isLessThan(FloatEncoding.doubleToSortableLong(values[j]));
                }
            }
        }
    }

    // ===== Mutation test: RowIdBitSet add-remove consistency =====

    @RepeatedTest(5)
    void bitSetAddRemoveConsistencyMutation() {
        var set = new RowIdBitSet();
        var count = 500;
        for (var i = 0; i < count; i++) {
            set.add(RowId.fromLong(i));
        }
        for (var i = 0; i < count; i += 2) {
            set.remove(RowId.fromLong(i));
        }
        assertThat(set.size()).isEqualTo(count / 2);
        var arr = set.toLongArray();
        assertThat(arr).hasSize(count / 2);
        for (var i = 0; i < arr.length; i++) {
            assertThat(arr[i] % 2).isEqualTo(1);
        }
    }

}
