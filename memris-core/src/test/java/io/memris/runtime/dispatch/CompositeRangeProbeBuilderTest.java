package io.memris.runtime.dispatch;

import io.memris.core.TypeCodes;
import io.memris.index.CompositeKey;
import io.memris.kernel.Predicate;
import io.memris.query.CompiledQuery;
import io.memris.query.LogicalQuery;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

class CompositeRangeProbeBuilderTest {

    @Test
    @DisplayName("should build BETWEEN probe from eq prefix and range")
    void shouldBuildBetweenProbeFromEqPrefixAndRange() {
        var conditions = new CompiledQuery.CompiledCondition[] {
                CompiledQuery.CompiledCondition.of(1,
                        TypeCodes.TYPE_STRING,
                        LogicalQuery.Operator.EQ,
                        0,
                        false,
                        LogicalQuery.Combinator.AND),
                CompiledQuery.CompiledCondition.of(2,
                        TypeCodes.TYPE_INT,
                        LogicalQuery.Operator.BETWEEN,
                        1,
                        false,
                        LogicalQuery.Combinator.OR)
        };
        var args = new Object[] { "us", 10, 20 };

        var probe = CompositeRangeProbeBuilder.build(conditions, 0, 1, new int[] { 1, 2 }, args);
        var summary = new ProbeSummary(
                probe.operator(),
                probe.lower(),
                probe.upper(),
                Arrays.toString(probe.consumed()));

        assertThat(summary).usingRecursiveComparison().isEqualTo(new ProbeSummary(
                Predicate.Operator.BETWEEN,
                CompositeKey.of(new Object[] { "us", 10 }),
                CompositeKey.of(new Object[] { "us", 20 }),
                "[true, true]"));
    }

    @Test
    @DisplayName("should return null without eq prefix")
    void shouldReturnNullWithoutEqPrefix() {
        var conditions = new CompiledQuery.CompiledCondition[] {
                CompiledQuery.CompiledCondition.of(2,
                        TypeCodes.TYPE_INT,
                        LogicalQuery.Operator.GTE,
                        0,
                        false,
                        LogicalQuery.Combinator.OR)
        };

        var probe = CompositeRangeProbeBuilder.build(conditions, 0, 0, new int[] { 1, 2 }, new Object[] { 10 });

        assertThat(probe).isNull();
    }

    @Test
    @DisplayName("should build full equality probe")
    void shouldBuildFullEqualityProbe() {
        var conditions = new CompiledQuery.CompiledCondition[] {
                cond(1, LogicalQuery.Operator.EQ, 0, false),
                cond(2, LogicalQuery.Operator.EQ, 1, false)
        };

        var probe = CompositeRangeProbeBuilder.build(
                conditions,
                0,
                1,
                new int[] { 1, 2 },
                new Object[] { "us", 5 });

        assertThat(probe.operator()).isEqualTo(Predicate.Operator.EQ);
        assertThat(probe.lower()).isEqualTo(CompositeKey.of(new Object[] { "us", 5 }));
        assertThat(probe.upper()).isNull();
    }

    @Test
    @DisplayName("should build range probes for GT/GTE/LT/LTE/EQ and fallback")
    void shouldBuildRangeProbesForOperators() {
        assertThat(buildForOperator(LogicalQuery.Operator.GT, 10).operator()).isEqualTo(Predicate.Operator.GT);
        assertThat(buildForOperator(LogicalQuery.Operator.GTE, 10).operator()).isEqualTo(Predicate.Operator.GTE);
        assertThat(buildForOperator(LogicalQuery.Operator.LT, 10).operator()).isEqualTo(Predicate.Operator.LT);
        assertThat(buildForOperator(LogicalQuery.Operator.LTE, 10).operator()).isEqualTo(Predicate.Operator.LTE);
        assertThat(buildForOperator(LogicalQuery.Operator.EQ, 10).operator()).isEqualTo(Predicate.Operator.BETWEEN);

        var fallback = buildForOperator(LogicalQuery.Operator.NOT_NULL, 10);
        assertThat(fallback.operator()).isEqualTo(Predicate.Operator.BETWEEN);
    }

    @Test
    @DisplayName("should use sentinel range when only eq prefix is present")
    void shouldUseSentinelRangeWhenNoRangeConditionFound() {
        var conditions = new CompiledQuery.CompiledCondition[] {
                cond(1, LogicalQuery.Operator.EQ, 0, false),
                cond(2, LogicalQuery.Operator.NOT_NULL, 1, false)
        };

        var probe = CompositeRangeProbeBuilder.build(
                conditions,
                0,
                1,
                new int[] { 1, 2, 3 },
                new Object[] { "us", 1 });

        assertThat(probe.operator()).isEqualTo(Predicate.Operator.BETWEEN);
        assertThat(probe.lower()).isEqualTo(CompositeKey.of(new Object[] {
                "us",
                CompositeKey.minSentinel(),
                CompositeKey.minSentinel()
        }));
        assertThat(probe.upper()).isEqualTo(CompositeKey.of(new Object[] {
                "us",
                CompositeKey.maxSentinel(),
                CompositeKey.maxSentinel()
        }));
    }

    @Test
    @DisplayName("compileShape should ignore ignoreCase predicates")
    void compileShapeShouldIgnoreIgnoreCasePredicates() {
        var conditions = new CompiledQuery.CompiledCondition[] {
                cond(1, LogicalQuery.Operator.EQ, 0, true),
                cond(1, LogicalQuery.Operator.EQ, 1, false)
        };

        var shape = CompositeRangeProbeBuilder.compileShape(conditions, 0, 1, new int[] { 1 });
        assertThat(shape).isNotNull();
        assertThat(shape.prefixArgumentIndexes()).containsExactly(1);
        assertThat(shape.consumedOffsets(0)).containsExactly(1);
    }

    private record ProbeSummary(Predicate.Operator operator, CompositeKey lower, CompositeKey upper, String consumed) {
    }

    private static CompiledQuery.CompiledCondition cond(int column, LogicalQuery.Operator op, int arg, boolean ignoreCase) {
        return CompiledQuery.CompiledCondition.of(
                column,
                TypeCodes.TYPE_INT,
                op,
                arg,
                ignoreCase,
                LogicalQuery.Combinator.AND);
    }

    private static CompositeRangeProbe buildForOperator(LogicalQuery.Operator operator, int value) {
        var conditions = new CompiledQuery.CompiledCondition[] {
                cond(1, LogicalQuery.Operator.EQ, 0, false),
                cond(2, operator, 1, false)
        };
        return CompositeRangeProbeBuilder.build(
                conditions,
                0,
                1,
                new int[] { 1, 2, 3 },
                new Object[] { "us", value, value + 2 });
    }
}
